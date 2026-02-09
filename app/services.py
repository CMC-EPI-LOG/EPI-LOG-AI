import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Any
from urllib.parse import urlparse
import voyageai
from openai import OpenAI
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def _infer_db_name_from_uri(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    try:
        parsed = urlparse(uri)
        # mongodb+srv://host/<db>?...
        db_name = parsed.path.lstrip("/").split("?")[0].strip()
        return db_name or None
    except Exception:
        return None


DB_NAME = os.getenv("MONGO_DB_NAME", "epilog_db")
AIR_QUALITY_DB_NAME = os.getenv("AIR_QUALITY_DB_NAME") or _infer_db_name_from_uri(MONGO_URI) or DB_NAME
GUIDELINES_COLLECTION = "medical_guidelines"
AIR_QUALITY_COLLECTION = "daily_air_quality"
AIR_QUALITY_DATA_COLLECTION = "air_quality_data"  # Lambda cron job collection
VECTOR_INDEX = "vector_index"
KST_TZ = ZoneInfo("Asia/Seoul")

if not MONGO_URI:
    # Fallback to a dummy URI if not set to prevent startup crash, but it will fail on request
    print("WARNING: MONGODB_URI is not set.")
    MONGO_URI = "mongodb://localhost:27017"

# Initialize Clients
try:
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    air_quality_db = mongo_client[AIR_QUALITY_DB_NAME]
    print(f"✅ MongoDB connected: main_db={DB_NAME}, air_quality_db={AIR_QUALITY_DB_NAME}")
except Exception as e:
    print(f"Error initializing MongoDB client: {e}")
    mongo_client = None
    db = None
    air_quality_db = None

# --- Logic Constants ---
GRADE_MAP = {
    "좋음": 1,
    "보통": 2,
    "나쁨": 3,
    "매우나쁨": 4
}

REVERSE_GRADE_MAP = {v: k for k, v in GRADE_MAP.items()}

# Correction Weights
HUMIDITY_WEIGHTS = {
    "high": 1.2,  # > 70%
    "low": 1.1,   # < 30%
    "normal": 1.0
}

def _get_corrected_grade(
    base_grade: str, 
    temp: Optional[float], 
    humidity: Optional[float], 
    condition: str,
    pollutant_type: str # "pm25" or "o3"
) -> str:
    """
    Apply correction logic based on temperature, humidity and disease condition.
    Returns the corrected grade string.
    """
    score = GRADE_MAP.get(base_grade, 2)
    
    # 1. Humidity Correction (W_h)
    w_h = 1.0
    if humidity is not None:
        if humidity > 70:
            w_h = HUMIDITY_WEIGHTS["high"]
        elif humidity < 30:
            w_h = HUMIDITY_WEIGHTS["low"]
    
    # 2. Temperature & Disease Trigger Logic
    # Asthma + Cold + PM2.5
    if condition == "asthma" and temp is not None and temp < 5 and pollutant_type == "pm25":
        if base_grade == "보통": return "나쁨"
        
    # Rhinitis + Dry + PM2.5
    if condition == "rhinitis" and humidity is not None and humidity < 30 and pollutant_type == "pm25":
        if base_grade == "보통": return "나쁨"
        
    # Atopy + Heat + O3
    if condition == "atopy" and temp is not None and temp > 30 and pollutant_type == "o3":
        if base_grade == "보통": return "나쁨"
        
    # General + High Humidity + PM2.5 Bad
    if humidity is not None and humidity > 80 and pollutant_type == "pm25" and base_grade == "나쁨":
        return "매우나쁨"

    # Apply multiplicative weight if no specific trigger fired
    # (Simplified: if score * w_h rounds up to next grade)
    final_score = min(4, max(1, round(score * w_h)))
    return REVERSE_GRADE_MAP.get(final_score, base_grade)


def _normalize_station_candidates(station_name: str) -> List[str]:
    cleaned = " ".join((station_name or "").strip().split())
    if not cleaned:
        return []

    seen = set()
    candidates: List[str] = []

    def add(value: str):
        normalized = " ".join(value.strip().split())
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    add(cleaned)
    add(cleaned.replace(" ", ""))

    tokens = cleaned.split(" ")
    if len(tokens) >= 2:
        add(tokens[-1])
        add(tokens[-2])
        add(f"{tokens[-2]} {tokens[-1]}")
    elif tokens:
        add(tokens[0])

    return candidates


def _parse_datetime_to_kst(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    dt: Optional[datetime] = None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        # AirKorea Lambda snapshot format: "YYYY-MM-DD HH:MM"
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M")
            dt = parsed.replace(tzinfo=KST_TZ)
        except ValueError:
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST_TZ)

    return dt.astimezone(KST_TZ)

# Decision Texts based on logic.csv (80-segment dataset)
DECISION_TEXTS = {
    "infant": {
        "general": {
            "ok": "유모차 산책 가요",
            "caution": "짧은 산책만 추천",
            "warning": "외부 공기 전면 차단"
        },
        "rhinitis": {
            "ok": "코가 편안한 날",
            "caution": "코점막 보습 집중",
            "warning": "호흡 곤란 주의"
        },
        "asthma": {
            "ok": "상쾌하게 숨 쉬어요",
            "caution": "찬 바람 노출 주의",
            "warning": "절대 안정 실내 대기"
        },
        "atopy": {
            "ok": "가려움 걱정 뚝",
            "caution": "땀나면 바로 닦아요",
            "warning": "보습제 2배 도포"
        }
    },
    "toddler": {
        "general": {
            "ok": "놀이터에서 뛰놀아요",
            "caution": "물 한 컵 마시고 외출",
            "warning": "오늘은 집에서 놀아요"
        },
        "rhinitis": {
            "ok": "코 면역력 키우는 날",
            "caution": "재채기 먼지 조심",
            "warning": "환기 금지 물걸레질"
        },
        "asthma": {
            "ok": "기도가 열리는 날씨",
            "caution": "갑작스런 기침 주의",
            "warning": "실외 이동 전면 제한"
        },
        "atopy": {
            "ok": "피부가 숨 쉬는 날",
            "caution": "땀과 먼지를 멀리해요",
            "warning": "피부 진정 팩 추천"
        }
    },
    "elementary_low": {
        "general": {
            "ok": "운동장에서 마음껏!",
            "caution": "체육 전 상태 확인",
            "warning": "실외 수업 참여 제외"
        },
        "rhinitis": {
            "ok": "숲 체험 가기 좋은 날",
            "caution": "마스크 쓰고 등교",
            "warning": "코막힘 증상 관리"
        },
        "asthma": {
            "ok": "컨디션 최상인 날",
            "caution": "운동 강도를 조절해요",
            "warning": "발작 위험 실외 금지"
        },
        "atopy": {
            "ok": "자외선 차단제 필수",
            "caution": "긴소매로 피부 보호",
            "warning": "가려움증 심화 주의"
        }
    },
    "elementary_high": {
        "general": {
            "ok": "환기하며 공부해요",
            "caution": "하교 후 손발 씻기",
            "warning": "실외 노출 최소화"
        },
        "rhinitis": {
            "ok": "코가 시원한 등굣길",
            "caution": "마스크 휴대하기",
            "warning": "환기 대신 공청기"
        },
        "asthma": {
            "ok": "활기차게 운동해요",
            "caution": "운동 전후 수분 보충",
            "warning": "실외 이동 전면 제한"
        },
        "atopy": {
            "ok": "상쾌한 야외 활동",
            "caution": "보습막 유지하기",
            "warning": "피부 진정 및 차단"
        }
    },
    "teen_adult": {
        "general": {
            "ok": "야외 운동 추천",
            "caution": "일상 활동 무관",
            "warning": "실외 활동 최소화"
        },
        "rhinitis": {
            "ok": "상쾌한 호흡",
            "caution": "코 세정 준비",
            "warning": "외부 공기 차단"
        },
        "asthma": {
            "ok": "유산소 운동 가능",
            "caution": "무리한 활동 자제",
            "warning": "실외 활동 전면 제한"
        },
        "atopy": {
            "ok": "야외 나들이 추천",
            "caution": "피부 청결 유지",
            "warning": "즉각적인 피부 세정"
        }
    }
}

# Action Items templates based on logic.csv
ACTION_ITEMS = {
    "infant": {
        "general": {
            "ok": ["유모차 산책", "15분 환기", "귀가 후 손발 씻기"],
            "caution": ["유모차 커버 사용", "그늘 위주 산책", "복귀 후 보습"],
            "warning": ["외출 절대 금지", "창문 틈새 밀폐", "물걸레 청소"]
        },
        "rhinitis": {
            "ok": ["쾌적한 환기", "집안 먼지 털기", "가벼운 외출"],
            "caution": ["가습기 가동", "미지근한 물 섭취", "유모차 가림막"],
            "warning": ["절대 실내 대기", "상비약 확인", "상태 집중 모니터"]
        },
        "asthma": {
            "ok": ["신선한 공기 유입", "보호자와 산책", "충분한 휴식"],
            "caution": ["목 가싸개 사용", "급격한 온도차 주의", "호흡 수시 관찰"],
            "warning": ["병원 외 외출 금지", "증상 대응 준비", "공청기 풀가동"]
        },
        "atopy": {
            "ok": ["외출 전 선크림", "활동 후 세안", "면 소재 옷 입기"],
            "caution": ["손수건 지참", "수시로 보습제", "얇은 긴소매 옷"],
            "warning": ["절대 실내 체류", "시원한 온도 유지", "고보습 크림 사용"]
        }
    },
    "toddler": {
        "general": {
            "ok": ["야외 놀이 권장", "전면 환기 시키기", "활동 후 수분 섭취"],
            "caution": ["중간 수분 섭취", "마스크 휴대하기", "장시간 체류 자제"],
            "warning": ["야외 활동 금지", "공기청정기 사용", "실내 적정 가습"]
        },
        "rhinitis": {
            "ok": ["숲 체험 추천", "환기 후 청소", "외출 후 세안"],
            "caution": ["마스크 필수 착용", "코 주변 보습", "식염수 세척"],
            "warning": ["절대 실내 대기", "공청기 가동", "증상 시 약 복용"]
        },
        "asthma": {
            "ok": ["유산소 놀이", "규칙적 약 복용", "실내외 공기 정화"],
            "caution": ["운동 강도 낮추기", "중간 휴식 취하기", "호흡 상태 확인"],
            "warning": ["외출 절대 금지", "보호자 밀착 관찰", "비상 시 병원 방문"]
        },
        "atopy": {
            "ok": ["선크림 도포", "활동 후 샤워", "면 소재 옷 추천"],
            "caution": ["수시로 땀 닦기", "외출 후 보습", "통기성 의류"],
            "warning": ["냉찜질 진정", "실외 활동 중단", "자극 없는 로션"]
        }
    },
    "elementary_low": {
        "general": {
            "ok": ["축구/달리기 추천", "교실 전면 환기", "야외 학습"],
            "caution": ["중간 수분 섭취", "활동 후 양치", "대기질 수시 체크"],
            "warning": ["실내 활동 전환", "창문 밀폐 관리", "실내 공기 정화"]
        },
        "rhinitis": {
            "ok": ["야외 산책", "환기 후 대청소", "충분한 휴식"],
            "caution": ["마스크 휴대", "손 씻기 교육", "물 자주 마시기"],
            "warning": ["환기 절대 금지", "식염수 코 세척", "증상 완화제 준비"]
        },
        "asthma": {
            "ok": ["학교 체육 참여", "깊은 호흡 운동", "컨디션 유지"],
            "caution": ["무리한 달리기 자제", "중간 휴식 늘리기", "호흡 모니터링"],
            "warning": ["노출 전면 차단", "보호자 상시 관찰", "비상약 위치 확인"]
        },
        "atopy": {
            "ok": ["선크림 바르기", "야외 활동 즐기기", "활동 후 세안"],
            "caution": ["손수건 지참", "외출 후 보습제", "면 속옷 입히기"],
            "warning": ["외출 금지", "냉찜질 진정", "실내 습도 조절"]
        }
    },
    "elementary_high": {
        "general": {
            "ok": ["전면 환기 실시", "야외 운동 권장", "자전거/도보 등교"],
            "caution": ["외출 후 위생 관리", "물 자주 마시기", "교실 환기 협조"],
            "warning": ["실외 활동 전면 중단", "창문 밀폐 확인", "공청기 가동"]
        },
        "rhinitis": {
            "ok": ["상쾌한 아침 산책", "교실 환기 권장", "규칙적 수면 관리"],
            "caution": ["손수건/마스크 지참", "콧물 증상 관리", "실내 습도 조절"],
            "warning": ["절대 실내 대기", "물걸레 청소", "증상 시 약 복용"]
        },
        "asthma": {
            "ok": ["운동장 활동 권장", "깊은 호흡 연습", "규칙적 투약 유지"],
            "caution": ["충분한 물 섭취", "컨디션 체크", "무리한 달리기 자제"],
            "warning": ["야외 학원 이동 자제", "보호자 밀착 확인", "비상 연락망 점검"]
        },
        "atopy": {
            "ok": ["보습제 도포 후 외출", "가벼운 운동", "땀 닦기"],
            "caution": ["보습제 휴대", "외출 후 세안", "면 소재 의류"],
            "warning": ["실외 활동 중단", "냉찜질 진정", "고보습 관리"]
        }
    },
    "teen_adult": {
        "general": {
            "ok": ["조깅/등산 권장", "전면 환기 실시", "야외 학습/업무"],
            "caution": ["충분한 수분 섭취", "손 씻기 생활화", "가벼운 환기"],
            "warning": ["외출 자제", "보건용 마스크 필착", "물걸레 청소"]
        },
        "rhinitis": {
            "ok": ["침구류 햇볕 소독", "전면 환기", "가벼운 산책"],
            "caution": ["마스크 휴대", "귀가 후 코 세척", "실내 습도 유지"],
            "warning": ["외출 금지", "창문 밀폐", "약물 복용 점검"]
        },
        "asthma": {
            "ok": ["규칙적 운동", "실내외 환기", "컨디션 관리"],
            "caution": ["증상 유무 확인", "무리한 등산 자제", "비상약 지참"],
            "warning": ["실내 안심 대기", "공기질 관리", "비상 시 의료기관"]
        },
        "atopy": {
            "ok": ["충분한 보습 후 외출", "자외선 차단", "면 소재 의류"],
            "caution": ["외출 후 가벼운 샤워", "보습제 도포", "수분 섭취"],
            "warning": ["야외 활동 중단", "저자극 세안 및 샤워", "고보습 진정 관리"]
        }
    }
}

def _calculate_decision(pm25_grade: str, o3_grade: str) -> str:
    """
    Calculate decision level: 'ok', 'caution', 'warning'
    
    Logic (1:좋음, 2:보통, 3:나쁨, 4:매우나쁨):
    • OK: PM2.5 <= 2 AND O3 <= 2
    • Caution: Either one is 3
    • Warning: Either one is 4 OR Both are 3
    """
    p_score = GRADE_MAP.get(pm25_grade, 2)
    o_score = GRADE_MAP.get(o3_grade, 2)
    
    # Check Warning Conditions
    if p_score >= 4 or o_score >= 4:
        return "warning"
    if p_score == 3 and o_score == 3:
        return "warning"
        
    # Check Caution Conditions
    if p_score == 3 or o_score == 3:
        return "caution"
        
    # Default OK
    return "ok"

def _normalize_age_group(age_group: Any) -> str:
    if age_group is None:
        return "elementary_high"
    raw = str(age_group).strip().lower()
    
    # Updated 5 groups based on planning document
    if raw in {"infant", "영아", "0-2", "0~2"}:
        return "infant"
    if raw in {"toddler", "유아", "3-6", "3~6"}:
        return "toddler"
    if raw in {"elementary_low", "초등저학년", "초등 저학년", "7-9", "7~9", "1-3", "1~3"}:
        return "elementary_low"
    if raw in {"elementary_high", "초등고학년", "초등 고학년", "10-12", "10~12", "4-6", "4~6"}:
        return "elementary_high"
    if raw in {"teen", "teen_adult", "청소년", "성인", "adult", "13-18", "13~18", "13+"}:
        return "teen_adult"
    
    # Fallbacks
    if "영아" in raw: return "infant"
    if "유아" in raw: return "toddler"
    if "저학년" in raw: return "elementary_low"
    if "고학년" in raw: return "elementary_high"
    if "청소년" in raw or "성인" in raw: return "teen_adult"
    
    return "elementary_high"

def _get_display_content(age_group: str, condition: str, decision_key: str):
    """
    Returns (decision_text, action_items)
    """
    # Normalize condition
    cond_key = condition if condition in ["general", "rhinitis", "asthma", "atopy"] else "general"
    
    # Get Text
    group_data = DECISION_TEXTS.get(age_group, DECISION_TEXTS["elementary_high"])
    cond_data = group_data.get(cond_key, group_data.get("general", {}))
    d_text = cond_data.get(decision_key, "상태 확인 필요")
    
    # Get Actions
    group_actions = ACTION_ITEMS.get(age_group, ACTION_ITEMS.get("toddler", {}))
    cond_actions = group_actions.get(cond_key, group_actions.get("general", {}))
    actions = cond_actions.get(decision_key, ["상태에 따른 주의가 필요합니다."])
    
    return d_text, actions[:] # Return a copy

try:
    vo_client = voyageai.Client(api_key=VOYAGE_API_KEY)
except Exception as e:
    print(f"Error initializing Voyage AI client: {e}")
    vo_client = None

try:
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
except Exception as e:
    print(f"Error initializing OpenAI client: {e}")
    openai_client = None

async def get_air_quality_from_mongodb(station_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch latest air quality data from MongoDB air_quality_data collection.
    This collection is populated by AWS Lambda cron job every hour.
    Returns None if no recent data (> 2 hours old) or not found.
    """
    if air_quality_db is None:
        return None
    
    try:
        station_candidates = _normalize_station_candidates(station_name)
        if not station_candidates:
            return None

        # Latest-first sort strategy:
        # 1) dataTime (AirKorea snapshot time), 2) updatedAt (ingest time), 3) _id (insert order)
        sort_criteria = [("dataTime", -1), ("updatedAt", -1), ("_id", -1)]
        data = None
        matched_candidate = None

        for candidate in station_candidates:
            data = await air_quality_db[AIR_QUALITY_DATA_COLLECTION].find_one(
                {"stationName": candidate},
                sort=sort_criteria
            )
            if data:
                matched_candidate = candidate
                break

        if not data:
            print(f"⚠️  No MongoDB data found for station candidates: {station_candidates}")
            return None

        # Check data freshness (must be within 2 hours)
        observed_at = (
            _parse_datetime_to_kst(data.get("dataTime"))
            or _parse_datetime_to_kst(data.get("updatedAt"))
            or _parse_datetime_to_kst(data.get("createdAt"))
        )
        if observed_at:
            now = datetime.now(KST_TZ)
            age = now - observed_at
            if age > timedelta(hours=2):
                print(
                    f"⚠️  MongoDB data is stale ({age.total_seconds()/3600:.1f} hours old), "
                    f"station={data.get('stationName')} observed_at={observed_at.isoformat()}"
                )
                return None

        # Convert grade strings to Korean text
        grade_map = {"1": "좋음", "2": "보통", "3": "나쁨", "4": "매우나쁨"}

        # Support both camelCase (Lambda docs) and snake_case (legacy docs)
        pm25_grade_value = data.get("pm25Grade", data.get("pm25_grade", "2"))
        pm10_grade_value = data.get("pm10Grade", data.get("pm10_grade", "2"))
        o3_grade_value = data.get("o3Grade", data.get("o3_grade", "1"))
        no2_grade_value = data.get("no2Grade", data.get("no2_grade", "1"))
        co_grade_value = data.get("coGrade", data.get("co_grade", "1"))
        so2_grade_value = data.get("so2Grade", data.get("so2_grade", "1"))

        def _coerce_number(value: Any) -> Optional[float]:
            if value is None:
                return None
            # Decimal128 in Mongo can break JSON serialization; normalize to float when possible.
            try:
                if hasattr(value, "to_decimal"):
                    return float(value.to_decimal())
            except Exception:
                pass
            try:
                return float(value)
            except Exception:
                return None

        temp_raw = data.get("temperature")
        if temp_raw is None:
            temp_raw = data.get("temp")
        humidity_raw = data.get("humidity")

        temp_value = _coerce_number(temp_raw)
        humidity_value = _coerce_number(humidity_raw)

        result = {
            "stationName": data.get("stationName", station_name),
            "sidoName": data.get("sidoName"),
            "pm25_grade": grade_map.get(str(pm25_grade_value), "보통"),
            "pm25_value": data.get("pm25Value", data.get("pm25_value", 50)),
            "pm10_grade": grade_map.get(str(pm10_grade_value), "보통"),
            "pm10_value": data.get("pm10Value", data.get("pm10_value", 70)),
            "o3_grade": grade_map.get(str(o3_grade_value), "좋음"),
            "o3_value": data.get("o3Value", data.get("o3_value", 0.05)),
            "no2_grade": grade_map.get(str(no2_grade_value), "좋음"),
            "no2_value": data.get("no2Value", data.get("no2_value", 0.02)),
            "co_grade": grade_map.get(str(co_grade_value), "좋음"),
            "co_value": data.get("coValue", data.get("co_value", 0.5)),
            "so2_grade": grade_map.get(str(so2_grade_value), "좋음"),
            "so2_value": data.get("so2Value", data.get("so2_value", 0.003)),
            # Lambda now stores weather on the same document.
            # Support both `temperature` (Lambda) and `temp` (legacy/other sources).
            "temp": temp_value,
            "humidity": humidity_value,
            "dataTime": data.get("dataTime"),
        }

        print(
            f"✅ Fetched latest air quality from MongoDB "
            f"(requested={station_name}, matched={matched_candidate}, station={result['stationName']}, dataTime={result.get('dataTime')})"
        )
        return result

    except Exception as e:
        print(f"❌ Error fetching from MongoDB: {e}")
        return None

async def get_air_quality_from_airkorea_api(station_name: str) -> Optional[Dict[str, Any]]:
    """
    Direct fallback to Air Korea OpenAPI.
    This replaces the dependency on EPI-LOG-AIRKOREA service.
    """
    import httpx
    
    # Air Korea OpenAPI endpoint (replace with actual endpoint if different)
    # Note: This is a placeholder - actual Air Korea API integration would require
    # API key and proper endpoint configuration
    
    # For now, we'll try the old EPI-LOG-AIRKOREA service as temporary fallback
    # TODO: Replace with direct Air Korea OpenAPI call
    AIRKOREA_API_URL = "https://epi-log-airkorea.vercel.app/api/stations"
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                AIRKOREA_API_URL,
                params={"stationName": station_name}
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # API returns array, take first item
                if data and len(data) > 0:
                    station = data[0]
                    realtime = station.get("realtime", {})
                    
                    # Convert grade numbers to Korean text
                    grade_map = {1: "좋음", 2: "보통", 3: "나쁨", 4: "매우나쁨"}
                    
                    # Extract and normalize data
                    result = {
                        "stationName": station.get("stationName", station_name),
                        "sidoName": station.get("sidoName"),
                        "pm25_grade": grade_map.get(realtime.get("pm25", {}).get("grade"), "보통"),
                        "pm25_value": realtime.get("pm25", {}).get("value") or 50,
                        "pm10_grade": grade_map.get(realtime.get("pm10", {}).get("grade"), "보통"),
                        "pm10_value": realtime.get("pm10", {}).get("value") or 70,
                        "o3_grade": grade_map.get(realtime.get("o3", {}).get("grade"), "보통"),
                        "o3_value": realtime.get("o3", {}).get("value") or 0.05,
                        "no2_grade": grade_map.get(realtime.get("no2", {}).get("grade"), "좋음"),
                        "no2_value": realtime.get("no2", {}).get("value") or 0.02,
                        "co_grade": grade_map.get(realtime.get("co", {}).get("grade"), "좋음"),
                        "co_value": realtime.get("co", {}).get("value") or 0.5,
                        "so2_grade": grade_map.get(realtime.get("so2", {}).get("grade"), "좋음"),
                        "so2_value": realtime.get("so2", {}).get("value") or 0.003,
                        "temp": None,
                        "humidity": None,
                        "dataTime": realtime.get("dataTime") or station.get("dataTime")
                    }
                    
                    print(f"✅ Fetched air quality for {station_name} from Air Korea API (fallback)")
                    return result
        
        print(f"⚠️  No data from Air Korea API for {station_name}")
        return None
        
    except Exception as e:
        print(f"❌ Error fetching from Air Korea API: {e}")
        return None

async def get_air_quality(station_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch air quality data with priority order:
    1. MongoDB air_quality_data (Lambda cron job data) - PRIORITY
    2. Air Korea OpenAPI (fallback for real-time data)
    3. Mock data (final fallback)
    
    Note: Temperature and humidity are expected to be present in the Lambda-stored MongoDB document.
    If missing, the API returns default placeholders.
    """
    # Priority 1: Try MongoDB (Lambda-stored data)
    data = await get_air_quality_from_mongodb(station_name)
    if data:
        # Add default temp/humidity for now (will be replaced with weather API)
        if data.get("temp") is None:
            data["temp"] = 22.0  # Default value
        if data.get("humidity") is None:
            data["humidity"] = 45.0  # Default value
        return data
    
    # Priority 2: Try Air Korea API (temporary fallback)
    data = await get_air_quality_from_airkorea_api(station_name)
    if data:
        # Add default temp/humidity
        if data.get("temp") is None:
            data["temp"] = 22.0
        if data.get("humidity") is None:
            data["humidity"] = 45.0
        return data
    
    # Priority 3: Return mock data (final fallback)
    print(f"⚠️  Using mock data for {station_name}")
    return {
        "sidoName": None,
        "stationName": station_name,
        "pm10_grade": "나쁨",
        "pm10_value": 85,
        "pm25_grade": "나쁨",
        "pm25_value": 65,
        "co_grade": "보통",
        "co_value": 0.7,
        "o3_grade": "보통",
        "o3_value": 0.065,
        "no2_grade": "좋음",
        "no2_value": 0.025,
        "so2_grade": "좋음",
        "so2_value": 0.004,
        "temp": 22.0,
        "humidity": 45.0,
        "dataTime": None,
    }

CACHE_COLLECTION = "rag_cache"
CACHE_TTL_SECONDS = 60 * 60 * 30  # 30 hours
_cache_ttl_index_ready = False

def _normalize_cache_token(value: Any) -> str:
    if value is None:
        return "na"
    token = str(value).strip()
    if not token:
        return "na"
    for old, new in [
        (" ", ""),
        (":", "-"),
        ("/", "-"),
        ("\\", "-"),
        ("\n", ""),
        ("\t", "")
    ]:
        token = token.replace(old, new)
    return token or "na"


def _generate_cache_key(air_data: Dict[str, Any], user_profile: Dict[str, Any]) -> str:
    grade_map = {"좋음": 1, "보통": 2, "나쁨": 3, "매우나쁨": 4}
    
    pm25 = grade_map.get(air_data.get("pm25_grade", ""), 0)
    pm10 = grade_map.get(air_data.get("pm10_grade", ""), 0)
    o3 = grade_map.get(air_data.get("o3_grade", ""), 0) # Added o3 as per user example
    
    age_group = _normalize_age_group(user_profile.get("ageGroup"))
    condition = user_profile.get("condition", "unknown")
    station_key = _normalize_cache_token(
        f"{air_data.get('sidoName', '')}_{air_data.get('stationName', '')}"
    )

    observed_at = (
        _parse_datetime_to_kst(air_data.get("dataTime"))
        or _parse_datetime_to_kst(air_data.get("updatedAt"))
    )
    observed_key = observed_at.strftime("%Y%m%d%H%M") if observed_at else datetime.now(KST_TZ).strftime("%Y%m%d")

    pm25_value = _normalize_cache_token(air_data.get("pm25_value"))
    pm10_value = _normalize_cache_token(air_data.get("pm10_value"))
    o3_value = _normalize_cache_token(air_data.get("o3_value"))
    no2_value = _normalize_cache_token(air_data.get("no2_value"))
    
    # Key format:
    # station:seoul_jongro_pm25:2_pm10:2_o3:1_age:toddler_cond:asthma_obs:202602071300_vals:14_52_0.03_0.009
    return (
        f"station:{station_key}_"
        f"pm25:{pm25}_pm10:{pm10}_o3:{o3}_"
        f"age:{_normalize_cache_token(age_group)}_cond:{_normalize_cache_token(condition)}_"
        f"obs:{observed_key}_vals:{pm25_value}_{pm10_value}_{o3_value}_{no2_value}"
    )

async def _ensure_cache_ttl_index():
    global _cache_ttl_index_ready
    if _cache_ttl_index_ready or db is None:
        return
    try:
        await db[CACHE_COLLECTION].create_index(
            "created_at",
            expireAfterSeconds=CACHE_TTL_SECONDS,
            name="rag_cache_ttl"
        )
        _cache_ttl_index_ready = True
    except Exception as e:
        print(f"⚠️ Cache TTL index creation failed: {e}")

async def get_medical_advice(station_name: str, user_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main orchestration function with correction logic.
    """
    # Step A: Get Air Quality
    air_data = await get_air_quality(station_name)
    if not air_data:
        raise ValueError(f"No air quality data found for station: {station_name}")

    # Extract Weather Info for Correction
    temp = air_data.get("temp")
    humidity = air_data.get("humidity")
    user_condition = user_profile.get("condition", "건강함")
    age_group_raw = user_profile.get("ageGroup")
    age_group = _normalize_age_group(age_group_raw)

    # Apply Correction Logic to get "Sensed" grades
    pm25_raw = air_data.get("pm25_grade", "보통")
    o3_raw = air_data.get("o3_grade", "보통")
    
    pm25_corrected = _get_corrected_grade(pm25_raw, temp, humidity, user_condition, "pm25")
    o3_corrected = _get_corrected_grade(o3_raw, temp, humidity, user_condition, "o3")

    cache_key = ""
    # [Step A.1] Check Cache
    if db is not None:
        try:
            await _ensure_cache_ttl_index()
            cache_key = _generate_cache_key(air_data, user_profile)
            cached_entry = await db[CACHE_COLLECTION].find_one({"_id": cache_key})
            
            if cached_entry:
                print(f"✅ Cache Hit! Key: {cache_key}")
                return cached_entry["data"]
        except Exception as e:
            print(f"⚠️ Cache check failed: {e}")

    # Determine main issue for search (using corrected grades)
    main_condition = "보통"
    if pm25_corrected in ["나쁨", "매우나쁨"]:
        main_condition = f"초미세먼지 {pm25_corrected}"
    elif air_data.get("pm10_grade") in ["나쁨", "매우나쁨"]:
        main_condition = f"미세먼지 {air_data['pm10_grade']}"
    elif o3_corrected in ["나쁨", "매우나쁨"]:
        main_condition = f"오존 {o3_corrected}"
        
    # Step B: Query Construction
    search_query = f"{main_condition} 상황에서 {user_condition} {age_group} 행동 요령 주의사항"
    print(f"Generated Search Query (Primary): {search_query}")

    # Step C: Vector Search
    relevant_docs = []
    if vo_client and db is not None:
        try:
            # 1. Primary Search
            embed_result = vo_client.embed([search_query], model="voyage-3-large", input_type="query")
            query_vector = embed_result.embeddings[0]
            
            pipeline = [
                {
                    "$vectorSearch": {
                        "index": VECTOR_INDEX,
                        "path": "embedding",
                        "queryVector": query_vector,
                        "numCandidates": 100,
                        "limit": 3
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "text": 1,
                        "category": 1,
                        "risk_level": 1,
                        "source": 1,
                        "score": {"$meta": "vectorSearchScore"}
                    }
                }
            ]
            
            cursor = db[GUIDELINES_COLLECTION].aggregate(pipeline)
            relevant_docs = await cursor.to_list(length=3)
            
            # 2. Fallback Search (If no docs found)
            if not relevant_docs:
                print("⚠️ Primary search returned no results. Attempting fallback (General) search.")
                fallback_query = f"{main_condition} 행동 요령"
                embed_result_fb = vo_client.embed([fallback_query], model="voyage-3-large", input_type="query")
                query_vector_fb = embed_result_fb.embeddings[0]
                
                pipeline[0]["$vectorSearch"]["queryVector"] = query_vector_fb
                
                cursor = db[GUIDELINES_COLLECTION].aggregate(pipeline)
                relevant_docs = await cursor.to_list(length=3)
                
        except Exception as e:
            print(f"Error during vector search: {e}")
            pass

    # Step D: LLM Generation
    if not openai_client:
         return {
            "decision": "Error",
            "three_reason": [
                "**AI 시스템**이 초기화되지 않았습니다.",
                "서버 설정을 확인해주세요.",
                "**관리자**에게 문의하세요."
            ],
            "detail_answer": "OpenAI Client not initialized",
            "actionItems": [],
            "references": []
        }

    # [Logic Update] Calculate Deterministic Decision & Action Items using CORRECTED grades
    decision_key = _calculate_decision(pm25_corrected, o3_corrected)
    decision_text, action_items = _get_display_content(age_group, user_condition, decision_key)
    
    # O3 Special Handling: Force-Append and Warnings
    is_o3_dominant = GRADE_MAP.get(o3_corrected, 1) >= GRADE_MAP.get(pm25_corrected, 1)
    if is_o3_dominant and GRADE_MAP.get(o3_corrected, 1) >= 3: # '나쁨' 이상
        decision_text += " (오존은 마스크로 걸러지지 않아요!)"
        # Force-Append Action Item
        o3_force_action = "오후 2~5시 사이에는 실외 활동을 전면 금지하고 실내에 머무르세요."
        if o3_force_action not in action_items:
            action_items.append(o3_force_action)

    # Infant Special Warning
    if age_group == "infant":
        infant_warning = "※ 주의: 마스크 착용 금지(질식 위험)"
        if infant_warning not in action_items:
            action_items.insert(0, infant_warning) # Put at top

    # Logic for dual bad condition text append
    if GRADE_MAP.get(pm25_corrected, 1) >= 3 and GRADE_MAP.get(o3_corrected, 1) >= 3:
        decision_text += " (미세먼지와 오존 둘 다 높아요!)"

    # Prepare Context
    context_text = "\n".join([f"- [출처: {doc.get('source', '가이드라인')}] {doc.get('text', '')}" for doc in relevant_docs]) if relevant_docs else "관련 의학적 가이드라인을 찾을 수 없습니다."
    
    system_prompt = """
    너는 복잡한 대기질 논문과 데이터를 학부모가 이해하기 쉽게 설명해주는 AI 비서다.
    
    [역할 및 출력 제약]
    1. 'decision'과 'actionItems'는 이미 시스템에서 계산되었습니다. 당신은 이 결정이 내려진 이유를 학부모가 이해하기 쉽게 설명해야 합니다.
    2. 반드시 다음 두 가지를 JSON 형식으로 출력하세요:
       - "three_reason": 정확히 3개의 짧은 요약 문장 배열 (각 문장은 한 줄로, 핵심 키워드는 **double asterisks**로 감싸기)
       - "detail_answer": 상세한 의학적/환경적 설명 (기존의 긴 설명)
    
    3. **키워드 하이라이팅 규칙**:
       - 질환명 (예: **천식**, **비염**, **아토피**)
       - 대기질 등급 (예: **좋음**, **나쁨**, **매우나쁨**)
       - 행동 요령 (예: **실외 활동**, **마스크 착용**, **환기**)
       - 중요한 수치나 시간대 (예: **35**, **오후 2~5시**)
    
    4. **톤앤매너**: 친절하지만 명확하고 단호하게. 학부모가 즉시 행동할 수 있도록 구체적으로 작성하세요.
    
    5. 보정 로직이 적용된 경우(예: 습도, 온도로 인한 등급 격상) 그 이유를 three_reason이나 detail_answer에 포함하세요.
    
    6. 제공된 [의학적 가이드라인] 내용을 최우선으로 반영하여 설명하세요.
    """
    
    user_prompt = f"""
    [상황 정보]
    - 대기질: 초미세먼지={pm25_raw}(보정후:{pm25_corrected}), 오존={o3_raw}(보정후:{o3_corrected})
    - 환경: 온도={temp}°C, 습도={humidity}%
    - 사용자: 연령대={age_group}, 기저질환={user_condition}
    - 시스템 결정: {decision_text}
    - 시스템 행동수칙: {action_items}
    
    [의학적 가이드라인 (참고 문헌)]
    {context_text}
    
    위 결정이 내려진 배경과 이유를 학부모가 이해하기 쉽게 설명해주세요.
    
    출력 형식 (JSON):
    {{
      "three_reason": [
        "첫 번째 요약 문장 (핵심 키워드는 **이렇게** 감싸기)",
        "두 번째 요약 문장",
        "세 번째 요약 문장"
      ],
      "detail_answer": "상세한 의학적 설명..."
    }}
    """
    
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=1 
        )
        
        content = response.choices[0].message.content
        llm_result = json.loads(content)
        
        # Merge Results
        final_result = {
            "decision": decision_text,
            "three_reason": llm_result.get("three_reason", [
                "대기질 정보를 분석하고 있습니다.",
                "잠시 후 다시 확인해주세요.",
                "문제가 지속되면 관리자에게 문의하세요."
            ]),
            "detail_answer": llm_result.get("detail_answer", "정보를 불러오는 중 문제가 발생했습니다."),
            "actionItems": action_items,
            "references": list(set([doc.get("source", "Unknown Source") for doc in relevant_docs])),
            # Add real-time air quality values for frontend display
            "pm25_value": air_data.get("pm25_value"),
            "o3_value": air_data.get("o3_value"),
            "pm10_value": air_data.get("pm10_value"),
            "no2_value": air_data.get("no2_value")
        }
        
        # [Step F] Save to Cache
        if db is not None and cache_key:
            try:
                await db[CACHE_COLLECTION].update_one(
                    {"_id": cache_key},
                    {"$set": {"data": final_result, "created_at": datetime.now(KST_TZ)}},
                    upsert=True
                )
                print(f"💾 Saved to cache: {cache_key}")
            except Exception as e:
                print(f"Error saving to cache: {e}")
                
        return final_result
        
    except Exception as e:
        print(f"Error calling OpenAI: {e}")
        # Fallback even if LLM fails, we satisfy the deterministic requirement
        return {
            "decision": decision_text,
            "three_reason": [
                "일시적인 오류로 상세 분석을 불러오지 못했습니다.",
                "하지만 **행동 지침**은 위와 같이 준수해주세요.",
                "문제가 지속되면 **관리자**에게 문의하세요."
            ],
            "detail_answer": "일시적인 오류로 상세 설명을 불러오지 못했습니다. 하지만 행동 지침은 위와 같이 준수해주세요.",
            "actionItems": action_items,
            "references": [],
            # Add real-time air quality values for frontend display
            "pm25_value": air_data.get("pm25_value"),
            "o3_value": air_data.get("o3_value"),
            "pm10_value": air_data.get("pm10_value"),
            "no2_value": air_data.get("no2_value")
        }

async def ingest_pdf(file_content: bytes, filename: str) -> Dict[str, Any]:
    """
    Ingest PDF content: Extract text -> Embed -> Store in DB.
    """
    import io
    from PyPDF2 import PdfReader

    if not vo_client:
        return {"status": "error", "message": "Voyage AI Client not initialized"}
    
    if db is None:
        return {"status": "error", "message": "Database not initialized"}

    try:
        # 1. Read PDF
        pdf_file = io.BytesIO(file_content)
        reader = PdfReader(pdf_file)
        
        extracted_text = ""
        documents_to_insert = []
        texts_to_embed = []
        
        # 2. Extract Text per Page (Chunking Strategy: 1 Page = 1 Doc for simplicity)
        print(f"📄 Processing PDF: {filename} ({len(reader.pages)} pages)")
        
        for i, page in enumerate(reader.pages):
            text = page.extract_text()
            if text and len(text.strip()) > 50: # Ignore empty/too short pages
                extracted_text += text + "\n"
                
                texts_to_embed.append(text)
                documents_to_insert.append({
                    "text": text,
                    "category": "pdf_upload",
                    "source": filename,
                    "page": i + 1,
                    "risk_level": "unknown", # Needs manual classification or LLM analysis
                    "created_at": datetime.now()
                })
        
        if not documents_to_insert:
            return {"status": "error", "message": "No extractable text found in PDF."}

        # 3. Embed Data
        print(f"🧠 Embedding {len(texts_to_embed)} pages with Voyage AI...")
        result = vo_client.embed(texts_to_embed, model="voyage-3-large", input_type="document")
        embeddings = result.embeddings
        
        for i, doc in enumerate(documents_to_insert):
            doc["embedding"] = embeddings[i]
            
        # 4. Insert into DB
        insert_result = await db[GUIDELINES_COLLECTION].insert_many(documents_to_insert)
        
        return {
            "status": "success",
            "message": f"Successfully ingested {len(insert_result.inserted_ids)} pages from {filename}",
            "inserted_ids": [str(id) for id in insert_result.inserted_ids]
        }
        
    except Exception as e:
        print(f"Error processing PDF: {e}")
        return {"status": "error", "message": str(e)}
