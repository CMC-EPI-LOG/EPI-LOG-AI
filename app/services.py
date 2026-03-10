import os
import json
import csv
import asyncio
import html
import re
from datetime import datetime, timedelta
from time import perf_counter
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Any
from urllib.parse import urlparse
from pathlib import Path
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


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _bounded_int_env(name: str, default: int, *, min_value: int, max_value: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw.strip())
    except Exception:
        print(f"⚠️ Invalid int env {name}={raw!r}; using default={default}")
        return default

    if value < min_value:
        print(f"⚠️ {name}={value} is below min={min_value}; clamping to {min_value}")
        return min_value
    if value > max_value:
        print(f"⚠️ {name}={value} exceeds max={max_value}; clamping to {max_value}")
        return max_value
    return value


DB_NAME = os.getenv("MONGO_DB_NAME", "epilog_db")
AIR_QUALITY_DB_NAME = os.getenv("AIR_QUALITY_DB_NAME") or _infer_db_name_from_uri(MONGO_URI) or DB_NAME
AIR_QUALITY_FORECAST_DB_NAME = (
    os.getenv("AIR_QUALITY_FORECAST_DB_NAME")
    or "air_quality"
)
WEATHER_FORECAST_DB_NAME = os.getenv("WEATHER_FORECAST_DB_NAME", "weather_forecast")
WEATHER_FORECAST_READER_COLLECTION = os.getenv(
    "WEATHER_FORECAST_READER_COLLECTION",
    "weather_forecast_data_shadow",
)
AIR_QUALITY_FORECAST_COLLECTION = os.getenv(
    "AIRKOREA_FORECAST_LATEST_COLLECTION",
    os.getenv("AIR_QUALITY_FORECAST_COLLECTION", "air_quality_forecast_daily"),
)
AIR_QUALITY_FORECAST_RUNS_COLLECTION = os.getenv(
    "AIRKOREA_FORECAST_RUNS_COLLECTION",
    os.getenv("AIR_QUALITY_FORECAST_RUNS_COLLECTION", "ingest_runs_forecast"),
)
GUIDELINES_COLLECTION = "medical_guidelines"
AIR_QUALITY_COLLECTION = "daily_air_quality"
AIR_QUALITY_DATA_COLLECTION = "air_quality_data"  # Lambda cron job collection
OPS_METRICS_COLLECTION = os.getenv("OPS_METRICS_COLLECTION", "ops_advice_events")
VECTOR_INDEX = "vector_index"
KST_TZ = ZoneInfo("Asia/Seoul")
FORECAST_INGEST_STALE_THRESHOLD_MINUTES = _bounded_int_env(
    "FORECAST_INGEST_STALE_THRESHOLD_MINUTES",
    180,
    min_value=30,
    max_value=24 * 60,
)

if not MONGO_URI:
    # Fallback to a dummy URI if not set to prevent startup crash, but it will fail on request
    print("WARNING: MONGODB_URI is not set.")
    MONGO_URI = "mongodb://localhost:27017"

# Initialize Clients
try:
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    air_quality_db = mongo_client[AIR_QUALITY_DB_NAME]
    forecast_monitor_db = mongo_client[AIR_QUALITY_FORECAST_DB_NAME]
    print(
        "✅ MongoDB connected: "
        f"main_db={DB_NAME}, air_quality_db={AIR_QUALITY_DB_NAME}, "
        f"forecast_db={AIR_QUALITY_FORECAST_DB_NAME}"
    )
except Exception as e:
    print(f"Error initializing MongoDB client: {e}")
    mongo_client = None
    db = None
    air_quality_db = None
    forecast_monitor_db = None

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

    def add_dong_number_variant(token: str):
        """
        Expand Korean dong names like '대저1동' -> '대저동'.
        This helps when station metadata omits the numeric suffix.
        """
        if not token:
            return
        import re
        m = re.match(r"^(.+?)([0-9]+)동$", token)
        if not m:
            return
        base = (m.group(1) or "").strip()
        if base:
            add(f"{base}동")

    add(cleaned)
    add(cleaned.replace(" ", ""))

    tokens = cleaned.split(" ")
    if len(tokens) >= 2:
        add(tokens[-1])
        add_dong_number_variant(tokens[-1])
        add(tokens[-2])
        add(f"{tokens[-2]} {tokens[-1]}")
        # Also try numeric-dong normalized pair, e.g. '강서구 대저동'
        import re
        if re.match(r"^[0-9]+동$", tokens[-1]):
            # e.g. '대저 1동' -> '대저1동'
            combined = f"{tokens[-2]}{tokens[-1]}"
            add(combined)
            add_dong_number_variant(combined)
        m = re.match(r"^(.+?)([0-9]+)동$", tokens[-1])
        if m and m.group(1):
            add(f"{tokens[-2]} {m.group(1)}동")
    elif tokens:
        add(tokens[0])

    return candidates


def _dedupe_preserve(values: List[str]) -> List[str]:
    seen = set()
    deduped: List[str] = []
    for value in values:
        normalized = _normalize_whitespace(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _derive_station_resolution_status(
    requested_station: str,
    matched_candidate: Optional[str],
    resolved_station: Optional[str],
) -> str:
    requested = _normalize_whitespace(requested_station)
    matched = _normalize_whitespace(matched_candidate)
    resolved = _normalize_whitespace(resolved_station)

    if not resolved:
        return "unresolved"
    if matched and matched == requested:
        return "exact"
    if resolved == requested:
        return "exact"
    if matched or resolved:
        return "candidate_fallback"
    return "unresolved"


_SIDO_ALIASES = {
    # Special cities
    "서울": "서울",
    "서울특별시": "서울",
    "부산": "부산",
    "부산광역시": "부산",
    "대구": "대구",
    "대구광역시": "대구",
    "인천": "인천",
    "인천광역시": "인천",
    "광주": "광주",
    "광주광역시": "광주",
    "대전": "대전",
    "대전광역시": "대전",
    "울산": "울산",
    "울산광역시": "울산",
    "세종": "세종",
    "세종특별자치시": "세종",
    # Provinces
    "경기": "경기",
    "경기도": "경기",
    "강원": "강원",
    "강원도": "강원",
    "충북": "충북",
    "충청북도": "충북",
    "충남": "충남",
    "충청남도": "충남",
    "전북": "전북",
    "전라북도": "전북",
    "전남": "전남",
    "전라남도": "전남",
    "경북": "경북",
    "경상북도": "경북",
    "경남": "경남",
    "경상남도": "경남",
    "제주": "제주",
    "제주특별자치도": "제주",
}


def _infer_preferred_sido_from_text(text: str) -> Optional[str]:
    """
    Infer a preferred `sidoName` (city/province) from a user-provided station/address string.
    Example: '부산 강서구 대저1동' -> '부산'
    """
    if not text:
        return None
    cleaned = " ".join(str(text).strip().split())
    if not cleaned:
        return None

    # Prefer prefix token (most common form: '<sido> <sigungu> ...')
    tokens = cleaned.split(" ")
    for token in tokens[:2]:
        if token in _SIDO_ALIASES:
            return _SIDO_ALIASES[token]

    # Fallback: substring scan for long-form names
    for key, canonical in _SIDO_ALIASES.items():
        if key and key in cleaned:
            return canonical

    return None


def _sido_name_variants(canonical: str) -> List[str]:
    if not canonical:
        return []
    variants = {canonical}
    # Add common full names used by different data sources.
    full_map = {
        "서울": "서울특별시",
        "부산": "부산광역시",
        "대구": "대구광역시",
        "인천": "인천광역시",
        "광주": "광주광역시",
        "대전": "대전광역시",
        "울산": "울산광역시",
        "세종": "세종특별자치시",
        "경기": "경기도",
        "강원": "강원도",
        "충북": "충청북도",
        "충남": "충청남도",
        "전북": "전라북도",
        "전남": "전라남도",
        "경북": "경상북도",
        "경남": "경상남도",
        "제주": "제주특별자치도",
    }
    if canonical in full_map:
        variants.add(full_map[canonical])
    return sorted(variants)


def _grade_from_value(pollutant: str, value: Any) -> Optional[str]:
    """
    Derive Korean grade text from raw values for consistency checks.
    Returns one of: '좋음', '보통', '나쁨', '매우나쁨', or None if unknown.
    """
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return None

    p = (pollutant or "").lower().strip()
    # Breakpoints reflect commonly used AirKorea-style 4-step guidance.
    if p == "pm25":
        if v <= 15: return "좋음"
        if v <= 35: return "보통"
        if v <= 75: return "나쁨"
        return "매우나쁨"
    if p == "pm10":
        if v <= 30: return "좋음"
        if v <= 80: return "보통"
        if v <= 150: return "나쁨"
        return "매우나쁨"
    if p == "o3":
        # ppm
        if v <= 0.03: return "좋음"
        if v <= 0.09: return "보통"
        if v <= 0.15: return "나쁨"
        return "매우나쁨"
    if p == "no2":
        # ppm
        if v <= 0.03: return "좋음"
        if v <= 0.06: return "보통"
        if v <= 0.20: return "나쁨"
        return "매우나쁨"
    if p == "co":
        # ppm
        if v <= 2.0: return "좋음"
        if v <= 9.0: return "보통"
        if v <= 15.0: return "나쁨"
        return "매우나쁨"
    if p == "so2":
        # ppm
        if v <= 0.02: return "좋음"
        if v <= 0.05: return "보통"
        if v <= 0.15: return "나쁨"
        return "매우나쁨"
    return None


def _max_korean_grade(*grades: Optional[str]) -> str:
    best = "좋음"
    best_score = 1
    for g in grades:
        if not g:
            continue
        s = GRADE_MAP.get(g, 2)
        if s > best_score:
            best_score = s
            best = g
    return best


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


def _coerce_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def get_clothing_recommendation(temp: Any, humidity: Any) -> Dict[str, Any]:
    """
    Return deterministic clothing guidance based on current temperature/humidity.
    """
    temperature = _coerce_number(temp)
    humid = _coerce_number(humidity)

    if temperature is None:
        temperature = 22.0
    if humid is None:
        humid = 45.0

    if temperature < -5:
        comfort_level = "FREEZING"
        recommendation = "패딩 + 두꺼운 니트 + 내복 + 목도리/장갑"
        summary = "한파 수준이에요. 방한 장비를 최대치로 준비하세요."
    elif temperature < 5:
        comfort_level = "COLD"
        recommendation = "두꺼운 코트/패딩 + 기모 상의 + 긴바지"
        summary = "매우 추워요. 보온 중심 레이어링이 필요해요."
    elif temperature < 12:
        comfort_level = "CHILLY"
        recommendation = "트렌치/자켓 + 긴팔 상의 + 긴바지"
        summary = "쌀쌀한 편이에요. 가벼운 겉옷을 꼭 챙기세요."
    elif temperature < 20:
        comfort_level = "MILD"
        recommendation = "가디건/맨투맨 + 긴바지"
        summary = "활동하기 무난한 기온이에요."
    elif temperature < 27:
        comfort_level = "WARM"
        recommendation = "반팔 + 얇은 셔츠(또는 가디건) + 통풍 좋은 하의"
        summary = "다소 따뜻해요. 얇고 통풍 잘되는 옷이 좋아요."
    else:
        comfort_level = "HOT"
        recommendation = "반팔 + 반바지/얇은 바지 + 통풍 좋은 소재"
        summary = "더운 날씨예요. 열 배출이 잘되는 복장이 좋아요."

    tips: List[str] = []

    if humid >= 75:
        tips.append("습도가 높아요. 땀 배출이 잘되는 기능성/면 혼방 소재를 권장해요.")
        if temperature >= 25:
            tips.append("더위 체감이 커질 수 있어요. 여벌 옷을 준비해 주세요.")
    elif humid <= 30:
        tips.append("건조한 편이에요. 얇은 겉옷으로 피부 건조와 냉기를 함께 관리하세요.")
    else:
        tips.append("현재 습도는 비교적 안정적이에요. 활동량에 맞춰 한 겹 조절하면 좋아요.")

    if temperature <= 5:
        tips.append("실내외 온도차가 큰 날이에요. 탈착 가능한 겉옷 구성이 안전해요.")
    elif temperature >= 28:
        tips.append("실외 활동 시 밝은 색, 통풍 좋은 소재를 추천해요.")

    return {
        "summary": summary,
        "recommendation": recommendation,
        "tips": tips[:3],
        "comfortLevel": comfort_level,
        "temperature": round(temperature, 1),
        "humidity": round(humid, 1),
        "source": "rule-based-v1"
    }


AGE_GROUP_LABELS = {
    "infant": "영아(0-2세)",
    "toddler": "유아(3-6세)",
    "elementary_low": "초등저(7-9세)",
    "elementary_high": "초등고(10-12세)",
    "teen_adult": "청소년/성인",
}

CONDITION_LABELS = {
    "general": "일반",
    "rhinitis": "비염",
    "asthma": "천식",
    "atopy": "아토피",
}

VALID_COMFORT_LEVELS = {"FREEZING", "COLD", "CHILLY", "MILD", "WARM", "HOT"}


def _normalize_condition_key(condition: Any) -> str:
    if condition is None:
        return "general"
    raw = str(condition).strip().lower()
    if not raw:
        return "general"

    alias_map = {
        "general": {"general", "일반", "none", "없음", "healthy", "normal", "건강함"},
        "rhinitis": {"rhinitis", "비염", "allergic_rhinitis", "allergy"},
        "asthma": {"asthma", "천식"},
        "atopy": {"atopy", "아토피", "eczema"},
    }
    for key, aliases in alias_map.items():
        if raw in aliases:
            return key

    if "비염" in raw or "rhinitis" in raw:
        return "rhinitis"
    if "천식" in raw or "asthma" in raw:
        return "asthma"
    if "아토피" in raw or "atopy" in raw or "eczema" in raw:
        return "atopy"
    return "general"


def _normalize_grade_label(value: Any, default: Optional[str] = "보통") -> Optional[str]:
    if value is None:
        return default
    raw = str(value).strip()
    if not raw:
        return default

    compact = raw.lower().replace(" ", "").replace("-", "").replace("_", "")
    alias_map = {
        "1": "좋음",
        "good": "좋음",
        "좋음": "좋음",
        "2": "보통",
        "moderate": "보통",
        "normal": "보통",
        "보통": "보통",
        "3": "나쁨",
        "bad": "나쁨",
        "poor": "나쁨",
        "나쁨": "나쁨",
        "4": "매우나쁨",
        "verybad": "매우나쁨",
        "verypoor": "매우나쁨",
        "매우나쁨": "매우나쁨",
    }
    return alias_map.get(compact, default)


def _resolve_air_grades(air_quality: Optional[Dict[str, Any]]) -> Dict[str, str]:
    payload = air_quality or {}
    overall = _normalize_grade_label(payload.get("grade"), default=None)
    pm25 = _normalize_grade_label(payload.get("pm25Grade"), default=None)
    pm10 = _normalize_grade_label(payload.get("pm10Grade"), default=None)
    o3 = _normalize_grade_label(payload.get("o3Grade"), default=None)

    if overall is None:
        known_grades = [g for g in [pm25, pm10, o3] if g]
        if known_grades:
            overall = max(known_grades, key=lambda grade: GRADE_MAP.get(grade, 2))
        else:
            overall = "보통"

    return {
        "overall": overall,
        "pm25": pm25 or overall,
        "pm10": pm10 or overall,
        "o3": o3 or overall,
    }


def get_ai_clothing_recommendation(
    temp: Any,
    humidity: Any,
    user_profile: Optional[Dict[str, Any]] = None,
    air_quality: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    fallback_result = get_clothing_recommendation(temp, humidity)
    user_profile = user_profile or {}
    air_quality = air_quality or {}

    has_profile_input = bool(user_profile.get("ageGroup")) or bool(user_profile.get("condition"))
    has_air_input = any(air_quality.get(key) for key in ("grade", "pm25Grade", "pm10Grade", "o3Grade"))
    if not (has_profile_input and has_air_input):
        return fallback_result

    if not openai_client:
        fallback_no_ai = dict(fallback_result)
        fallback_no_ai["source"] = "rule-based-fallback-no-openai"
        return fallback_no_ai

    age_group_key = _normalize_age_group(user_profile.get("ageGroup"))
    condition_key = _normalize_condition_key(user_profile.get("condition"))
    age_group_label = AGE_GROUP_LABELS.get(age_group_key, AGE_GROUP_LABELS["elementary_high"])
    condition_label = CONDITION_LABELS.get(condition_key, CONDITION_LABELS["general"])
    air_grades = _resolve_air_grades(air_quality)

    system_prompt = """
    너는 날씨/대기질/연령/기저질환을 함께 반영해 실용적인 옷차림을 추천하는 AI 스타일 코치다.
    출력은 반드시 JSON 객체 하나만 반환한다.
    필수 키:
    - summary: 한 문장 요약
    - recommendation: 핵심 착장 한 문장
    - tips: 1~3개의 짧은 실천 팁 배열
    - comfortLevel: FREEZING|COLD|CHILLY|MILD|WARM|HOT 중 하나
    규칙:
    - 입력 수치와 모순된 추천을 하지 말 것
    - 과도한 의료 지시는 피할 것
    - 한국어로 간결하고 즉시 실행 가능하게 작성할 것
    """

    user_prompt = f"""
    [입력]
    - 기온: {fallback_result.get("temperature")}°C
    - 습도: {fallback_result.get("humidity")}%
    - 연령대: {age_group_label}
    - 기저질환: {condition_label}
    - 대기등급(종합): {air_grades["overall"]}
    - PM2.5 등급: {air_grades["pm25"]}
    - PM10 등급: {air_grades["pm10"]}
    - 오존(O3) 등급: {air_grades["o3"]}

    [규칙기반 참고안]
    - summary: {fallback_result.get("summary")}
    - recommendation: {fallback_result.get("recommendation")}
    - tips: {fallback_result.get("tips")}
    - comfortLevel: {fallback_result.get("comfortLevel")}

    위 정보를 바탕으로 개인화된 옷차림을 생성해줘.
    """

    try:
        response = openai_client.chat.completions.create(
            model=CLOTHING_LLM_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or "{}"
        llm_result = json.loads(content)

        summary = str(llm_result.get("summary", "")).strip()
        recommendation = str(llm_result.get("recommendation", "")).strip()
        raw_tips = llm_result.get("tips")
        tips = []
        if isinstance(raw_tips, list):
            for item in raw_tips:
                text = str(item).strip()
                if text:
                    tips.append(text)
        comfort_level = str(llm_result.get("comfortLevel", "")).strip().upper()

        if not summary or not recommendation or not tips:
            raise ValueError("LLM response missing required clothing fields")
        if comfort_level not in VALID_COMFORT_LEVELS:
            comfort_level = fallback_result.get("comfortLevel", "MILD")

        return {
            "summary": summary,
            "recommendation": recommendation,
            "tips": tips[:3],
            "comfortLevel": comfort_level,
            "temperature": fallback_result.get("temperature"),
            "humidity": fallback_result.get("humidity"),
            "source": "ai-dynamic-v1"
        }
    except Exception as e:
        print(f"Error generating AI clothing recommendation: {e}")
        fallback_error = dict(fallback_result)
        fallback_error["source"] = "rule-based-fallback-on-error"
        return fallback_error


CSV_AGE_MAP = {
    "영아(0-2세)": "infant",
    "유아(3-6세)": "toddler",
    "초등저(7-9세)": "elementary_low",
    "초등고(10-12)": "elementary_high",
    "청소년/성인": "teen_adult",
}

CSV_CONDITION_MAP = {
    "일반": "general",
    "비염": "rhinitis",
    "천식": "asthma",
    "아토피": "atopy",
}

GRADE_ORDER = ("좋음", "보통", "나쁨", "매우나쁨")


def _load_decision_matrix_from_csv() -> Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]:
    """
    Load the full 80-row decision matrix from logic.csv.
    Matrix shape: age_group -> condition -> grade -> {text, reason, actions}
    """
    csv_path = os.getenv("DECISION_LOGIC_CSV_PATH")
    if csv_path:
        path = Path(csv_path).expanduser()
    else:
        path = Path(__file__).resolve().parents[1] / "logic.csv"

    if not path.exists():
        print(f"⚠️ decision matrix CSV not found: {path}")
        return {}

    matrix: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}
    loaded_rows = 0

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                age_key = CSV_AGE_MAP.get((row.get("연령대") or "").strip())
                cond_key = CSV_CONDITION_MAP.get((row.get("질환군") or "").strip())
                grade_key = (row.get("대기등급") or "").strip()

                if not age_key or not cond_key or grade_key not in GRADE_ORDER:
                    continue

                action_items: List[str] = []
                for action_col in ("행동1", "행동2", "행동3"):
                    action_value = (row.get(action_col) or "").strip()
                    if action_value:
                        action_items.append(action_value)

                matrix.setdefault(age_key, {}).setdefault(cond_key, {})[grade_key] = {
                    "text": (row.get("메인문구") or "").strip(),
                    "reason": (row.get("이유") or "").strip(),
                    "actions": action_items,
                }
                loaded_rows += 1
    except Exception as e:
        print(f"⚠️ failed to load decision matrix CSV: {e}")
        return {}

    expected_rows = len(CSV_AGE_MAP) * len(CSV_CONDITION_MAP) * len(GRADE_ORDER)
    if loaded_rows != expected_rows:
        print(
            f"⚠️ decision matrix coverage mismatch: loaded={loaded_rows}, expected={expected_rows}, path={path}"
        )
    else:
        print(f"✅ decision matrix loaded: {loaded_rows} rows from {path}")

    return matrix


DECISION_MATRIX = _load_decision_matrix_from_csv()

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
    Legacy helper for 3-level decision key.
    """
    p_score = GRADE_MAP.get(pm25_grade, 2)
    o_score = GRADE_MAP.get(o3_grade, 2)
    worst = max(p_score, o_score)
    if worst >= 4:
        return "warning"
    if worst == 3:
        return "caution"
    return "ok"


def _calculate_final_grade(pm25_grade: str, pm10_grade: Optional[str], o3_grade: str) -> str:
    return _max_korean_grade(pm25_grade, pm10_grade, o3_grade)


def _escalate_grade_score(score: int) -> int:
    return min(4, max(1, int(score) + 1))


def _grade_to_legacy_decision_key(grade: str) -> str:
    score = GRADE_MAP.get(grade, 2)
    if score >= 4:
        return "warning"
    if score == 3:
        return "caution"
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

def _get_display_content(age_group: str, condition: str, final_grade: str):
    """
    Returns (decision_text, action_items, reason_text)
    """
    # Normalize condition
    cond_key = condition if condition in ["general", "rhinitis", "asthma", "atopy"] else "general"

    # Primary: full 80-row CSV decision matrix (4 grades).
    matrix_age_key = age_group if age_group in DECISION_MATRIX else "elementary_high"
    group_data = DECISION_MATRIX.get(matrix_age_key, {})
    cond_data = group_data.get(cond_key, group_data.get("general", {}))
    entry = cond_data.get(final_grade)

    if entry:
        d_text = (entry.get("text") or "").strip() or "상태 확인 필요"
        reason = (entry.get("reason") or "").strip()
        actions = entry.get("actions") if isinstance(entry.get("actions"), list) else []
        if not actions:
            actions = ["상태에 따른 주의가 필요합니다."]
        return d_text, actions[:], reason

    # Fallback: legacy in-code 60-slot table.
    decision_key = _grade_to_legacy_decision_key(final_grade)
    legacy_text_by_age = DECISION_TEXTS.get(age_group, DECISION_TEXTS["elementary_high"])
    legacy_text_by_cond = legacy_text_by_age.get(cond_key, legacy_text_by_age.get("general", {}))
    d_text = legacy_text_by_cond.get(decision_key, "상태 확인 필요")

    legacy_actions_by_age = ACTION_ITEMS.get(age_group, ACTION_ITEMS.get("toddler", {}))
    legacy_actions_by_cond = legacy_actions_by_age.get(cond_key, legacy_actions_by_age.get("general", {}))
    actions = legacy_actions_by_cond.get(decision_key, ["상태에 따른 주의가 필요합니다."])

    return d_text, actions[:], ""

try:
    vo_client = voyageai.Client(api_key=VOYAGE_API_KEY)
except Exception as e:
    print(f"Error initializing Voyage AI client: {e}")
    vo_client = None

OPENAI_MAX_RETRIES = _bounded_int_env(
    "OPENAI_MAX_RETRIES",
    0,
    min_value=0,
    max_value=3,
)
ADVICE_LLM_MODEL = (os.getenv("ADVICE_LLM_MODEL") or "gpt-4.1-nano").strip() or "gpt-4.1-nano"
CLOTHING_LLM_MODEL = (os.getenv("CLOTHING_LLM_MODEL") or ADVICE_LLM_MODEL).strip() or ADVICE_LLM_MODEL

try:
    openai_client = OpenAI(api_key=OPENAI_API_KEY, max_retries=OPENAI_MAX_RETRIES)
except Exception as e:
    print(f"Error initializing OpenAI client: {e}")
    openai_client = None

ADVICE_VECTOR_SEARCH_ENABLED = _env_flag("ADVICE_VECTOR_SEARCH_ENABLED", False)
_vector_search_enabled = bool(ADVICE_VECTOR_SEARCH_ENABLED and vo_client and VOYAGE_API_KEY)
_vector_search_skip_notice_emitted = False

if ADVICE_VECTOR_SEARCH_ENABLED and not VOYAGE_API_KEY:
    print("⚠️ Vector search requested but VOYAGE_API_KEY is missing. Running without vector search.")
elif ADVICE_VECTOR_SEARCH_ENABLED and not vo_client:
    print("⚠️ Vector search requested but Voyage client is unavailable. Running without vector search.")
elif not ADVICE_VECTOR_SEARCH_ENABLED:
    print("ℹ️ Vector search disabled by config (ADVICE_VECTOR_SEARCH_ENABLED=0).")

async def get_air_quality_from_mongodb(station_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch latest air quality data from MongoDB air_quality_data collection.
    This collection is populated by AWS Lambda cron job every hour.
    Returns None if no recent data (> 2 hours old) or not found.
    """
    if air_quality_db is None:
        return None
    
    try:
        station_name_cleaned = " ".join((station_name or "").strip().split())
        first_token = station_name_cleaned.split(" ")[0] if station_name_cleaned else ""
        explicit_sido_prefix = first_token in _SIDO_ALIASES

        preferred_sido = _infer_preferred_sido_from_text(station_name)
        preferred_sido_variants = _sido_name_variants(preferred_sido) if preferred_sido else []

        station_candidates = _normalize_station_candidates(station_name)
        if not station_candidates:
            return None

        # Latest-first sort strategy:
        # 1) dataTime (AirKorea snapshot time), 2) updatedAt (ingest time), 3) _id (insert order)
        sort_criteria = [("dataTime", -1), ("updatedAt", -1), ("_id", -1)]
        data = None
        matched_candidate = None

        for candidate in station_candidates:
            if preferred_sido_variants:
                data = await air_quality_db[AIR_QUALITY_DATA_COLLECTION].find_one(
                    {"stationName": candidate, "sidoName": {"$in": preferred_sido_variants}},
                    sort=sort_criteria
                )
                if data:
                    matched_candidate = candidate
                    break
                # If the user explicitly specified a city/province (e.g., starts with '부산'),
                # do not silently fall back to a different region with the same stationName.
                if explicit_sido_prefix:
                    continue

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

        # Support both camelCase (Lambda docs) and snake_case (legacy docs)
        pm25_value = _coerce_number(data.get("pm25Value", data.get("pm25_value")))
        pm10_value = _coerce_number(data.get("pm10Value", data.get("pm10_value")))
        o3_value = _coerce_number(data.get("o3Value", data.get("o3_value")))
        no2_value = _coerce_number(data.get("no2Value", data.get("no2_value")))
        co_value = _coerce_number(data.get("coValue", data.get("co_value")))
        so2_value = _coerce_number(data.get("so2Value", data.get("so2_value")))

        # We intentionally do NOT trust stored Grade fields because they can drift from Value.
        # Use them only when a numeric value is missing.
        pm25_grade_value = data.get("pm25Grade", data.get("pm25_grade"))
        pm10_grade_value = data.get("pm10Grade", data.get("pm10_grade"))
        o3_grade_value = data.get("o3Grade", data.get("o3_grade"))
        no2_grade_value = data.get("no2Grade", data.get("no2_grade"))
        co_grade_value = data.get("coGrade", data.get("co_grade"))
        so2_grade_value = data.get("so2Grade", data.get("so2_grade"))

        temp_raw = data.get("temperature")
        if temp_raw is None:
            temp_raw = data.get("temp")
        humidity_raw = data.get("humidity")

        temp_value = _coerce_number(temp_raw)
        humidity_value = _coerce_number(humidity_raw)

        def _grade_from_value_or_fallback(pollutant: str, value: Optional[float], fallback_grade_value: Any, default: str) -> str:
            computed = _grade_from_value(pollutant, value)
            if computed:
                return computed
            if fallback_grade_value is None:
                return default
            return grade_map.get(str(fallback_grade_value), default)

        result = {
            "requestedStation": station_name,
            "matchedCandidate": matched_candidate,
            "resolvedStation": data.get("stationName", station_name),
            "stationName": data.get("stationName", station_name),
            "sidoName": data.get("sidoName"),
            "pm25_value": pm25_value if pm25_value is not None else 50,
            "pm10_value": pm10_value if pm10_value is not None else 70,
            "o3_value": o3_value if o3_value is not None else 0.05,
            "no2_value": no2_value if no2_value is not None else 0.02,
            "co_value": co_value if co_value is not None else 0.5,
            "so2_value": so2_value if so2_value is not None else 0.003,
            "pm25_grade": _grade_from_value_or_fallback("pm25", pm25_value, pm25_grade_value, "보통"),
            "pm10_grade": _grade_from_value_or_fallback("pm10", pm10_value, pm10_grade_value, "보통"),
            "o3_grade": _grade_from_value_or_fallback("o3", o3_value, o3_grade_value, "좋음"),
            "no2_grade": _grade_from_value_or_fallback("no2", no2_value, no2_grade_value, "좋음"),
            "co_grade": _grade_from_value_or_fallback("co", co_value, co_grade_value, "좋음"),
            "so2_grade": _grade_from_value_or_fallback("so2", so2_value, so2_grade_value, "좋음"),
            # Lambda now stores weather on the same document.
            # Support both `temperature` (Lambda) and `temp` (legacy/other sources).
            "temp": temp_value,
            "humidity": humidity_value,
            "dataTime": data.get("dataTime"),
            "source": "mongodb_air",
            "weatherSource": "air_document" if temp_value is not None and humidity_value is not None else "missing",
            "stationResolutionStatus": _derive_station_resolution_status(
                station_name_cleaned,
                matched_candidate,
                data.get("stationName", station_name),
            ),
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
        station_name_cleaned = " ".join((station_name or "").strip().split())
        first_token = station_name_cleaned.split(" ")[0] if station_name_cleaned else ""
        explicit_sido_prefix = first_token in _SIDO_ALIASES

        preferred_sido = _infer_preferred_sido_from_text(station_name)
        preferred_sido_variants = _sido_name_variants(preferred_sido) if preferred_sido else []

        async with httpx.AsyncClient(timeout=10.0) as client:
            station_candidates = _normalize_station_candidates(station_name)
            if not station_candidates:
                station_candidates = [station_name]

            # Try a few candidate queries to avoid false misses when users pass full addresses.
            for candidate in station_candidates[:6]:
                response = await client.get(
                    AIRKOREA_API_URL,
                    params={"stationName": candidate}
                )

                if response.status_code != 200:
                    continue

                data = response.json()
                if not data:
                    continue

                # API returns array; when ambiguous (e.g., '강서구'), prefer matching sidoName.
                station = None
                if preferred_sido_variants:
                    for item in data:
                        if item.get("sidoName") in preferred_sido_variants:
                            station = item
                            break
                    if station is None and explicit_sido_prefix:
                        # Explicit city/province prefix but no matching station in that region.
                        # Try next candidate rather than returning a wrong-region station.
                        continue
                if station is None:
                    station = data[0]

                realtime = station.get("realtime", {})

                # We do NOT trust `grade` from upstream. Recompute grade from numeric values.

                # Extract and normalize data
                pm25_value = realtime.get("pm25", {}).get("value")
                pm10_value = realtime.get("pm10", {}).get("value")
                o3_value = realtime.get("o3", {}).get("value")
                no2_value = realtime.get("no2", {}).get("value")
                co_value = realtime.get("co", {}).get("value")
                so2_value = realtime.get("so2", {}).get("value")

                result = {
                    "requestedStation": station_name,
                    "matchedCandidate": candidate,
                    "resolvedStation": station.get("stationName", candidate),
                    "stationName": station.get("stationName", candidate),
                    "sidoName": station.get("sidoName"),
                    "pm25_value": pm25_value or 50,
                    "pm10_value": pm10_value or 70,
                    "o3_value": o3_value or 0.05,
                    "no2_value": no2_value or 0.02,
                    "co_value": co_value or 0.5,
                    "so2_value": so2_value or 0.003,
                    "pm25_grade": _grade_from_value("pm25", pm25_value) or "보통",
                    "pm10_grade": _grade_from_value("pm10", pm10_value) or "보통",
                    "o3_grade": _grade_from_value("o3", o3_value) or "보통",
                    "no2_grade": _grade_from_value("no2", no2_value) or "좋음",
                    "co_grade": _grade_from_value("co", co_value) or "좋음",
                    "so2_grade": _grade_from_value("so2", so2_value) or "좋음",
                    "temp": None,
                    "humidity": None,
                    "dataTime": realtime.get("dataTime") or station.get("dataTime"),
                    "source": "airkorea_api",
                    "weatherSource": "missing",
                    "stationResolutionStatus": _derive_station_resolution_status(
                        station_name_cleaned,
                        candidate,
                        station.get("stationName", candidate),
                    ),
                }

                print(f"✅ Fetched air quality for {station_name} from Air Korea API (fallback, matched={candidate})")
                return result
        
        print(f"⚠️  No data from Air Korea API for {station_name}")
        return None
        
    except Exception as e:
        print(f"❌ Error fetching from Air Korea API: {e}")
        return None


def _parse_weather_data_time_to_kst(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None

    text = _normalize_whitespace(raw)
    if not text:
        return None

    compact_match = re.match(r"^(\d{4})(\d{2})(\d{2})\s+(\d{2})(\d{2})$", text)
    if compact_match:
        year, month, day, hour, minute = compact_match.groups()
        return datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            tzinfo=KST_TZ,
        )

    return _parse_datetime_to_kst(text)


def _parse_weather_forecast_at_to_kst(doc: Dict[str, Any]) -> Optional[datetime]:
    forecast_at_utc = doc.get("forecastAtUtc")
    if forecast_at_utc:
        parsed = _parse_datetime_to_kst(forecast_at_utc)
        if parsed:
            return parsed

    parsed_data_time = _parse_weather_data_time_to_kst(doc.get("dataTime"))
    if parsed_data_time:
        return parsed_data_time

    fcst_date = _normalize_whitespace(doc.get("fcstDate"))
    fcst_time = _normalize_whitespace(doc.get("fcstTime")).zfill(4)
    if len(fcst_date) == 8 and len(fcst_time) == 4 and fcst_date.isdigit() and fcst_time.isdigit():
        return datetime(
            int(fcst_date[:4]),
            int(fcst_date[4:6]),
            int(fcst_date[6:8]),
            int(fcst_time[:2]),
            int(fcst_time[2:4]),
            tzinfo=KST_TZ,
        )

    forecast_date = _normalize_whitespace(doc.get("forecastDate"))
    forecast_hour = doc.get("forecastHour")
    forecast_hour_number = _coerce_number(forecast_hour)
    if len(forecast_date) == 8 and forecast_date.isdigit() and forecast_hour_number is not None:
        return datetime(
            int(forecast_date[:4]),
            int(forecast_date[4:6]),
            int(forecast_date[6:8]),
            int(round(forecast_hour_number)),
            0,
            tzinfo=KST_TZ,
        )

    return None


async def get_weather_from_mongodb(
    station_name: str,
    *,
    resolved_station: Optional[str] = None,
    preferred_sido: Optional[str] = None,
    additional_candidates: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    if mongo_client is None:
        return None

    candidates = _dedupe_preserve([
        station_name,
        resolved_station or "",
        *(_normalize_station_candidates(station_name) or []),
        *(_normalize_station_candidates(resolved_station or "") or []),
        *(additional_candidates or []),
    ])
    if not candidates:
        return None

    preferred_sido_value = preferred_sido or _infer_preferred_sido_from_text(station_name)
    preferred_sido_variants = _sido_name_variants(preferred_sido_value) if preferred_sido_value else []

    weather_collection = mongo_client[WEATHER_FORECAST_DB_NAME][WEATHER_FORECAST_READER_COLLECTION]
    now_kst = datetime.now(KST_TZ)
    start_key = (now_kst - timedelta(days=1)).strftime("%Y%m%d")
    end_key = (now_kst + timedelta(days=1)).strftime("%Y%m%d")
    projection = {
        "_id": 0,
        "stationName": 1,
        "sidoName": 1,
        "forecastDate": 1,
        "forecastHour": 1,
        "fcstDate": 1,
        "fcstTime": 1,
        "dataTime": 1,
        "temperature": 1,
        "humidity": 1,
        "updatedAt": 1,
        "forecastAtUtc": 1,
    }

    docs = await weather_collection.find(
        {
            "stationName": {"$in": candidates},
            "forecastDate": {"$gte": start_key, "$lte": end_key},
        },
        projection,
    ).sort([("updatedAt", -1), ("forecastDate", 1), ("forecastHour", 1)]).to_list(length=1200)

    if not docs:
        regex_clauses = [{"stationName": {"$regex": re.escape(candidate)}} for candidate in candidates[:6]]
        docs = await weather_collection.find(
            {
                "forecastDate": {"$gte": start_key, "$lte": end_key},
                "$or": regex_clauses,
            },
            projection,
        ).sort([("updatedAt", -1), ("forecastDate", 1), ("forecastHour", 1)]).to_list(length=1200)

    if not docs:
        return None

    filtered_docs: List[Dict[str, Any]] = []
    now_timestamp = now_kst.timestamp()
    deduped: Dict[str, Dict[str, Any]] = {}

    for doc in docs:
        station_value = _normalize_whitespace(doc.get("stationName"))
        if not station_value:
            continue

        if preferred_sido_variants and doc.get("sidoName") not in preferred_sido_variants:
            continue

        temperature = _coerce_number(doc.get("temperature"))
        humidity = _coerce_number(doc.get("humidity"))
        if temperature is None and humidity is None:
            continue

        forecast_at = _parse_weather_forecast_at_to_kst(doc)
        if forecast_at is None:
            continue

        dedupe_key = f"{station_value}|{forecast_at.isoformat()}"
        updated_at = _parse_datetime_to_kst(doc.get("updatedAt"))
        existing = deduped.get(dedupe_key)
        if existing:
            existing_updated_at = _parse_datetime_to_kst(existing.get("updatedAt"))
            if existing_updated_at and updated_at and updated_at <= existing_updated_at:
                continue

        candidate_doc = dict(doc)
        candidate_doc["temperature"] = temperature
        candidate_doc["humidity"] = humidity
        candidate_doc["forecastAtKst"] = forecast_at.isoformat()
        candidate_doc["forecastAtTs"] = forecast_at.timestamp()
        deduped[dedupe_key] = candidate_doc

    filtered_docs = list(deduped.values())
    if not filtered_docs:
        return None

    def candidate_matches(candidate: str, station_value: str) -> bool:
        normalized_candidate = _normalize_whitespace(candidate)
        normalized_station = _normalize_whitespace(station_value)
        if not normalized_candidate or not normalized_station:
            return False
        return (
            normalized_candidate == normalized_station
            or normalized_candidate in normalized_station
            or normalized_station in normalized_candidate
        )

    def choose_best(items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not items:
            return None
        return sorted(
            items,
            key=lambda item: (
                abs(item["forecastAtTs"] - now_timestamp),
                0 if item["forecastAtTs"] >= now_timestamp else 1,
                -(_parse_datetime_to_kst(item.get("updatedAt")) or now_kst).timestamp(),
            ),
        )[0]

    for candidate in candidates:
        matching_items = [
            item for item in filtered_docs if candidate_matches(candidate, item.get("stationName", ""))
        ]
        best = choose_best(matching_items)
        if not best:
            continue
        return {
            "temp": best.get("temperature"),
            "humidity": best.get("humidity"),
            "weatherSource": "weather_forecast_db",
            "weatherMatchedStation": best.get("stationName"),
            "weatherForecastAt": best.get("forecastAtKst"),
        }

    best = choose_best(filtered_docs)
    if not best:
        return None

    return {
        "temp": best.get("temperature"),
        "humidity": best.get("humidity"),
        "weatherSource": "weather_forecast_db",
        "weatherMatchedStation": best.get("stationName"),
        "weatherForecastAt": best.get("forecastAtKst"),
    }

async def get_air_quality(station_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch air quality data with priority order:
    1. MongoDB air_quality_data (Lambda cron job data) - PRIORITY
    2. Air Korea OpenAPI (fallback for real-time data)
    3. Mock data (final fallback)
    
    Note: Temperature and humidity are expected to be present in the Lambda-stored MongoDB document.
    If missing, the API returns default placeholders.
    """
    data = await get_air_quality_from_mongodb(station_name)

    if not data:
        data = await get_air_quality_from_airkorea_api(station_name)

    if data:
        preferred_sido = data.get("sidoName") or _infer_preferred_sido_from_text(station_name)
        weather = await get_weather_from_mongodb(
            station_name,
            resolved_station=data.get("resolvedStation") or data.get("stationName"),
            preferred_sido=preferred_sido,
            additional_candidates=[
                data.get("stationName") or "",
                data.get("matchedCandidate") or "",
            ],
        )
        if weather:
            if weather.get("temp") is not None:
                data["temp"] = weather.get("temp")
            if weather.get("humidity") is not None:
                data["humidity"] = weather.get("humidity")
            data["weatherSource"] = weather.get("weatherSource", "weather_forecast_db")
            data["weatherMatchedStation"] = weather.get("weatherMatchedStation")
            data["weatherForecastAt"] = weather.get("weatherForecastAt")

        defaulted_weather = False
        if data.get("temp") is None:
            data["temp"] = 22.0
            defaulted_weather = True
        if data.get("humidity") is None:
            data["humidity"] = 45.0
            defaulted_weather = True
        if defaulted_weather:
            data["weatherSource"] = "default_placeholder"
        elif not data.get("weatherSource"):
            data["weatherSource"] = "default_placeholder"
        return data

    print(f"⚠️  Using mock data for {station_name}")
    return {
        "sidoName": None,
        "requestedStation": station_name,
        "resolvedStation": station_name,
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
        "source": "mock",
        "weatherSource": "mock",
        "stationResolutionStatus": "unresolved",
    }

CACHE_COLLECTION = "rag_cache"
CACHE_TTL_SECONDS = 60 * 60 * 30  # 30 hours
_cache_ttl_index_ready = False
_ops_metrics_ttl_index_ready = False
ADVICE_DETAIL_MAX_CHARS = _bounded_int_env(
    "ADVICE_DETAIL_MAX_CHARS",
    520,
    min_value=120,
    max_value=1200,
)
ADVICE_CONTEXT_DOC_LIMIT = _bounded_int_env(
    "ADVICE_CONTEXT_DOC_LIMIT",
    2,
    min_value=1,
    max_value=4,
)
ADVICE_CONTEXT_DOC_MAX_CHARS = _bounded_int_env(
    "ADVICE_CONTEXT_DOC_MAX_CHARS",
    220,
    min_value=80,
    max_value=500,
)
ADVICE_AIR_FETCH_TIMEOUT_MS = _bounded_int_env(
    "ADVICE_AIR_FETCH_TIMEOUT_MS",
    2500,
    min_value=300,
    max_value=5000,
)
ADVICE_CACHE_READ_TIMEOUT_MS = _bounded_int_env(
    "ADVICE_CACHE_READ_TIMEOUT_MS",
    900,
    min_value=150,
    max_value=1800,
)
ADVICE_VECTOR_EMBED_TIMEOUT_MS = _bounded_int_env(
    "ADVICE_VECTOR_EMBED_TIMEOUT_MS",
    900,
    min_value=250,
    max_value=2500,
)
ADVICE_VECTOR_QUERY_TIMEOUT_MS = _bounded_int_env(
    "ADVICE_VECTOR_QUERY_TIMEOUT_MS",
    700,
    min_value=200,
    max_value=2200,
)
ADVICE_LLM_TIMEOUT_MS = _bounded_int_env(
    "ADVICE_LLM_TIMEOUT_MS",
    4500,
    min_value=600,
    max_value=12000,
)
ADVICE_CACHE_WRITE_TIMEOUT_MS = _bounded_int_env(
    "ADVICE_CACHE_WRITE_TIMEOUT_MS",
    900,
    min_value=150,
    max_value=1800,
)
ADVICE_CACHE_STALE_MINUTES = _bounded_int_env(
    "ADVICE_CACHE_STALE_MINUTES",
    360,
    min_value=30,
    max_value=72 * 60,
)
OPS_METRICS_RETENTION_DAYS = _bounded_int_env(
    "OPS_METRICS_RETENTION_DAYS",
    30,
    min_value=7,
    max_value=180,
)
OPS_METRICS_WRITE_TIMEOUT_MS = _bounded_int_env(
    "OPS_METRICS_WRITE_TIMEOUT_MS",
    500,
    min_value=100,
    max_value=2000,
)


def _normalize_whitespace(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _truncate_text(value: Any, max_chars: int) -> str:
    text = _normalize_whitespace(value)
    if max_chars <= 0 or len(text) <= max_chars:
        return text

    candidate = text[:max_chars]
    min_cut = int(max_chars * 0.6)
    for sep in ["다. ", "요. ", ". ", "! ", "? "]:
        cut = candidate.rfind(sep)
        if cut >= min_cut:
            return candidate[:cut + 2].rstrip()

    return candidate.rstrip(" ,;:") + "…"


def _build_compact_context_text(relevant_docs: List[Dict[str, Any]]) -> str:
    if not relevant_docs:
        return "관련 의학적 가이드라인 없음"

    lines: List[str] = []
    for doc in relevant_docs[:ADVICE_CONTEXT_DOC_LIMIT]:
        source = _normalize_whitespace(doc.get("source", "가이드라인")) or "가이드라인"
        text = _truncate_text(doc.get("text", ""), ADVICE_CONTEXT_DOC_MAX_CHARS)
        if text:
            lines.append(f"- [{source}] {text}")

    return "\n".join(lines) if lines else "관련 의학적 가이드라인 없음"


def _log_advice_timing(
    station_name: str,
    cache_hit: bool,
    timings: Dict[str, float],
    stage: str = "ok",
) -> None:
    order = [
        "air_fetch_ms",
        "cache_check_ms",
        "vector_search_ms",
        "llm_ms",
        "cache_write_ms",
        "total_ms",
    ]
    chunks = [
        f"{key}={timings[key]:.1f}ms"
        for key in order
        if key in timings and isinstance(timings[key], (int, float))
    ]
    print(
        f"⏱️ Advice timing station={station_name} stage={stage} "
        f"cache_hit={cache_hit} {' '.join(chunks)}"
    )


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(KST_TZ).isoformat()
    parsed = _parse_datetime_to_kst(value)
    return parsed.isoformat() if parsed else str(value)


def _safe_ratio(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((count / total) * 100, 1)


def _parse_airkorea_forecast_issued_at_to_kst(value: Any) -> Optional[datetime]:
    parsed = _parse_datetime_to_kst(value)
    if parsed is not None:
        return parsed

    text = _normalize_whitespace(value)
    matched = re.match(r"^(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2})시", text)
    if not matched:
        return None

    try:
        year, month, day, hour = [int(part) for part in matched.groups()]
        return datetime(year, month, day, hour, 0, tzinfo=KST_TZ)
    except ValueError:
        return None


def _normalize_forecast_date(value: Any) -> Optional[str]:
    text = _normalize_whitespace(value)
    if not text:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return text
    if re.match(r"^\d{8}$", text):
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"

    parsed = _parse_datetime_to_kst(text)
    return parsed.date().isoformat() if parsed else None


def _build_forecast_ingest_stale_ratio(status: Dict[str, Any]) -> Dict[str, Any]:
    latest_success_at = _normalize_whitespace(status.get("latestSuccessAt"))
    threshold = status.get("staleThresholdMinutes", FORECAST_INGEST_STALE_THRESHOLD_MINUTES)
    sub_parts = [f"threshold={threshold}m"]
    if latest_success_at:
        sub_parts.insert(0, f"latestSuccess={latest_success_at}")

    return {
        "count": 1 if status.get("isStale") else 0,
        "ratio": 100.0 if status.get("isStale") else 0.0,
        "total": 1,
        "subLabel": " · ".join(sub_parts),
    }


async def _get_forecast_ingest_status(generated_at: datetime) -> Dict[str, Any]:
    if forecast_monitor_db is None:
        return {
            "available": False,
            "isStale": True,
            "staleThresholdMinutes": FORECAST_INGEST_STALE_THRESHOLD_MINUTES,
            "latestSuccessAt": None,
            "latestSuccessAgeMinutes": None,
            "latestRunStatus": None,
            "latestIssuedAt": None,
            "latestForecastDate": None,
            "latestCodes": [],
        }

    try:
        runs_collection = forecast_monitor_db[AIR_QUALITY_FORECAST_RUNS_COLLECTION]
        forecast_collection = forecast_monitor_db[AIR_QUALITY_FORECAST_COLLECTION]

        latest_run, latest_success_run, latest_docs, latest_forecast_doc = await asyncio.gather(
            runs_collection.find(
                {"jobName": "airkorea-forecast"},
                {"_id": 0, "status": 1, "startedAt": 1, "finishedAt": 1, "updatedAt": 1, "summary": 1},
            ).sort([("startedAt", -1), ("updatedAt", -1)]).limit(1).to_list(length=1),
            runs_collection.find(
                {"jobName": "airkorea-forecast", "status": {"$in": ["success", "partial_failed"]}},
                {"_id": 0, "status": 1, "startedAt": 1, "finishedAt": 1, "updatedAt": 1, "summary": 1},
            ).sort([("finishedAt", -1), ("updatedAt", -1)]).limit(1).to_list(length=1),
            forecast_collection.find(
                {},
                {"_id": 0, "issuedAt": 1, "issuedAtUtc": 1, "forecastDate": 1, "informCode": 1},
            ).sort([("issuedAtUtc", -1), ("updatedAt", -1), ("forecastDate", -1)]).limit(10).to_list(length=10),
            forecast_collection.find(
                {},
                {"_id": 0, "forecastDate": 1, "issuedAtUtc": 1, "updatedAt": 1},
            ).sort([("forecastDate", -1), ("issuedAtUtc", -1), ("updatedAt", -1)]).limit(1).to_list(length=1),
        )
    except Exception as e:
        print(f"⚠️ Forecast ingest monitoring query failed: {e}")
        return {
            "available": False,
            "isStale": True,
            "staleThresholdMinutes": FORECAST_INGEST_STALE_THRESHOLD_MINUTES,
            "latestSuccessAt": None,
            "latestSuccessAgeMinutes": None,
            "latestRunStatus": None,
            "latestIssuedAt": None,
            "latestForecastDate": None,
            "latestCodes": [],
            "error": str(e),
        }

    latest_run_doc = latest_run[0] if latest_run else {}
    latest_success_doc = latest_success_run[0] if latest_success_run else {}
    latest_forecast_date_doc = latest_forecast_doc[0] if latest_forecast_doc else {}

    latest_success_at = _parse_datetime_to_kst(
        latest_success_doc.get("finishedAt")
        or latest_success_doc.get("updatedAt")
        or latest_success_doc.get("startedAt")
    )
    latest_issued_at = _parse_airkorea_forecast_issued_at_to_kst(
        next(
            (
                doc.get("issuedAtUtc") or doc.get("issuedAt")
                for doc in latest_docs
                if doc.get("issuedAtUtc") or doc.get("issuedAt")
            ),
            None,
        )
    )
    latest_forecast_date = _normalize_forecast_date(latest_forecast_date_doc.get("forecastDate"))
    latest_success_age_minutes = (
        max(0, int((generated_at - latest_success_at).total_seconds() // 60))
        if latest_success_at is not None
        else None
    )
    today_kst = generated_at.date().isoformat()
    is_stale = (
        latest_success_age_minutes is None
        or latest_success_age_minutes > FORECAST_INGEST_STALE_THRESHOLD_MINUTES
        or latest_forecast_date is None
        or latest_forecast_date < today_kst
    )

    return {
        "available": True,
        "isStale": is_stale,
        "staleThresholdMinutes": FORECAST_INGEST_STALE_THRESHOLD_MINUTES,
        "latestSuccessAt": _to_iso(latest_success_at),
        "latestSuccessAgeMinutes": latest_success_age_minutes,
        "latestRunStatus": _normalize_whitespace(
            latest_run_doc.get("status") or latest_success_doc.get("status")
        ),
        "latestIssuedAt": _to_iso(latest_issued_at),
        "latestForecastDate": latest_forecast_date,
        "latestCodes": _dedupe_preserve(
            [_normalize_whitespace(doc.get("informCode")) or "" for doc in latest_docs]
        ),
    }


def _build_advice_ops_event(
    *,
    station_name: str,
    air_data: Dict[str, Any],
    air_fetch_mode: str,
    stage: str,
    timings: Dict[str, float],
    cache_hit: bool,
    stale_cache_hit: bool,
    cache_age_seconds: Optional[int],
    overlay_used: bool,
    llm_timeout: bool,
    response_fallback_used: bool,
    context_doc_count: int = 0,
    context_chars: int = 0,
) -> Dict[str, Any]:
    air_source = _normalize_whitespace(air_data.get("source")) or "unknown"
    weather_source = _normalize_whitespace(air_data.get("weatherSource")) or "unknown"
    station_resolution_status = (
        _normalize_whitespace(air_data.get("stationResolutionStatus")) or "unknown"
    )
    station_resolution_failed = station_resolution_status != "exact"

    data_fallback_used = air_source in {"airkorea_api", "mock"} or weather_source == "default_placeholder"
    fallback_used = bool(response_fallback_used or data_fallback_used)

    rounded_timings = {
        key: round(float(value), 1)
        for key, value in timings.items()
        if isinstance(value, (int, float))
    }

    return {
        "createdAt": datetime.now(KST_TZ),
        "stationName": air_data.get("stationName"),
        "requestedStation": station_name,
        "resolvedStation": air_data.get("resolvedStation") or air_data.get("stationName"),
        "sidoName": air_data.get("sidoName"),
        "dataTime": air_data.get("dataTime"),
        "airFetchMode": air_fetch_mode,
        "airSource": air_source,
        "weatherSource": weather_source,
        "fallbackUsed": fallback_used,
        "responseFallbackUsed": bool(response_fallback_used),
        "dataFallbackUsed": bool(data_fallback_used),
        "overlayUsed": bool(overlay_used),
        "llmTimeout": bool(llm_timeout),
        "cacheHit": bool(cache_hit),
        "staleCacheHit": bool(stale_cache_hit),
        "cacheAgeSeconds": cache_age_seconds,
        "stationResolutionStatus": station_resolution_status,
        "stationResolutionFailed": station_resolution_failed,
        "contextDocCount": context_doc_count,
        "contextChars": context_chars,
        "timings": rounded_timings,
        "stage": stage,
    }


async def _ensure_ops_metrics_ttl_index():
    global _ops_metrics_ttl_index_ready
    if _ops_metrics_ttl_index_ready or db is None:
        return

    try:
        await db[OPS_METRICS_COLLECTION].create_index(
            "createdAt",
            expireAfterSeconds=OPS_METRICS_RETENTION_DAYS * 24 * 60 * 60,
            name="ops_advice_events_ttl",
        )
        _ops_metrics_ttl_index_ready = True
    except Exception as e:
        print(f"⚠️ Ops metrics TTL index creation failed: {e}")


async def _record_advice_ops_event(event: Dict[str, Any]) -> None:
    if db is None:
        return

    try:
        await asyncio.wait_for(
            _ensure_ops_metrics_ttl_index(),
            timeout=max(OPS_METRICS_WRITE_TIMEOUT_MS, 1) / 1000,
        )
        await asyncio.wait_for(
            db[OPS_METRICS_COLLECTION].insert_one(event),
            timeout=max(OPS_METRICS_WRITE_TIMEOUT_MS, 1) / 1000,
        )
    except Exception as e:
        if _is_timeout_error(e):
            print(f"⚠️ Ops metrics write timed out: timeout_ms={OPS_METRICS_WRITE_TIMEOUT_MS}")
        else:
            print(f"⚠️ Ops metrics write failed: {e}")


async def get_ops_metrics_summary(hours: int = 24, recent_limit: int = 50) -> Dict[str, Any]:
    window_hours = max(1, min(int(hours), 24 * 30))
    recent_count = max(1, min(int(recent_limit), 200))
    generated_at = datetime.now(KST_TZ)
    forecast_ingest_status = await _get_forecast_ingest_status(generated_at)
    forecast_ingest_stale_ratio = _build_forecast_ingest_stale_ratio(forecast_ingest_status)

    if db is None:
        return {
            "generatedAt": generated_at.isoformat(),
            "windowHours": window_hours,
            "totalRequests": 0,
            "error": "MongoDB unavailable",
            "forecastIngestStatus": forecast_ingest_status,
            "forecastIngestStaleRatio": forecast_ingest_stale_ratio,
            "recentEvents": [],
        }

    window_start = generated_at - timedelta(hours=window_hours)
    docs = await db[OPS_METRICS_COLLECTION].find(
        {"createdAt": {"$gte": window_start}},
        {"_id": 0},
    ).sort([("createdAt", -1)]).to_list(length=5000)

    total = len(docs)
    fallback_count = sum(1 for doc in docs if doc.get("fallbackUsed"))
    overlay_count = sum(1 for doc in docs if doc.get("overlayUsed"))
    llm_timeout_count = sum(1 for doc in docs if doc.get("llmTimeout"))
    stale_cache_count = sum(1 for doc in docs if doc.get("staleCacheHit"))
    cache_hit_count = sum(1 for doc in docs if doc.get("cacheHit"))
    station_resolution_failure_count = sum(1 for doc in docs if doc.get("stationResolutionFailed"))

    def breakdown(key: str) -> List[Dict[str, Any]]:
        counts: Dict[str, int] = {}
        for doc in docs:
            value = _normalize_whitespace(doc.get(key)) or "unknown"
            counts[value] = counts.get(value, 0) + 1
        return [
            {"name": name, "count": count, "ratio": _safe_ratio(count, total)}
            for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ]

    recent_events: List[Dict[str, Any]] = []
    for doc in docs[:recent_count]:
        recent_events.append({
            "createdAt": _to_iso(doc.get("createdAt")),
            "stationName": doc.get("stationName"),
            "requestedStation": doc.get("requestedStation"),
            "resolvedStation": doc.get("resolvedStation"),
            "stage": doc.get("stage"),
            "airFetchMode": doc.get("airFetchMode"),
            "airSource": doc.get("airSource"),
            "weatherSource": doc.get("weatherSource"),
            "fallbackUsed": bool(doc.get("fallbackUsed")),
            "overlayUsed": bool(doc.get("overlayUsed")),
            "llmTimeout": bool(doc.get("llmTimeout")),
            "cacheHit": bool(doc.get("cacheHit")),
            "staleCacheHit": bool(doc.get("staleCacheHit")),
            "responseFallbackUsed": bool(doc.get("responseFallbackUsed")),
            "dataFallbackUsed": bool(doc.get("dataFallbackUsed")),
            "stationResolutionStatus": doc.get("stationResolutionStatus"),
            "timings": doc.get("timings", {}),
        })

    return {
        "generatedAt": generated_at.isoformat(),
        "windowHours": window_hours,
        "totalRequests": total,
        "fallbackRatio": {"count": fallback_count, "ratio": _safe_ratio(fallback_count, total)},
        "overlayUsageRatio": {"count": overlay_count, "ratio": _safe_ratio(overlay_count, total)},
        "llmTimeoutRatio": {"count": llm_timeout_count, "ratio": _safe_ratio(llm_timeout_count, total)},
        "staleCacheUsageRatio": {"count": stale_cache_count, "ratio": _safe_ratio(stale_cache_count, total)},
        "cacheHitRatio": {"count": cache_hit_count, "ratio": _safe_ratio(cache_hit_count, total)},
        "stationResolutionFailureRatio": {
            "count": station_resolution_failure_count,
            "ratio": _safe_ratio(station_resolution_failure_count, total),
        },
        "forecastIngestStatus": forecast_ingest_status,
        "forecastIngestStaleRatio": forecast_ingest_stale_ratio,
        "airFetchModeBreakdown": breakdown("airFetchMode"),
        "airSourceBreakdown": breakdown("airSource"),
        "weatherSourceBreakdown": breakdown("weatherSource"),
        "stageBreakdown": breakdown("stage"),
        "stationResolutionBreakdown": breakdown("stationResolutionStatus"),
        "responseFallbackBreakdown": breakdown("responseFallbackUsed"),
        "dataFallbackBreakdown": breakdown("dataFallbackUsed"),
        "recentEvents": recent_events,
    }


def render_ops_dashboard_html(summary: Dict[str, Any]) -> str:
    def card(title: str, payload: Dict[str, Any]) -> str:
        count = payload.get("count", 0)
        ratio = payload.get("ratio", 0.0)
        total = payload.get("total", summary.get("totalRequests", 0))
        sub_label = payload.get("subLabel")
        return (
            "<div class='card'>"
            f"<div class='label'>{html.escape(title)}</div>"
            f"<div class='value'>{ratio}%</div>"
            f"<div class='sub'>{html.escape(str(sub_label or f'{count} / {total}'))}</div>"
            "</div>"
        )

    def status_card(title: str, payload: Dict[str, Any]) -> str:
        is_stale = bool(payload.get("isStale"))
        status = "STALE" if is_stale else "OK"
        latest_success_age = payload.get("latestSuccessAgeMinutes")
        stale_threshold = payload.get("staleThresholdMinutes")
        latest_success_text = (
            f"{latest_success_age} min ago"
            if isinstance(latest_success_age, int)
            else "no recent success"
        )
        latest_forecast_date = html.escape(str(payload.get("latestForecastDate") or "-"))
        latest_issued_at = html.escape(str(payload.get("latestIssuedAt") or "-"))
        status_class = "value danger" if is_stale else "value success"
        return (
            "<div class='card'>"
            f"<div class='label'>{html.escape(title)}</div>"
            f"<div class='{status_class}'>{status}</div>"
            f"<div class='sub'>{html.escape(latest_success_text)}</div>"
            f"<div class='sub'>threshold={html.escape(str(stale_threshold or '-'))}m</div>"
            f"<div class='sub'>forecastDate={latest_forecast_date}</div>"
            f"<div class='sub'>issuedAt={latest_issued_at}</div>"
            "</div>"
        )

    def table_rows(items: List[Dict[str, Any]]) -> str:
        if not items:
            return "<tr><td colspan='3'>데이터 없음</td></tr>"
        return "".join(
            "<tr>"
            f"<td>{html.escape(str(item.get('name', 'unknown')))}</td>"
            f"<td>{item.get('count', 0)}</td>"
            f"<td>{item.get('ratio', 0.0)}%</td>"
            "</tr>"
            for item in items
        )

    recent_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(item.get('createdAt') or '-'))}</td>"
        f"<td>{html.escape(str(item.get('requestedStation') or '-'))}</td>"
        f"<td>{html.escape(str(item.get('resolvedStation') or '-'))}</td>"
        f"<td>{html.escape(str(item.get('stage') or '-'))}</td>"
        f"<td>{html.escape(str(item.get('airFetchMode') or '-'))}</td>"
        f"<td>{html.escape(str(item.get('airSource') or '-'))}</td>"
        f"<td>{html.escape(str(item.get('weatherSource') or '-'))}</td>"
        f"<td>{'Y' if item.get('fallbackUsed') else 'N'}</td>"
        f"<td>{'Y' if item.get('overlayUsed') else 'N'}</td>"
        f"<td>{'Y' if item.get('llmTimeout') else 'N'}</td>"
        f"<td>{'Y' if item.get('staleCacheHit') else 'N'}</td>"
        f"<td>{html.escape(str(item.get('stationResolutionStatus') or '-'))}</td>"
        "</tr>"
        for item in summary.get("recentEvents", [])
    ) or "<tr><td colspan='12'>최근 이벤트 없음</td></tr>"

    generated_at = html.escape(str(summary.get("generatedAt", "-")))
    window_hours = html.escape(str(summary.get("windowHours", 24)))
    total_requests = summary.get("totalRequests", 0)
    forecast_ingest_status = summary.get("forecastIngestStatus", {})
    forecast_codes = ", ".join(forecast_ingest_status.get("latestCodes", [])) or "-"
    forecast_rows = "".join([
        "<tr>"
        f"<td>{'Y' if forecast_ingest_status.get('available') else 'N'}</td>"
        f"<td>{'Y' if forecast_ingest_status.get('isStale') else 'N'}</td>"
        f"<td>{html.escape(str(forecast_ingest_status.get('latestRunStatus') or '-'))}</td>"
        f"<td>{html.escape(str(forecast_ingest_status.get('latestSuccessAt') or '-'))}</td>"
        f"<td>{html.escape(str(forecast_ingest_status.get('latestSuccessAgeMinutes') or '-'))}</td>"
        f"<td>{html.escape(str(forecast_ingest_status.get('latestIssuedAt') or '-'))}</td>"
        f"<td>{html.escape(str(forecast_ingest_status.get('latestForecastDate') or '-'))}</td>"
        f"<td>{html.escape(str(forecast_ingest_status.get('staleThresholdMinutes') or '-'))}</td>"
        f"<td>{html.escape(forecast_codes)}</td>"
        "</tr>"
    ])

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>EPI-LOG AI Ops Dashboard</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --panel: #fffdf8;
      --ink: #1f1a14;
      --muted: #6b6257;
      --accent: #b44f28;
      --line: #ddd2c1;
    }}
    body {{ margin:0; font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, sans-serif; background: linear-gradient(180deg, #f8f4ec 0%, #efe5d4 100%); color: var(--ink); }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 32px 20px 60px; }}
    .hero {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-end; margin-bottom:24px; }}
    h1 {{ margin:0; font-size: 34px; line-height:1; }}
    .meta {{ color: var(--muted); font-size:14px; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:12px; margin-bottom:20px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 18px; box-shadow: 0 10px 30px rgba(72, 48, 24, 0.06); }}
    .label {{ color: var(--muted); font-size:13px; margin-bottom:8px; }}
    .value {{ font-size: 30px; font-weight: 700; }}
    .value.success {{ color: #1d7a45; }}
    .value.danger {{ color: #b44f28; }}
    .sub {{ margin-top:6px; color: var(--muted); font-size:13px; }}
    .section {{ background: var(--panel); border:1px solid var(--line); border-radius:20px; padding:18px; margin-top:14px; box-shadow: 0 10px 30px rgba(72, 48, 24, 0.05); }}
    .section h2 {{ margin:0 0 14px; font-size:18px; }}
    .split {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:14px; }}
    table {{ width:100%; border-collapse: collapse; font-size:14px; }}
    th, td {{ text-align:left; padding:10px 8px; border-bottom:1px solid var(--line); vertical-align:top; }}
    th {{ color: var(--muted); font-weight:600; }}
    .badge {{ display:inline-block; padding:4px 8px; border-radius:999px; background:#f6e3d2; color: var(--accent); font-size:12px; font-weight:700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <div class="badge">AI Server Ops</div>
        <h1>EPI-LOG Advice Dashboard</h1>
      </div>
      <div class="meta">window={window_hours}h · total={total_requests} · generatedAt={generated_at}</div>
    </div>
    <div class="grid">
      {card("Fallback Ratio", summary.get("fallbackRatio", dict()))}
      {card("Overlay Usage", summary.get("overlayUsageRatio", dict()))}
      {card("LLM Timeout Ratio", summary.get("llmTimeoutRatio", dict()))}
      {card("Stale Cache Usage", summary.get("staleCacheUsageRatio", dict()))}
      {card("Cache Hit Ratio", summary.get("cacheHitRatio", dict()))}
      {card("Station Resolution Failure", summary.get("stationResolutionFailureRatio", dict()))}
      {status_card("Forecast Ingest", forecast_ingest_status)}
    </div>
    <div class="split">
      <div class="section">
        <h2>Air Fetch Mode</h2>
        <table><thead><tr><th>Name</th><th>Count</th><th>Ratio</th></tr></thead><tbody>{table_rows(summary.get("airFetchModeBreakdown", []))}</tbody></table>
      </div>
      <div class="section">
        <h2>Air Source</h2>
        <table><thead><tr><th>Name</th><th>Count</th><th>Ratio</th></tr></thead><tbody>{table_rows(summary.get("airSourceBreakdown", []))}</tbody></table>
      </div>
      <div class="section">
        <h2>Weather Source</h2>
        <table><thead><tr><th>Name</th><th>Count</th><th>Ratio</th></tr></thead><tbody>{table_rows(summary.get("weatherSourceBreakdown", []))}</tbody></table>
      </div>
      <div class="section">
        <h2>Stage Breakdown</h2>
        <table><thead><tr><th>Name</th><th>Count</th><th>Ratio</th></tr></thead><tbody>{table_rows(summary.get("stageBreakdown", []))}</tbody></table>
      </div>
      <div class="section">
        <h2>Station Resolution</h2>
        <table><thead><tr><th>Name</th><th>Count</th><th>Ratio</th></tr></thead><tbody>{table_rows(summary.get("stationResolutionBreakdown", []))}</tbody></table>
      </div>
      <div class="section">
        <h2>Forecast Ingest Health</h2>
        <table>
          <thead><tr><th>Available</th><th>Stale</th><th>Run Status</th><th>Latest Success</th><th>Age (min)</th><th>Latest Issued</th><th>Forecast Date</th><th>Threshold</th><th>Codes</th></tr></thead>
          <tbody>{forecast_rows}</tbody>
        </table>
      </div>
    </div>
    <div class="section">
      <h2>Recent Events</h2>
      <table>
        <thead>
          <tr>
            <th>Created</th><th>Requested</th><th>Resolved</th><th>Stage</th><th>Fetch Mode</th><th>Air Source</th><th>Weather</th><th>Fallback</th><th>Overlay</th><th>LLM Timeout</th><th>Stale Cache</th><th>Resolution</th>
          </tr>
        </thead>
        <tbody>{recent_rows}</tbody>
      </table>
    </div>
  </div>
</body>
</html>"""


def _is_timeout_error(error: Exception) -> bool:
    return isinstance(error, asyncio.TimeoutError)


def _is_voyage_forbidden_error(error: Exception) -> bool:
    text = str(error).lower()
    return "forbidden" in text or "403" in text


async def _run_blocking_with_timeout(timeout_ms: int, func, *args, **kwargs):
    timeout_s = max(timeout_ms, 1) / 1000
    return await asyncio.wait_for(
        asyncio.to_thread(func, *args, **kwargs),
        timeout=timeout_s,
    )


def _enforce_advice_response_limits(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload or {})

    raw_detail_answer = normalized.get("detail_answer", normalized.get("reason"))
    detail_answer = _truncate_text(raw_detail_answer, ADVICE_DETAIL_MAX_CHARS)
    if not detail_answer:
        detail_answer = "정보를 불러오는 중 문제가 발생했습니다."
    normalized["detail_answer"] = detail_answer
    normalized["reason"] = detail_answer

    raw_three_reason = normalized.get("three_reason")
    three_reason: List[str] = []
    if isinstance(raw_three_reason, list):
        for item in raw_three_reason:
            sentence = _truncate_text(item, 90)
            if sentence:
                three_reason.append(sentence)

    fallback_three_reason = [
        "대기질 정보를 분석하고 있습니다.",
        "잠시 후 다시 확인해주세요.",
        "문제가 지속되면 관리자에게 문의하세요."
    ]
    for fallback in fallback_three_reason:
        if len(three_reason) >= 3:
            break
        three_reason.append(fallback)

    normalized["three_reason"] = three_reason[:3]
    return normalized


def _build_metric_summary(air_data: Dict[str, Any]) -> str:
    metric_parts: List[str] = []
    if air_data.get("pm25_value") is not None:
        metric_parts.append(f"초미세먼지 {air_data['pm25_value']}ug/m3")
    if air_data.get("pm10_value") is not None:
        metric_parts.append(f"미세먼지 {air_data['pm10_value']}ug/m3")
    if air_data.get("o3_value") is not None:
        metric_parts.append(f"오존 {air_data['o3_value']}ppm")
    if air_data.get("no2_value") is not None:
        metric_parts.append(f"이산화질소 {air_data['no2_value']}ppm")
    return ", ".join(metric_parts)


def _normalize_korean_grade_label(value: Any) -> Optional[str]:
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    normalized = raw.upper()
    grade_aliases = {
        "1": "좋음",
        "GOOD": "좋음",
        "좋음": "좋음",
        "2": "보통",
        "NORMAL": "보통",
        "보통": "보통",
        "3": "나쁨",
        "BAD": "나쁨",
        "나쁨": "나쁨",
        "4": "매우나쁨",
        "VERY_BAD": "매우나쁨",
        "VERYBAD": "매우나쁨",
        "매우나쁨": "매우나쁨",
    }
    return grade_aliases.get(normalized) or grade_aliases.get(raw)


def _normalize_authoritative_air_quality(
    current_air_quality: Optional[Dict[str, Any]],
    requested_station_name: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(current_air_quality, dict) or not current_air_quality:
        return None

    def pick(*keys: str) -> Any:
        for key in keys:
            if key in current_air_quality and current_air_quality.get(key) is not None:
                return current_air_quality.get(key)
        return None

    station_value = pick("resolvedStation", "stationName", "requestedStation") or requested_station_name
    station_name = _normalize_whitespace(station_value) or requested_station_name
    sido_name = pick("sidoName")

    pm25_value = _coerce_number(pick("pm25_value", "pm25Value"))
    pm10_value = _coerce_number(pick("pm10_value", "pm10Value"))
    o3_value = _coerce_number(pick("o3_value", "o3Value"))
    no2_value = _coerce_number(pick("no2_value", "no2Value"))
    co_value = _coerce_number(pick("co_value", "coValue"))
    so2_value = _coerce_number(pick("so2_value", "so2Value"))
    temp_value = _coerce_number(pick("temp", "temperature"))
    humidity_value = _coerce_number(pick("humidity"))

    overall_grade = _normalize_korean_grade_label(pick("grade"))
    pm25_grade = (
        _normalize_korean_grade_label(pick("pm25_grade", "pm25Grade"))
        or _grade_from_value("pm25", pm25_value)
        or overall_grade
    )
    pm10_grade = (
        _normalize_korean_grade_label(pick("pm10_grade", "pm10Grade"))
        or _grade_from_value("pm10", pm10_value)
        or overall_grade
    )
    o3_grade = (
        _normalize_korean_grade_label(pick("o3_grade", "o3Grade"))
        or _grade_from_value("o3", o3_value)
        or overall_grade
    )
    no2_grade = (
        _normalize_korean_grade_label(pick("no2_grade", "no2Grade"))
        or _grade_from_value("no2", no2_value)
    )
    co_grade = (
        _normalize_korean_grade_label(pick("co_grade", "coGrade"))
        or _grade_from_value("co", co_value)
    )
    so2_grade = (
        _normalize_korean_grade_label(pick("so2_grade", "so2Grade"))
        or _grade_from_value("so2", so2_value)
    )

    data_time = pick("dataTime", "updatedAt", "observedAt")
    resolved_station = _normalize_whitespace(pick("resolvedStation")) or station_name
    requested_station = _normalize_whitespace(pick("requestedStation")) or requested_station_name
    station_resolution_status = _normalize_whitespace(pick("stationResolutionStatus"))
    if not station_resolution_status:
        station_resolution_status = _derive_station_resolution_status(
            requested_station,
            pick("matchedCandidate", "resolvedStation", "stationName"),
            resolved_station,
        )

    return {
        "stationName": station_name,
        "resolvedStation": resolved_station,
        "requestedStation": requested_station,
        "sidoName": sido_name,
        "pm25_value": pm25_value,
        "pm10_value": pm10_value,
        "o3_value": o3_value,
        "no2_value": no2_value,
        "co_value": co_value,
        "so2_value": so2_value,
        "pm25_grade": pm25_grade,
        "pm10_grade": pm10_grade,
        "o3_grade": o3_grade,
        "no2_grade": no2_grade,
        "co_grade": co_grade,
        "so2_grade": so2_grade,
        "temp": temp_value,
        "humidity": humidity_value,
        "dataTime": data_time,
        "updatedAt": pick("updatedAt"),
        "source": pick("source") or "authoritative_request",
        "weatherSource": pick("weatherSource") or ("authoritative_request" if temp_value is not None and humidity_value is not None else "missing"),
        "weatherMatchedStation": pick("weatherMatchedStation", "stationName", "resolvedStation"),
        "weatherForecastAt": pick("weatherForecastAt", "dataTime"),
        "stationResolutionStatus": station_resolution_status,
    }


def _authoritative_air_quality_missing_core_fields(air_data: Optional[Dict[str, Any]]) -> bool:
    if not air_data:
        return True

    required_keys = (
        "stationName",
        "dataTime",
        "pm25_value",
        "pm10_value",
        "o3_value",
        "no2_value",
        "temp",
        "humidity",
    )
    return any(air_data.get(key) is None for key in required_keys)


def _resolve_grade_with_fallback(pollutant: str, value: Any, existing_grade: Any, default_grade: str) -> str:
    normalized_existing = _normalize_korean_grade_label(existing_grade)
    computed = _grade_from_value(pollutant, value)
    if computed:
        return computed
    if normalized_existing:
        return normalized_existing
    return default_grade


def _overlay_air_quality(
    baseline_air_data: Optional[Dict[str, Any]],
    authoritative_air_data: Optional[Dict[str, Any]],
    requested_station_name: str,
) -> Optional[Dict[str, Any]]:
    if not baseline_air_data and not authoritative_air_data:
        return None

    merged: Dict[str, Any] = {}
    if isinstance(baseline_air_data, dict):
        merged.update(baseline_air_data)

    if isinstance(authoritative_air_data, dict):
        for key, value in authoritative_air_data.items():
            if value is not None:
                merged[key] = value

    station_name = (
        _normalize_whitespace(
            merged.get("stationName")
            or merged.get("resolvedStation")
            or merged.get("requestedStation")
            or requested_station_name
        )
        or requested_station_name
    )
    authoritative_resolved_station = (
        authoritative_air_data.get("resolvedStation")
        if isinstance(authoritative_air_data, dict)
        else None
    )
    baseline_resolved_station = (
        baseline_air_data.get("resolvedStation")
        if isinstance(baseline_air_data, dict)
        else None
    )
    resolved_station = (
        _normalize_whitespace(
            merged.get("resolvedStation")
            or authoritative_resolved_station
            or baseline_resolved_station
            or station_name
        )
        or station_name
    )
    requested_station = (
        _normalize_whitespace(
            merged.get("requestedStation")
            or requested_station_name
        )
        or requested_station_name
    )

    merged["stationName"] = station_name
    merged["resolvedStation"] = resolved_station
    merged["requestedStation"] = requested_station
    merged["source"] = (
        authoritative_air_data.get("source")
        if isinstance(authoritative_air_data, dict) and authoritative_air_data.get("source")
        else merged.get("source")
        or "server_lookup"
    )
    merged["weatherSource"] = (
        authoritative_air_data.get("weatherSource")
        if isinstance(authoritative_air_data, dict) and authoritative_air_data.get("weatherSource")
        else merged.get("weatherSource")
        or ("missing" if merged.get("temp") is None or merged.get("humidity") is None else "air_document")
    )
    merged["weatherMatchedStation"] = (
        authoritative_air_data.get("weatherMatchedStation")
        if isinstance(authoritative_air_data, dict) and authoritative_air_data.get("weatherMatchedStation")
        else merged.get("weatherMatchedStation")
        or merged.get("stationName")
    )
    merged["weatherForecastAt"] = (
        authoritative_air_data.get("weatherForecastAt")
        if isinstance(authoritative_air_data, dict) and authoritative_air_data.get("weatherForecastAt")
        else merged.get("weatherForecastAt")
        or merged.get("dataTime")
    )
    merged["stationResolutionStatus"] = (
        authoritative_air_data.get("stationResolutionStatus")
        if isinstance(authoritative_air_data, dict) and authoritative_air_data.get("stationResolutionStatus")
        else merged.get("stationResolutionStatus")
        or _derive_station_resolution_status(requested_station, merged.get("matchedCandidate"), resolved_station)
    )

    grade_defaults = {
        "pm25": "보통",
        "pm10": "보통",
        "o3": "보통",
        "no2": "좋음",
        "co": "좋음",
        "so2": "좋음",
    }
    for pollutant, default_grade in grade_defaults.items():
        value_key = f"{pollutant}_value"
        grade_key = f"{pollutant}_grade"
        merged[grade_key] = _resolve_grade_with_fallback(
            pollutant,
            merged.get(value_key),
            merged.get(grade_key),
            default_grade,
        )

    return merged


def _build_air_context_summary(air_data: Dict[str, Any]) -> str:
    station_name = _normalize_whitespace(air_data.get("stationName")) or "선택 지역"
    observed_at = air_data.get("dataTime") or air_data.get("updatedAt")
    metric_summary = _build_metric_summary(air_data)
    weather_parts: List[str] = []

    if air_data.get("temp") is not None:
        weather_parts.append(f"기온 {air_data['temp']}도")
    if air_data.get("humidity") is not None:
        weather_parts.append(f"습도 {air_data['humidity']}%")

    parts = [f"{station_name} 기준"]
    if observed_at:
        parts.append(f"측정시각 {observed_at}")
    if metric_summary:
        parts.append(metric_summary)
    if weather_parts:
        parts.append(", ".join(weather_parts))

    return ", ".join(parts)


def _build_deterministic_advice_payload(
    *,
    decision_text: str,
    csv_reason: str,
    action_items: List[str],
    air_data: Dict[str, Any],
    references: Optional[List[str]] = None,
) -> Dict[str, Any]:
    reason_items: List[str] = []

    csv_reason_text = _normalize_whitespace(csv_reason)
    if csv_reason_text:
        reason_items.append(csv_reason_text)
    else:
        reason_items.append("현재 대기질 수치와 사용자 조건을 함께 반영해 안내했어요.")

    air_context_summary = _build_air_context_summary(air_data)
    if air_context_summary:
        reason_items.append(f"{air_context_summary} 기준으로 판단했어요.")

    visible_actions = [item for item in action_items if _normalize_whitespace(item)]
    if visible_actions:
        reason_items.append(f"우선 {', '.join(visible_actions[:2])}부터 챙겨주세요.")

    detail_answer = " ".join(reason_items[:3])
    if not detail_answer:
        detail_answer = "현재 대기질 수치와 사용자 조건을 함께 반영해 안내했어요."

    return _enforce_advice_response_limits({
        "decision": decision_text,
        "csv_reason": csv_reason,
        "reason": detail_answer,
        "three_reason": reason_items[:3],
        "detail_answer": detail_answer,
        "actionItems": action_items,
        "references": references or [],
        "pm25_value": air_data.get("pm25_value"),
        "o3_value": air_data.get("o3_value"),
        "pm10_value": air_data.get("pm10_value"),
        "no2_value": air_data.get("no2_value"),
    })


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
    condition = _normalize_condition_key(user_profile.get("condition"))
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
    temp_value = _normalize_cache_token(air_data.get("temp"))
    humidity_value = _normalize_cache_token(air_data.get("humidity"))
    
    # Key format:
    # station:seoul_jongro_pm25:2_pm10:2_o3:1_age:toddler_cond:asthma_obs:202602071300_vals:14_52_0.03_0.009_weather:3_65
    return (
        f"station:{station_key}_"
        f"pm25:{pm25}_pm10:{pm10}_o3:{o3}_"
        f"age:{_normalize_cache_token(age_group)}_cond:{_normalize_cache_token(condition)}_"
        f"obs:{observed_key}_vals:{pm25_value}_{pm10_value}_{o3_value}_{no2_value}_"
        f"weather:{temp_value}_{humidity_value}"
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

async def get_medical_advice(
    station_name: str,
    user_profile: Dict[str, Any],
    current_air_quality: Optional[Dict[str, Any]] = None,
    air_quality_summary: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Main orchestration function with correction logic.
    """
    request_started_at = perf_counter()
    timings: Dict[str, float] = {}
    cache_hit = False
    stale_cache_hit = False
    cache_age_seconds: Optional[int] = None

    # Step A: Get Air Quality
    air_fetch_started_at = perf_counter()
    authoritative_air_data = _normalize_authoritative_air_quality(current_air_quality, station_name)
    baseline_air_data: Optional[Dict[str, Any]] = None
    needs_overlay_lookup = _authoritative_air_quality_missing_core_fields(authoritative_air_data)
    air_data_source = "server_lookup"

    if authoritative_air_data and not needs_overlay_lookup:
        air_data_source = "authoritative_request"
        print(
            "✅ Using authoritative air quality from request "
            f"station={station_name} resolved={authoritative_air_data.get('stationName')} "
            f"dataTime={authoritative_air_data.get('dataTime')}"
        )
    else:
        if authoritative_air_data:
            print(
                "ℹ️ Authoritative air quality is partial; fetching baseline for overlay "
                f"station={station_name}"
            )
        try:
            baseline_air_data = await asyncio.wait_for(
                get_air_quality(station_name),
                timeout=max(ADVICE_AIR_FETCH_TIMEOUT_MS, 1) / 1000,
            )
        except asyncio.TimeoutError:
            baseline_air_data = None
            print(
                f"⚠️ Air fetch timed out: station={station_name} timeout_ms={ADVICE_AIR_FETCH_TIMEOUT_MS}"
            )

        air_data = _overlay_air_quality(baseline_air_data, authoritative_air_data, station_name)
        if authoritative_air_data and air_data:
            air_data_source = "authoritative_overlay"
            print(
                "✅ Overlayed authoritative air quality onto baseline "
                f"station={station_name} resolved={air_data.get('stationName')} "
                f"dataTime={air_data.get('dataTime')}"
            )
        else:
            air_data_source = "server_lookup"
    if authoritative_air_data and not needs_overlay_lookup:
        air_data = authoritative_air_data
    timings["air_fetch_ms"] = round((perf_counter() - air_fetch_started_at) * 1000, 1)
    if not air_data:
        raise ValueError(f"No air quality data found for station: {station_name}")

    overlay_used = air_data_source == "authoritative_overlay"

    # Extract Weather Info for Correction
    temp = air_data.get("temp")
    humidity = air_data.get("humidity")
    user_condition = _normalize_condition_key(user_profile.get("condition"))
    age_group_raw = user_profile.get("ageGroup")
    age_group = _normalize_age_group(age_group_raw)
    age_group_label = AGE_GROUP_LABELS.get(age_group, AGE_GROUP_LABELS["elementary_high"])
    condition_label = CONDITION_LABELS.get(user_condition, CONDITION_LABELS["general"])
    authoritative_summary = _normalize_whitespace(air_quality_summary) or _build_air_context_summary(air_data)

    # Apply Correction Logic to get "Sensed" grades
    pm25_raw = air_data.get("pm25_grade", "보통")
    o3_raw = air_data.get("o3_grade", "보통")
    
    pm25_corrected = _get_corrected_grade(pm25_raw, temp, humidity, user_condition, "pm25")
    o3_corrected = _get_corrected_grade(o3_raw, temp, humidity, user_condition, "o3")

    cache_key = ""
    # [Step A.1] Check Cache
    cache_check_started_at = perf_counter()
    if db is not None:
        try:
            await asyncio.wait_for(
                _ensure_cache_ttl_index(),
                timeout=max(ADVICE_CACHE_READ_TIMEOUT_MS, 1) / 1000,
            )
            cache_key = _generate_cache_key(
                air_data,
                {
                    **user_profile,
                    "ageGroup": age_group,
                    "condition": user_condition,
                },
            )
            cached_entry = await asyncio.wait_for(
                db[CACHE_COLLECTION].find_one({"_id": cache_key}),
                timeout=max(ADVICE_CACHE_READ_TIMEOUT_MS, 1) / 1000,
            )
            
            if cached_entry:
                cache_hit = True
                timings["cache_check_ms"] = round((perf_counter() - cache_check_started_at) * 1000, 1)
                timings["total_ms"] = round((perf_counter() - request_started_at) * 1000, 1)
                cached_created_at = _parse_datetime_to_kst(
                    cached_entry.get("created_at") or cached_entry.get("createdAt")
                )
                if cached_created_at:
                    cache_age_seconds = max(
                        0,
                        int((datetime.now(KST_TZ) - cached_created_at).total_seconds()),
                    )
                    stale_cache_hit = cache_age_seconds > (ADVICE_CACHE_STALE_MINUTES * 60)
                cached_data = cached_entry.get("data") if isinstance(cached_entry, dict) else {}
                if not isinstance(cached_data, dict):
                    cached_data = {}
                normalized_cached_data = _enforce_advice_response_limits(cached_data)
                print(f"✅ Cache Hit! Key: {cache_key}")
                _log_advice_timing(station_name, cache_hit=True, timings=timings, stage="cache_hit")
                await _record_advice_ops_event(
                    _build_advice_ops_event(
                        station_name=station_name,
                        air_data=air_data,
                        air_fetch_mode=air_data_source,
                        stage="cache_hit",
                        timings=timings,
                        cache_hit=True,
                        stale_cache_hit=stale_cache_hit,
                        cache_age_seconds=cache_age_seconds,
                        overlay_used=overlay_used,
                        llm_timeout=False,
                        response_fallback_used=False,
                    )
                )
                return normalized_cached_data
        except Exception as e:
            if _is_timeout_error(e):
                print(
                    f"⚠️ Cache check timed out: station={station_name} timeout_ms={ADVICE_CACHE_READ_TIMEOUT_MS}"
                )
            else:
                print(f"⚠️ Cache check failed: {e}")
    timings["cache_check_ms"] = round((perf_counter() - cache_check_started_at) * 1000, 1)

    # Determine main issue for search (using corrected grades)
    main_condition = "보통"
    if pm25_corrected in ["나쁨", "매우나쁨"]:
        main_condition = f"초미세먼지 {pm25_corrected}"
    elif air_data.get("pm10_grade") in ["나쁨", "매우나쁨"]:
        main_condition = f"미세먼지 {air_data['pm10_grade']}"
    elif o3_corrected in ["나쁨", "매우나쁨"]:
        main_condition = f"오존 {o3_corrected}"
        
    # Step B: Query Construction
    search_query = f"{main_condition} 상황에서 {condition_label} {age_group_label} 행동 요령 주의사항"
    print(f"Generated Search Query (Primary): {search_query}")

    # Step C: Vector Search
    vector_search_started_at = perf_counter()
    relevant_docs = []
    global _vector_search_enabled
    global _vector_search_skip_notice_emitted
    if vo_client and db is not None and _vector_search_enabled:
        try:
            # 1. Primary Search
            embed_result = await _run_blocking_with_timeout(
                ADVICE_VECTOR_EMBED_TIMEOUT_MS,
                vo_client.embed,
                [search_query],
                model="voyage-3-large",
                input_type="query",
            )
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
            relevant_docs = await asyncio.wait_for(
                cursor.to_list(length=3),
                timeout=max(ADVICE_VECTOR_QUERY_TIMEOUT_MS, 1) / 1000,
            )
            
            # 2. Fallback Search (If no docs found)
            if not relevant_docs:
                print("⚠️ Primary search returned no results. Attempting fallback (General) search.")
                fallback_query = f"{main_condition} 행동 요령"
                embed_result_fb = await _run_blocking_with_timeout(
                    ADVICE_VECTOR_EMBED_TIMEOUT_MS,
                    vo_client.embed,
                    [fallback_query],
                    model="voyage-3-large",
                    input_type="query",
                )
                query_vector_fb = embed_result_fb.embeddings[0]
                
                pipeline[0]["$vectorSearch"]["queryVector"] = query_vector_fb
                
                cursor = db[GUIDELINES_COLLECTION].aggregate(pipeline)
                relevant_docs = await asyncio.wait_for(
                    cursor.to_list(length=3),
                    timeout=max(ADVICE_VECTOR_QUERY_TIMEOUT_MS, 1) / 1000,
                )

        except Exception as e:
            if _is_voyage_forbidden_error(e):
                _vector_search_enabled = False
                print(f"⚠️ Vector search disabled due to forbidden error: {e}")
            elif _is_timeout_error(e):
                print(
                    f"⚠️ Vector search timed out: station={station_name} "
                    f"embed_timeout_ms={ADVICE_VECTOR_EMBED_TIMEOUT_MS} query_timeout_ms={ADVICE_VECTOR_QUERY_TIMEOUT_MS}"
                )
            else:
                print(f"Error during vector search: {e}")
            pass
    elif vo_client and db is not None and not _vector_search_enabled and not _vector_search_skip_notice_emitted:
        _vector_search_skip_notice_emitted = True
        print("⚠️ Vector search skipped (disabled)")
    timings["vector_search_ms"] = round((perf_counter() - vector_search_started_at) * 1000, 1)

    # Step D: LLM Generation
    context_text = _build_compact_context_text(relevant_docs)
    context_doc_count = min(len(relevant_docs), ADVICE_CONTEXT_DOC_LIMIT)
    context_chars = len(context_text)

    # Calculate 4-level final grade and map directly into the 80-row matrix.
    final_grade = _calculate_final_grade(pm25_corrected, air_data.get("pm10_grade"), o3_corrected)
    final_grade_score = GRADE_MAP.get(final_grade, 2)

    # Weather sensitivity: for younger groups, cold/heat can effectively increase risk by one level.
    if age_group in {"infant", "toddler", "elementary_low"} and temp is not None:
        try:
            t = float(temp)
            if t < 5 or t > 30:
                final_grade_score = _escalate_grade_score(final_grade_score)
        except Exception:
            pass

    final_grade = REVERSE_GRADE_MAP.get(final_grade_score, final_grade)
    decision_text, action_items, csv_reason = _get_display_content(age_group, user_condition, final_grade)
    
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

    if not openai_client:
        timings["total_ms"] = round((perf_counter() - request_started_at) * 1000, 1)
        _log_advice_timing(station_name, cache_hit=cache_hit, timings=timings, stage="no_openai_client")
        print("⚠️ OpenAI client unavailable, returning deterministic advice fallback")
        fallback_result = _build_deterministic_advice_payload(
            decision_text=decision_text,
            csv_reason=csv_reason or "",
            action_items=action_items,
            air_data=air_data,
        )
        await _record_advice_ops_event(
            _build_advice_ops_event(
                station_name=station_name,
                air_data=air_data,
                air_fetch_mode=air_data_source,
                stage="no_openai_client",
                timings=timings,
                cache_hit=cache_hit,
                stale_cache_hit=stale_cache_hit,
                cache_age_seconds=cache_age_seconds,
                overlay_used=overlay_used,
                llm_timeout=False,
                response_fallback_used=True,
                context_doc_count=context_doc_count,
                context_chars=context_chars,
            )
        )
        return fallback_result

    system_prompt = (
        "너는 학부모용 대기질 안내 코치다. "
        "반드시 JSON 객체 하나만 반환한다. "
        "키는 three_reason(정확히 3개 배열), detail_answer(설명)만 사용한다. "
        "decision/actionItems와 모순되지 않게 작성하고, 과장 없이 실천 가능한 문장으로 답한다. "
        "입력에 제공된 현재 대기질 수치와 측정시각을 유일한 사실로 간주하고, 다른 수치를 추정하거나 바꿔 쓰지 마라."
    )

    user_prompt = (
        f"[입력]\n"
        f"- 현재 수치 소스: {air_data_source}\n"
        f"- 측정소: 요청={station_name}, 실제기준={air_data.get('stationName')}\n"
        f"- 측정시각: {air_data.get('dataTime') or air_data.get('updatedAt') or '미상'}\n"
        f"- 현재 대기질 요약: {authoritative_summary}\n"
        f"- 대기질 수치: PM2.5={air_data.get('pm25_value')}ug/m3, PM10={air_data.get('pm10_value')}ug/m3, "
        f"O3={air_data.get('o3_value')}ppm, NO2={air_data.get('no2_value')}ppm, "
        f"CO={air_data.get('co_value')}ppm, SO2={air_data.get('so2_value')}ppm\n"
        f"- 대기질 등급: PM2.5={pm25_raw}(보정:{pm25_corrected}), PM10={air_data.get('pm10_grade')}, "
        f"O3={o3_raw}(보정:{o3_corrected}), NO2={air_data.get('no2_grade')}\n"
        f"- 환경: 온도={temp}°C, 습도={humidity}%\n"
        f"- 사용자: 연령대={age_group_label}, 기저질환={condition_label}\n"
        f"- 최종등급: {final_grade}\n"
        f"- 시스템 결정: {decision_text}\n"
        f"- 시스템 행동수칙: {action_items}\n"
        f"- 결정 근거 문장: {csv_reason or '해당 없음'}\n\n"
        f"[의학 근거 요약]\n{context_text}\n\n"
        f"[출력 규칙]\n"
        f"- 반드시 위 현재 수치, 최종등급, 시스템 결정과 행동수칙을 기준으로 설명할 것\n"
        f"- 입력에 없는 다른 수치나 다른 등급을 새로 만들지 말 것\n"
        f"- three_reason: 핵심 3줄, 각 문장 짧고 명확하게\n"
        f"- detail_answer: 3~5문장, 최대 {ADVICE_DETAIL_MAX_CHARS}자\n"
        f"- 한국어 JSON만 반환"
    )

    llm_started_at = perf_counter()
    try:
        response = await _run_blocking_with_timeout(
            ADVICE_LLM_TIMEOUT_MS,
            openai_client.chat.completions.create,
            model=ADVICE_LLM_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            timeout=max(ADVICE_LLM_TIMEOUT_MS, 1) / 1000,
        )

        timings["llm_ms"] = round((perf_counter() - llm_started_at) * 1000, 1)

        content = response.choices[0].message.content or "{}"
        llm_result = json.loads(content)

        raw_detail_answer = llm_result.get("detail_answer", "정보를 불러오는 중 문제가 발생했습니다.")
        detail_answer = _truncate_text(raw_detail_answer, ADVICE_DETAIL_MAX_CHARS)
        if not detail_answer:
            detail_answer = "정보를 불러오는 중 문제가 발생했습니다."

        raw_three_reason = llm_result.get("three_reason")
        three_reason: List[str] = []
        if isinstance(raw_three_reason, list):
            for item in raw_three_reason:
                sentence = _truncate_text(item, 90)
                if sentence:
                    three_reason.append(sentence)

        fallback_three_reason = [
            "대기질 정보를 분석하고 있습니다.",
            "잠시 후 다시 확인해주세요.",
            "문제가 지속되면 관리자에게 문의하세요."
        ]
        for fallback in fallback_three_reason:
            if len(three_reason) >= 3:
                break
            three_reason.append(fallback)
        three_reason = three_reason[:3]
        
        # Merge Results
        final_result = _enforce_advice_response_limits({
            "decision": decision_text,
            "csv_reason": csv_reason,
            "reason": detail_answer,
            "three_reason": three_reason,
            "detail_answer": detail_answer,
            "actionItems": action_items,
            "references": list(set([doc.get("source", "Unknown Source") for doc in relevant_docs])),
            # Add real-time air quality values for frontend display
            "pm25_value": air_data.get("pm25_value"),
            "o3_value": air_data.get("o3_value"),
            "pm10_value": air_data.get("pm10_value"),
            "no2_value": air_data.get("no2_value")
        })

        print(
            "🧾 Advice prompt stats "
            f"station={station_name} context_docs={context_doc_count} "
            f"context_chars={context_chars} detail_max_chars={ADVICE_DETAIL_MAX_CHARS}"
        )
        
        # [Step F] Save to Cache
        if db is not None and cache_key:
            cache_write_started_at = perf_counter()
            try:
                await asyncio.wait_for(
                    db[CACHE_COLLECTION].update_one(
                        {"_id": cache_key},
                        {"$set": {"data": final_result, "created_at": datetime.now(KST_TZ)}},
                        upsert=True
                    ),
                    timeout=max(ADVICE_CACHE_WRITE_TIMEOUT_MS, 1) / 1000,
                )
                print(f"💾 Saved to cache: {cache_key}")
            except Exception as e:
                if _is_timeout_error(e):
                    print(
                        f"⚠️ Cache write timed out: key={cache_key} timeout_ms={ADVICE_CACHE_WRITE_TIMEOUT_MS}"
                    )
                else:
                    print(f"Error saving to cache: {e}")
            finally:
                timings["cache_write_ms"] = round((perf_counter() - cache_write_started_at) * 1000, 1)

        timings["total_ms"] = round((perf_counter() - request_started_at) * 1000, 1)
        _log_advice_timing(station_name, cache_hit=cache_hit, timings=timings, stage="ok")
        await _record_advice_ops_event(
            _build_advice_ops_event(
                station_name=station_name,
                air_data=air_data,
                air_fetch_mode=air_data_source,
                stage="ok",
                timings=timings,
                cache_hit=cache_hit,
                stale_cache_hit=stale_cache_hit,
                cache_age_seconds=cache_age_seconds,
                overlay_used=overlay_used,
                llm_timeout=False,
                response_fallback_used=False,
                context_doc_count=context_doc_count,
                context_chars=context_chars,
            )
        )
        return final_result
        
    except Exception as e:
        timings["llm_ms"] = round((perf_counter() - llm_started_at) * 1000, 1)
        timings["total_ms"] = round((perf_counter() - request_started_at) * 1000, 1)
        _log_advice_timing(station_name, cache_hit=cache_hit, timings=timings, stage="llm_error")
        llm_timeout = _is_timeout_error(e)
        if _is_timeout_error(e):
            print(f"⚠️ LLM call timed out: station={station_name} timeout_ms={ADVICE_LLM_TIMEOUT_MS}")
        else:
            print(f"Error calling OpenAI: {e}")
        fallback_result = _build_deterministic_advice_payload(
            decision_text=decision_text,
            csv_reason=csv_reason or "",
            action_items=action_items,
            air_data=air_data,
        )
        await _record_advice_ops_event(
            _build_advice_ops_event(
                station_name=station_name,
                air_data=air_data,
                air_fetch_mode=air_data_source,
                stage="llm_error",
                timings=timings,
                cache_hit=cache_hit,
                stale_cache_hit=stale_cache_hit,
                cache_age_seconds=cache_age_seconds,
                overlay_used=overlay_used,
                llm_timeout=llm_timeout,
                response_fallback_used=True,
                context_doc_count=context_doc_count,
                context_chars=context_chars,
            )
        )
        return fallback_result

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
