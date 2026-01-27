import os
import json
from datetime import datetime
from typing import List, Dict, Optional, Any
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
DB_NAME = "epilog_db"
GUIDELINES_COLLECTION = "medical_guidelines"
AIR_QUALITY_COLLECTION = "daily_air_quality"

if not MONGO_URI:
    # Fallback to a dummy URI if not set to prevent startup crash, but it will fail on request
    print("WARNING: MONGODB_URI is not set.")
    MONGO_URI = "mongodb://localhost:27017"

# Initialize Clients
try:
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DB_NAME]
except Exception as e:
    print(f"Error initializing MongoDB client: {e}")
    mongo_client = None
    db = None

# --- Logic Constants ---
GRADE_MAP = {
    "좋음": 0,
    "보통": 1,
    "나쁨": 2,
    "매우나쁨": 3
}

# Decision Texts
DECISION_TEXTS = {
    "infant": {
        "ok": "오늘은 바깥놀이 괜찮아요 🙂",
        "caution": "오늘은 짧게 다녀와요!",
        "warning": "오늘은 실내가 더 편해요 🏠"
    },
    "elementary_low": {
        "ok": "오늘은 밖에서 놀기 좋아요! 물은 꼭 챙기기!",
        "caution": "오늘은 잠깐만 다녀와요. 땀나는 놀이는 쉬기!",
        "warning": "오늘은 실내 놀이가 더 좋아요!"
    },
    "elementary_high": {
        "ok": "오늘은 야외활동 괜찮아요. 물 자주 마셔요!",
        "caution": "오늘은 야외 활동은 가능하지만 강도는 낮게!",
        "warning": "오늘은 실내 활동이 안전해요."
    },
    "teen": {
        "ok": "오늘은 야외 활동 무리 없어요. 수분 섭취 잊지 마세요.",
        "caution": "오늘은 야외 운동 강도는 낮추고 시간은 짧게!",
        "warning": "오늘은 실내 활동이 더 안전합니다."
    }
}

# Action Items templates
ACTION_ITEMS = {
    "infant": {
        "ok": [
            "가까운 공원에서 가볍게 뛰어놀기",
            "물 자주 마시기",
            "집에 오면 손·얼굴 씻기"
        ],
        "caution": [
            "외출은 20–30분 이내로 짧게",
            "뛰는 놀이는 잠깐만",
            "집에서는 블록/역할놀이로 바꿔보기"
        ],
        "warning": [
            "외출 대신 장난감 정리+찾기 게임",
            "실내에서 풍선배구/장애물 코스(가볍게)",
            "환기는 짧게(5–10분) 하고 바로 닫기"
        ]
    },
    "elementary_low": {
        "ok": [
            "가벼운 달리기/자전거",
            "물 자주 마시기",
            "귀가 후 손씻기/세안"
        ],
        "caution": [
            "땀 많이 나는 놀이는 잠깐만",
            "외출은 30분 이내",
            "실내에서는 만들기/보드게임 추천"
        ],
        "warning": [
            "밖 대신 실내 놀이(보드게임/만들기)",
            "창문 환기는 짧게",
            "기침/쌕쌕이면 쉬기"
        ]
    },
    "elementary_high": {
        "ok": [
            "가벼운 운동이나 산책",
            "마스크/손씻기(필요 시)",
            "귀가 후 샤워/세안"
        ],
        "caution": [
            "체육/뛰기 대신 산책·자전거 천천히",
            "시간은 짧게(30–60분)",
            "실내에서는 독서/보드게임/만들기"
        ],
        "warning": [
            "야외 활동 대신 실내 활동",
            "창문 환기는 짧게",
            "호흡기 증상 있으면 무리하지 않기"
        ]
    },
    "teen": {
        "ok": [
            "가벼운 운동이나 산책",
            "마스크/손씻기(필요 시)",
            "귀가 후 샤워/세안"
        ],
        "caution": [
            "격한 운동은 피하고 강도 낮추기",
            "외출 시간은 짧게(30–60분)",
            "실내에서는 스트레칭/가벼운 운동 추천"
        ],
        "warning": [
            "야외 활동 대신 실내 운동",
            "창문 환기는 짧게",
            "호흡기 증상 있으면 무리하지 않기"
        ]
    }
}

def _calculate_decision(pm25_grade: str, o3_grade: str) -> str:
    """
    Calculate decision level: 'ok', 'caution', 'warning'
    
    Logic:
    • OK: PM2.5 <= 보통 AND O3 <= 보통
    • Caution: One of them is 나쁨
    • Warning: One of them is 매우나쁨 OR Both are 나쁨
    """
    p_score = GRADE_MAP.get(pm25_grade, 0)
    o_score = GRADE_MAP.get(o3_grade, 0)
    
    # Check Warning Conditions
    # 1. Any '매우나쁨' (score 3)
    if p_score >= 3 or o_score >= 3:
        return "warning"
    # 2. Both '나쁨' (score 2)
    if p_score == 2 and o_score == 2:
        return "warning"
        
    # Check Caution Conditions
    # One is '나쁨' (score 2) - note: the case where both are bad is handled above
    if p_score == 2 or o_score == 2:
        return "caution"
        
    # Default OK
    return "ok"

def _normalize_age_group(age_group: Any) -> str:
    if age_group is None:
        return "elementary_high"
    raw = str(age_group).strip().lower()
    if raw in {
        "infant",
        "유아",
        "영유아",
        "0-6",
        "0~6",
        "0-5",
        "0~5",
        "0-3",
        "0~3"
    }:
        return "infant"
    if raw in {
        "elementary_low",
        "초등 저학년",
        "초등저학년",
        "1-3",
        "1~3",
        "7-9",
        "7~9"
    }:
        return "elementary_low"
    if raw in {
        "elementary_high",
        "초등 고학년",
        "초등고학년",
        "4-6",
        "4~6",
        "10-12",
        "10~12"
    }:
        return "elementary_high"
    if raw in {
        "teen",
        "청소년",
        "중등",
        "고등",
        "중학생",
        "고등학생",
        "13-15",
        "13~15",
        "16-18",
        "16~18"
    }:
        return "teen"
    if raw in {"child", "children", "초등", "아동"}:
        return "elementary_high"
    if raw in {"adult", "성인"}:
        return "teen"
    if "유아" in raw:
        return "infant"
    if "초등" in raw or "아동" in raw:
        return "elementary_high"
    if "저학년" in raw:
        return "elementary_low"
    if "고학년" in raw:
        return "elementary_high"
    if "중등" in raw or "고등" in raw or "청소년" in raw:
        return "teen"
    return "elementary_high"

def _get_display_content(age_group: str, decision_key: str):
    """
    Returns (decision_text, action_items)
    """
    # Normalize age group to key
    group_key = _normalize_age_group(age_group)
    
    # Get Text
    d_text = DECISION_TEXTS.get(group_key, DECISION_TEXTS["elementary_high"]).get(decision_key, "상태 확인 필요")
    
    # Get Actions
    actions = ACTION_ITEMS.get(group_key, ACTION_ITEMS["elementary_high"]).get(decision_key, [])
    
    return d_text, actions

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

async def get_air_quality(station_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch air quality data for the given station and today's date.
    """
    if db is None:
        raise Exception("Database connection not initialized")
        
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Try to find today's data for the station
    # Note: In a real scenario, you might need to query an external API if DB doesn't have it.
    # For this task, we assume it's in the DB or we simulate it if not found (for dev purposes).
    
    try:
        result = await db[AIR_QUALITY_COLLECTION].find_one({
            "stationName": station_name,
            "date": today_str
        })
        
        if result:
            return result
            
        # Mock data if not found (for demonstration purposes as requested structure implies data exists)
        # In production, this should return None or raise specific error
        print(f"No air quality data found for {station_name} on {today_str}. Using mock data.")
        return {
            "stationName": station_name,
            "date": today_str,
            "pm10_grade": "나쁨",
            "pm25_grade": "나쁨",
            "co_grade": "보통",
            "o3_grade": "보통",
            "no2_grade": "좋음",
            "so2_grade": "좋음",
            "integrated_grade": "나쁨"
        }
        
    except Exception as e:
        print(f"Error fetching air quality: {e}")
        raise e

CACHE_COLLECTION = "rag_cache"

def _generate_cache_key(air_data: Dict[str, Any], user_profile: Dict[str, Any]) -> str:
    grade_map = {"좋음": 1, "보통": 2, "나쁨": 3, "매우나쁨": 4}
    
    pm25 = grade_map.get(air_data.get("pm25_grade", ""), 0)
    pm10 = grade_map.get(air_data.get("pm10_grade", ""), 0)
    o3 = grade_map.get(air_data.get("o3_grade", ""), 0) # Added o3 as per user example
    
    age_group = _normalize_age_group(user_profile.get("ageGroup"))
    condition = user_profile.get("condition", "unknown")
    
    # Key format: pm25:3_pm10:2_o3:1_age:adult_cond:asthma
    return f"pm25:{pm25}_pm10:{pm10}_o3:{o3}_age:{age_group}_cond:{condition}"

async def get_medical_advice(station_name: str, user_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main orchestration function:
    1. Get Air Quality
    2. Check Cache
    3. Construct Query
    4. Vector Search
    5. Generate Advice with LLM
    6. Save to Cache & Return
    """
    # Step A: Get Air Quality
    air_data = await get_air_quality(station_name)
    if not air_data:
        raise ValueError(f"No air quality data found for station: {station_name}")

    cache_key = ""
    # [Step A.1] Check Cache
    if db is not None:
        try:
            cache_key = _generate_cache_key(air_data, user_profile)
            cached_entry = await db[CACHE_COLLECTION].find_one({"_id": cache_key})
            
            if cached_entry:
                print(f"✅ Cache Hit! Key: {cache_key}")
                return cached_entry["data"]
        except Exception as e:
            print(f"⚠️ Cache check failed: {e}")

    # Determine main issue (simplified logic)
    main_condition = "보통"
    if air_data.get("pm25_grade") in ["나쁨", "매우나쁨"]:
        main_condition = f"초미세먼지 {air_data['pm25_grade']}"
    elif air_data.get("pm10_grade") in ["나쁨", "매우나쁨"]:
        main_condition = f"미세먼지 {air_data['pm10_grade']}"
    elif air_data.get("so2_grade") in ["나쁨", "매우나쁨"]:
        main_condition = f"황사/이산화황 {air_data['so2_grade']}" # Simplified
        
    # Step B: Query Construction
    user_condition = user_profile.get("condition", "건강함")
    age_group = _normalize_age_group(user_profile.get("ageGroup"))
    
    # Primary Query: Specific
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
                        "index": "default",
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
            "reason": "OpenAI Client not initialized",
            "actionItems": [],
            "references": []
        }

    # [Logic Update] Calculate Deterministic Decision & Action Items
    pm25_g = air_data.get("pm25_grade", "보통")
    o3_g = air_data.get("o3_grade", "보통")
    
    decision_key = _calculate_decision(pm25_g, o3_g)
    decision_text, action_items = _get_display_content(age_group, decision_key)
    
    # Logic for dual bad condition text append
    # "둘 다 높은 경우: 더 나쁜 쪽을 따라가되, 문구는 '둘 다 높아요'로 1줄 추가"
    # -> If reasoning needs this, we can add it to prompt context or just append to decision text if needed.
    # The requirement says "문구는 '둘 다 높아요'로 1줄 추가". 
    # Let's append it to decision text if both are >= '나쁨'.
    p_score = GRADE_MAP.get(pm25_g, 0)
    o_score = GRADE_MAP.get(o3_g, 0)
    if p_score >= 2 and o_score >= 2:
        decision_text += " (미세먼지와 오존 둘 다 높아요!)"

    # Prepare Context
    context_text = "\n".join([f"- [출처: {doc.get('source', '가이드라인')}] {doc.get('text', '')}" for doc in relevant_docs]) if relevant_docs else "관련 의학적 가이드라인을 찾을 수 없습니다."
    
    system_prompt = """
    당신은 환경보건 의사입니다. 대기질 데이터와 환자의 기저질환 정보를 바탕으로 판단 근거(Reason)를 작성해주세요.
    
    [중요]
    1. 'decision'과 'actionItems'는 이미 시스템에서 계산되었습니다. 당신은 이 결정이 내려진 '의학적/환경적 이유(reason)'만 작성하면 됩니다.
    2. 제공된 [의학적 가이드라인] 내용을 최우선으로 반영하여 설명하세요.
    3. 반드시 JSON 형식으로 응답해야 합니다.
    
    응답 포맷:
    {
        "reason": "판단 근거 (가이드라인 내용 인용 포함)"
    }
    """
    
    user_prompt = f"""
    [상황 정보]
    - 대기질: PM2.5={pm25_g}, O3={o3_g}
    - 사용자: {age_group}, {user_condition}
    - 시스템 결정: {decision_text}
    - 시스템 행동수칙: {action_items}
    
    [의학적 가이드라인 (참고 문헌)]
    {context_text}
    
    위 결정이 내려진 배경과 이유를 가이드라인을 참고하여 친절하게 설명해주세요.
    """
    
    try:
        response = openai_client.chat.completions.create(
            model="gpt-5-nano",
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
            "reason": llm_result.get("reason", "정보를 불러오는 중 문제가 발생했습니다."),
            "actionItems": action_items,
            "references": list(set([doc.get("source", "Unknown Source") for doc in relevant_docs]))
        }
        
        # [Step F] Save to Cache
        if db is not None and cache_key:
            try:
                await db[CACHE_COLLECTION].update_one(
                    {"_id": cache_key},
                    {"$set": {"data": final_result, "created_at": datetime.now()}},
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
            "reason": "일시적인 오류로 상세 설명을 불러오지 못했습니다. 하지만 행동 지침은 위와 같이 준수해주세요.",
            "actionItems": action_items,
            "references": []
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
