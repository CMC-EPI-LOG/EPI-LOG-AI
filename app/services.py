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
    
    age_group = user_profile.get("ageGroup", "unknown")
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
    age_group = user_profile.get("ageGroup", "성인")
    
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
        
    # Prepare Context
    context_text = "\n".join([f"- [출처: {doc.get('source', '가이드라인')}] {doc.get('text', '')}" for doc in relevant_docs]) if relevant_docs else "관련 의학적 가이드라인을 찾을 수 없습니다."
    
    system_prompt = """
    당신은 환경보건 의사입니다. 대기질 데이터와 환자의 기저질환 정보를 바탕으로 오늘의 행동 지침을 내려주세요.
    
    [중요]
    1. 제공된 [의학적 가이드라인] 내용을 최우선으로 반영하여 조언을 작성하세요.
    2. 가이드라인에 근거가 있다면, 그 내용을 바탕으로 판단 이유를 설명하세요.
    3. 반드시 JSON 형식으로 응답해야 합니다.
    
    응답 포맷:
    {
        "decision": "O" | "X" | "△",
        "reason": "판단 근거 (가이드라인 내용 인용 포함)",
        "actionItems": ["행동요령1", "행동요령2", "행동요령3"]
    }
    """
    
    user_prompt = f"""
    [상황 정보]
    - 대기질 상태: {json.dumps(air_data, ensure_ascii=False)}
    - 사용자 정보: {json.dumps(user_profile, ensure_ascii=False)}
    
    [의학적 가이드라인 (참고 문헌)]
    {context_text}
    
    위 정보를 종합하여 이 사용자에게 맞는 오늘의 행동 지침을 작성해주세요.
    가이드라인의 내용을 적극적으로 활용하세요.
    """
    
    try:
        response = openai_client.chat.completions.create(
            model="gpt-5-nano",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.7 # Slight reduction for more adherence to context
        )
        
        content = response.choices[0].message.content
        result_json = json.loads(content)
        
        # [Step E] Add References
        references = list(set([doc.get("source", "Unknown Source") for doc in relevant_docs]))
        result_json["references"] = references
        
        # [Step F] Save to Cache
        if db is not None and cache_key:
            try:
                await db[CACHE_COLLECTION].update_one(
                    {"_id": cache_key},
                    {"$set": {"data": result_json, "created_at": datetime.now()}},
                    upsert=True
                )
                print(f"💾 Saved to cache: {cache_key}")
            except Exception as e:
                print(f"Error saving to cache: {e}")
                
        return result_json
        
    except Exception as e:
        print(f"Error calling OpenAI: {e}")
        return {
            "decision": "Error",
            "reason": f"Failed to generate advice: {str(e)}",
            "actionItems": [],
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
