from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import uvicorn
import os
from contextlib import asynccontextmanager

from app.services import get_medical_advice, ingest_pdf, db, get_clothing_recommendation
from app.openai_proxy import router as openai_proxy_router

# Define Request Model
class AdviceRequest(BaseModel):
    stationName: str
    userProfile: Dict[str, Any]

# Define Response Model
class AdviceResponse(BaseModel):
    decision: str
    csv_reason: Optional[str] = None
    reason: Optional[str] = None
    three_reason: List[str]  # 3 bullet points with **keyword** highlighting
    detail_answer: str       # Detailed medical explanation
    actionItems: List[str]
    references: List[str]


class ClothingRecommendationRequest(BaseModel):
    temperature: float = 22.0
    humidity: float = 45.0


class ClothingRecommendationResponse(BaseModel):
    summary: str
    recommendation: str
    tips: List[str]
    comfortLevel: str
    temperature: float
    humidity: float
    source: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    print("🚀 Epilogue API is starting up...")
    yield
    # Shutdown logic
    print("👋 Epilogue API is shutting down...")
    if db is not None:
        # Close MongoDB connection if necessary (Motor handles it, but good practice to allow cleanup)
        pass

app = FastAPI(title="Epilogue API", lifespan=lifespan)
app.include_router(openai_proxy_router)

@app.get("/")
def read_root():
    return {"status": "ok", "service": "Epilogue API"}

@app.post("/api/advice", response_model=AdviceResponse)
async def give_advice(request: AdviceRequest):
    try:
        # Delegate logic to service layer
        result = await get_medical_advice(request.stationName, request.userProfile)
        return JSONResponse(content=result)
    except Exception as e:
        print(f"Error processing advice request: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "decision": "Error",
                "three_reason": ["시스템 오류가 발생했습니다.", "잠시 후 다시 시도해주세요.", "문제가 지속되면 관리자에게 문의하세요."],
                "detail_answer": "Internal Server Error",
                "details": str(e),
                "actionItems": [],
                "references": []
            }
        )

@app.get("/api/air-quality")
async def get_air_quality_endpoint(stationName: str):
    """
    Public endpoint for air quality data retrieval.
    Replaces EPI-LOG-AIRKOREA /api/stations endpoint.
    
    Query Parameters:
    - stationName: Name of the monitoring station (e.g., "종로구", "신풍동")
    
    Returns:
    - Air quality data including PM2.5, PM10, O3, and other pollutants
    """
    from app.services import get_air_quality
    
    try:
        data = await get_air_quality(stationName)
        if not data:
            return JSONResponse(
                status_code=404,
                content={"error": f"No data found for station: {stationName}"}
            )
        
        return JSONResponse(content=data)
    except Exception as e:
        print(f"Error fetching air quality: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error", "details": str(e)}
        )


@app.post("/api/clothing-recommendation", response_model=ClothingRecommendationResponse)
async def clothing_recommendation(request: ClothingRecommendationRequest):
    try:
        result = get_clothing_recommendation(request.temperature, request.humidity)
        return JSONResponse(content=result)
    except Exception as e:
        print(f"Error generating clothing recommendation: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "summary": "옷차림 추천을 불러오지 못했어요.",
                "recommendation": "얇은 겉옷을 함께 준비해 체온 변화를 조절해 주세요.",
                "tips": ["실내외 온도차를 고려해 레이어드 착용을 권장해요."],
                "comfortLevel": "UNKNOWN",
                "temperature": float(request.temperature),
                "humidity": float(request.humidity),
                "source": "fallback"
            },
        )

from fastapi import UploadFile, File

@app.post("/api/ingest/pdf")
async def upload_pdf(file: UploadFile = File(...)):
    if not file.filename.endswith(".pdf"):
        return JSONResponse(status_code=400, content={"message": "File must be a PDF."})
    
    try:
        content = await file.read()
        result = await ingest_pdf(content, file.filename)
        return JSONResponse(content=result)
    except Exception as e:
         return JSONResponse(status_code=500, content={"message": str(e)})

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
