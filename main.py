from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, List
import uvicorn
import os
from contextlib import asynccontextmanager

from app.services import get_medical_advice, ingest_pdf, db

# Define Request Model
class AdviceRequest(BaseModel):
    stationName: str
    userProfile: Dict[str, Any]

# Define Response Model
class AdviceResponse(BaseModel):
    decision: str
    reason: str
    actionItems: List[str]
    references: List[str]

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
                "reason": "Internal Server Error",
                "details": str(e),
                "actionItems": [],
                "references": []
            }
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
