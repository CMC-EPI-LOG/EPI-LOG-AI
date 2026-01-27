# Epilogue AI API Guide

This documentation provides details on how to integrate with the Epilogue AI API server.

## Base URL
- **Local:** `http://localhost:8000`
- **Production (Vercel):** `https://<your-project-name>.vercel.app`

---

## 1. Get Medical Advice (RAG)
Retrieves context-aware medical advice based on air quality and user profile.

- **Endpoint:** `POST /api/advice`
- **Content-Type:** `application/json`

### Request Body
| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `stationName` | String | Station name for Air Quality lookup | `"강남구"` |
| `userProfile` | Object | User's health profile | See below |

**`userProfile` Schema:**
```json
{
  "ageGroup": "infant" | "child" | "adult" | "elderly",
  "condition": "asthma" | "rhinitis" | "none" | "etc"
}
```

### Example Request
```bash
curl -X POST "https://<your-domain>/api/advice" \
     -H "Content-Type: application/json" \
     -d '{
           "stationName": "서초구",
           "userProfile": { "ageGroup": "child", "condition": "asthma" }
         }'
```

### Example Response
```json
{
  "decision": "오늘은 실내가 더 편해요 🏠",
  "reason": "현재 미세먼지 농도가 나쁨 수준이며, 천식 환자에게는 위험할 수 있습니다.",
  "actionItems": [
    "외출 대신 장난감 정리+찾기 게임",
    "실내에서 풍선배구/장애물 코스(가볍게)",
    "환기는 짧게(5–10분) 하고 바로 닫기"
  ],
  "references": [
    "질병관리청 미세먼지 대응지침 2024",
    "천식 및 알레르기 학회 가이드라인"
  ]
}
```

---

## 2. Ingest PDF (Single File)
Uploads a single PDF file to the vector database.

- **Endpoint:** `POST /api/ingest/pdf`
- **Content-Type:** `multipart/form-data`

### Request
| Field | Type | Description |
|-------|------|-------------|
| `file` | File | PDF file to upload |

### Example Request
```bash
curl -X POST -F "file=@/path/to/paper.pdf" "https://<your-domain>/api/ingest/pdf"
```

### Example Response
```json
{
  "status": "success",
  "message": "Successfully ingested 12 pages from paper.pdf",
  "inserted_ids": ["..."]
}
```

---

## Deployment to Vercel

1. **Install Vercel CLI** (if not installed):
   ```bash
   npm i -g vercel
   ```

2. **Deploy**:
   ```bash
   vercel
   ```

3. **Environment Variables**:
   Ensure the following variables are set in your Vercel Project Settings:
   - `MONGODB_URI`
   - `VOYAGE_API_KEY`
   - `OPENAI_API_KEY`
