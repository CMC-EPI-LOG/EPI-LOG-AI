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
  "decision": "X",
  "reason": "현재 미세먼지 농도가 나쁨 수준이며, 천식 환에게는 위험할 수 있습니다.",
  "actionItems": [
    "실내 활동 권장",
    "부득이한 외출 시 KF80 이상 마스크 착용",
    "공기청정기 가동"
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
