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
  "ageGroup": "infant" | "elementary_low" | "elementary_high" | "teen",
  "condition": "asthma" | "rhinitis" | "none" | "etc"
}
```

**`ageGroup` Label Guide (KR):**
- `infant`: 유아
- `elementary_low`: 초등 저학년
- `elementary_high`: 초등 고학년
- `teen`: 청소년

### Example Request
```bash
curl -X POST "https://<your-domain>/api/advice" \
     -H "Content-Type: application/json" \
     -d '{
           "stationName": "서초구",
           "userProfile": { "ageGroup": "elementary_high", "condition": "asthma" }
         }'
```

### Example Response
```json
{
  "decision": "오늘은 실내가 더 편해요 🏠",
  "three_reason": [
    "현재 미세먼지가 **나쁨** 수준이라 호흡기가 예민할 수 있어요.",
    "특히 **천식**이 있다면 기도가 수축될 위험이 높습니다.",
    "오늘은 **실외 활동**을 자제하고 마스크를 꼭 챙겨주세요."
  ],
  "detail_answer": "현재 미세먼지 농도가 나쁨 수준이며, 천식 환자에게는 위험할 수 있습니다. 온도와 습도를 고려할 때 기도가 더욱 민감해질 수 있으므로, 실외 활동을 최소화하고 실내에서 안전하게 지내는 것이 좋습니다.",
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

**Response Fields:**
- `decision` (String): Short decision text from the system
- `three_reason` (Array[String]): 3 concise summary points with `**keyword**` markers for frontend highlighting
- `detail_answer` (String): Detailed medical explanation
- `actionItems` (Array[String]): Recommended action items
- `references` (Array[String]): Source references from medical guidelines

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

## 3. OpenAI Responses Proxy (Server-to-Server)
OpenAI Responses API 호출을 이 서버가 중계합니다.

- **Health Endpoint:** `GET /api/openai/v1/health`
- **Proxy Endpoint:** `POST /api/openai/v1/responses`
- **Content-Type:** `application/json`

### Security
- `OPENAI_PROXY_TOKEN`이 설정된 경우, 요청 헤더 `x-proxy-token`이 반드시 일치해야 합니다.

### Request Body
- OpenAI Responses API payload를 그대로 전달합니다.

### Example Request
```bash
curl -X POST "https://<your-domain>/api/openai/v1/responses" \
  -H "Content-Type: application/json" \
  -H "x-proxy-token: <OPENAI_PROXY_TOKEN>" \
  -d '{
    "model": "gpt-5-nano",
    "input": [{"role":"user","content":[{"type":"input_text","text":"hello"}]}]
  }'
```

### Example Worker Setting
EPI-LOG-USERLOG Worker에서는 아래처럼 설정하면 이 프록시를 사용합니다.

- `OPENAI_BASE_URL=https://<your-domain>/api/openai/v1`
- 프록시 토큰 헤더(`x-proxy-token`)가 필요하면 Worker 코드에서 함께 전달해야 합니다.

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
   - `OPENAI_PROXY_TOKEN` (optional but recommended)
   - `OPENAI_UPSTREAM_BASE_URL` (optional, default `https://api.openai.com/v1`)
   - `OPENAI_PROXY_TIMEOUT_SECONDS` (optional, default `300`)
