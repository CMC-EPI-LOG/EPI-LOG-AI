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
| `currentAirQuality` | Object | 상위 BFF가 전달하는 authoritative 현재 대기질/온습도 컨텍스트 | See below |
| `authoritativeAirQuality` | Object | `currentAirQuality`와 동일 의미의 별칭. 둘 다 오면 이 값을 우선 사용 | See below |
| `airQualitySummary` | String | 현재 수치를 한 줄로 요약한 설명. 프롬프트 기준 문장으로 사용 | `"강남구 기준, 측정시각 2026-03-08 21:00, 초미세먼지 19ug/m3 ..."` |

**`userProfile` Schema (canonical values):**
```json
{
  "ageGroup": "infant" | "toddler" | "elementary_low" | "elementary_high" | "teen_adult",
  "condition": "general" | "rhinitis" | "asthma" | "atopy"
}
```

Aliases like `teen`, `none`, `healthy`, `normal`, `일반` are accepted and normalized internally.

**`currentAirQuality` / `authoritativeAirQuality` Schema (optional):**
```json
{
  "requestedStation": "강남구",
  "resolvedStation": "강남구",
  "stationName": "강남구",
  "sidoName": "서울",
  "dataTime": "2026-03-08 21:00",
  "pm25_value": 19,
  "pm10_value": 29,
  "o3_value": 0.039,
  "no2_value": 0.017,
  "co_value": 0.4,
  "so2_value": 0.003,
  "temp": 2,
  "humidity": 70
}
```

**`ageGroup` Label Guide (KR, canonical):**
- `infant`: 영아(0-2세)
- `toddler`: 유아(3-6세)
- `elementary_low`: 초등 저학년
- `elementary_high`: 초등 고학년
- `teen_adult`: 청소년/성인

### Example Request
```bash
curl -X POST "https://<your-domain>/api/advice" \
     -H "Content-Type: application/json" \
     -d '{
           "stationName": "서초구",
           "userProfile": { "ageGroup": "elementary_high", "condition": "asthma" },
           "currentAirQuality": {
             "resolvedStation": "서초구",
             "sidoName": "서울",
             "dataTime": "2026-03-08 21:00",
             "pm25_value": 19,
             "pm10_value": 29,
             "o3_value": 0.039,
             "no2_value": 0.017,
             "temp": 2,
             "humidity": 70
           },
           "airQualitySummary": "서초구 기준, 측정시각 2026-03-08 21:00, 초미세먼지 19ug/m3, 미세먼지 29ug/m3, 오존 0.039ppm, 이산화질소 0.017ppm, 기온 2도, 습도 70%"
         }'
```

### Example Response
```json
{
  "decision": "실내 놀이가 더 안전해요",
  "csv_reason": "초미세먼지 농도가 높아 호흡기 자극 위험이 커졌어요.",
  "reason": "초미세먼지와 오존 수준을 기준으로 행동 지침을 안내합니다.",
  "three_reason": [
    "현재 **초미세먼지**가 높아 **호흡기 자극** 위험이 커졌어요.",
    "특히 **천식**이 있다면 증상이 악화될 수 있어요.",
    "오늘은 **실외 활동**을 줄이고 실내 공기질 관리가 필요해요."
  ],
  "detail_answer": "현재 대기질과 사용자 프로필을 종합하면 실외 노출 최소화가 권장됩니다.",
  "actionItems": [
    "외출 대신 실내 활동으로 대체하기",
    "환기는 짧게 하고 즉시 닫기",
    "귀가 후 손/얼굴 세정하기"
  ],
  "references": [
    "질병관리청 미세먼지 대응지침 2024",
    "천식 및 알레르기 학회 가이드라인"
  ],
  "pm25_value": 51,
  "pm10_value": 72,
  "o3_value": 0.041,
  "no2_value": 0.019
}
```

**Response Fields:**
- `decision` (String): CSV `메인문구` 기반의 최종 결정 문구
- `csv_reason` (String | null): CSV `이유` 컬럼 값
- `reason` (String | null): `detail_answer`와 동일한 상세 설명(호환용)
- `three_reason` (Array[String]): 3개 핵심 요약 문장
- `detail_answer` (String): 상세 설명
- `actionItems` (Array[String]): CSV `행동1~3` 기반 행동 지침
- `references` (Array[String]): 벡터 검색으로 참조된 출처
- `pm25_value`, `pm10_value`, `o3_value`, `no2_value` (Number): 현재 대기질 수치(실행 시점에 포함될 수 있음)

Note:
- OpenAI 생성이 실패해도 `/api/advice`는 `decision/actionItems/csv_reason/실측값`을 바탕으로 `reason`, `detail_answer`, `three_reason`를 결정형 문장으로 합성해 반환합니다.
- `currentAirQuality` 또는 `authoritativeAirQuality`가 오면 AI 서버는 자체 대기질 조회보다 이 값을 우선 사용합니다.
- OpenAI 프롬프트도 전달된 현재 수치와 측정시각을 사실 기준으로 사용하며, 캐시 키 역시 해당 수치와 온습도를 포함해 분리됩니다.
- 부분 payload만 와도 누락 필드는 서버 내부 조회값으로 보완한 뒤 overlay된 결과를 기준으로 판단합니다.
- 서버 내부 조회는 `weather_forecast.weather_forecast_data_shadow`에서 KMA 최신 예보를 우선 읽어 온습도를 결합하고, KMA 미스일 때만 기존 문서값/기본값으로 내려갑니다.

### CSV Mapping Rule
`/api/advice`는 CSV 컬럼명을 그대로 반환하지 않고 아래처럼 매핑해서 반환합니다.

| CSV Column | Returned Field | Returned? | Notes |
|---|---|---|---|
| `대기등급` | - | No | 내부에서 최종 등급 계산에 사용 |
| `메인문구` | `decision` | Yes | 핵심 결정 문구 |
| `연령대` | - | No | 요청 `userProfile.ageGroup`으로 입력받아 사용 |
| `이유` | `csv_reason` | Yes | CSV 근거 문장 |
| `질환군` | - | No | 요청 `userProfile.condition`으로 입력받아 사용 |
| `행동1` | `actionItems[0]` | Yes | 행동 지침 배열 |
| `행동2` | `actionItems[1]` | Yes | 행동 지침 배열 |
| `행동3` | `actionItems[2]` | Yes | 행동 지침 배열 |

Note: `ageGroup=infant`인 경우 `actionItems` 맨 앞에 `"※ 주의: 마스크 착용 금지(질식 위험)"` 경고 문구가 추가될 수 있습니다.

---

## 2. Get Air Quality
Returns latest air quality snapshot for a station (with internal fallback chain).

- **Endpoint:** `GET /api/air-quality`
- **Query Parameters:**

| Field | Type | Required | Description | Example |
|---|---|---|---|---|
| `stationName` | String | Yes | 측정소/지역명 | `"종로구"` |

### Example Request
```bash
curl -G "https://<your-domain>/api/air-quality" \
  --data-urlencode "stationName=종로구"
```

### Example Response
```json
{
  "stationName": "종로구",
  "sidoName": "서울",
  "pm25_value": 23,
  "pm10_value": 41,
  "o3_value": 0.031,
  "no2_value": 0.018,
  "co_value": 0.5,
  "so2_value": 0.003,
  "pm25_grade": "보통",
  "pm10_grade": "보통",
  "o3_grade": "보통",
  "no2_grade": "좋음",
  "co_grade": "좋음",
  "so2_grade": "좋음",
  "temp": 22.0,
  "humidity": 45.0,
  "dataTime": "2026-03-03 13:00"
}
```

**Response Fields:**
- `stationName` (String): 최종 매칭된 측정소명
- `sidoName` (String | null): 시/도명
- `pm25_value`, `pm10_value`, `o3_value`, `no2_value`, `co_value`, `so2_value` (Number): 오염물질 수치
- `pm25_grade`, `pm10_grade`, `o3_grade`, `no2_grade`, `co_grade`, `so2_grade` (String): 4단계 등급(`좋음/보통/나쁨/매우나쁨`)
- `temp` (Number): 기온
- `humidity` (Number): 습도
- `dataTime` (String | null): 관측 시각

**Status Codes:**
- `200`: 정상 반환
- `404`: 해당 측정소 데이터 없음
- `500`: 내부 오류

동작 우선순위:
1. MongoDB `air_quality_data` 최신 데이터
2. Air Korea fallback API
3. KMA weather forecast DB에서 최신 온습도 보강
4. Mock data fallback

---

## 3. Clothing Recommendation (Rule-based)
현재 버전은 기온/습도 규칙 기반 옷차림 추천을 제공합니다.

- **Endpoint:** `POST /api/clothing-recommendation`
- **Content-Type:** `application/json`

### Request Body
| Field | Type | Description |
|-------|------|-------------|
| `temperature` | Number | 기온 (기본값: `22.0`) |
| `humidity` | Number | 습도 (기본값: `45.0`) |

### Example Request
```bash
curl -X POST "https://<your-domain>/api/clothing-recommendation" \
  -H "Content-Type: application/json" \
  -d '{
    "temperature": 26.1,
    "humidity": 71
  }'
```

### Example Response
```json
{
  "summary": "다소 덥고 습한 날씨예요. 통풍이 잘되는 옷차림을 추천해요.",
  "recommendation": "반팔 + 얇은 바람막이 + 통풍 좋은 긴바지를 추천해요.",
  "tips": [
    "바깥 활동 후 겉옷을 바로 털고 손/얼굴을 씻어주세요.",
    "땀을 빨리 말리는 소재를 선택하세요."
  ],
  "comfortLevel": "WARM",
  "temperature": 26.1,
  "humidity": 71.0,
  "source": "rule-based-v1"
}
```

동작:
- 현재 구현은 규칙 기반으로만 동작합니다.
- 서버 오류 시 `source: "fallback"` 응답으로 대체됩니다.

---

## 4. Ingest PDF (Single File)
Uploads a single PDF file to the vector database.

- **Endpoint:** `POST /api/ingest/pdf`
- **Content-Type:** `multipart/form-data`

### Request
| Field | Type | Description |
|-------|------|-------------|
| `file` | File | PDF file to upload |

---

## 5. Ops Metrics
운영용 지표를 JSON으로 반환합니다.

- **Endpoint:** `GET /api/admin/ops-metrics`
- **Query Parameters:** `hours`(기본 `24`), `recent`(기본 `50`)
- **Auth:** `ADMIN_DASHBOARD_TOKEN`이 설정된 경우 `x-admin-token` 헤더 또는 `?token=` 쿼리 필요

### Example Request
```bash
curl -G "https://<your-domain>/api/admin/ops-metrics" \
  -H "x-admin-token: <ADMIN_DASHBOARD_TOKEN>" \
  --data-urlencode "hours=24" \
  --data-urlencode "recent=20"
```

### Example Response Fields
- `fallbackRatio`
- `overlayUsageRatio`
- `llmTimeoutRatio`
- `staleCacheUsageRatio`
- `cacheHitRatio`
- `forecastIngestStatus`
- `forecastIngestStaleRatio`
- `stationResolutionFailureRatio`
- `airFetchModeBreakdown`
- `airSourceBreakdown`
- `weatherSourceBreakdown`
- `stageBreakdown`
- `recentEvents`

---

## 6. Ops Dashboard
브라우저에서 운영 지표를 바로 보는 관리자 대시보드입니다.

- **Endpoint:** `GET /admin/ops-dashboard`
- **Query Parameters:** `hours`, `recent`
- **Auth:** `ADMIN_DASHBOARD_TOKEN`이 설정된 경우 `x-admin-token` 헤더 또는 `?token=` 쿼리 필요

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

## 5. OpenAI Responses Proxy (Server-to-Server)
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
