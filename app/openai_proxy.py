import os
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Body, Header, HTTPException
from fastapi.responses import JSONResponse, Response

router = APIRouter()


def _settings() -> Dict[str, Any]:
    return {
        "openai_api_key": os.getenv("OPENAI_API_KEY"),
        "proxy_token": os.getenv("OPENAI_PROXY_TOKEN"),
        "upstream_base_url": (os.getenv("OPENAI_UPSTREAM_BASE_URL") or "https://api.openai.com/v1").rstrip("/"),
        "timeout_seconds": float(os.getenv("OPENAI_PROXY_TIMEOUT_SECONDS") or "180")
    }


def _authorize(proxy_token: Optional[str], provided_token: Optional[str]) -> None:
    # If OPENAI_PROXY_TOKEN is configured, require exact match from x-proxy-token.
    if proxy_token and provided_token != proxy_token:
        raise HTTPException(status_code=401, detail="unauthorized")


@router.get("/api/openai/v1/health")
async def openai_proxy_health() -> JSONResponse:
    cfg = _settings()
    return JSONResponse(
        content={
            "ok": True,
            "service": "openai-proxy",
            "upstream_base_url": cfg["upstream_base_url"],
            "proxy_token_required": bool(cfg["proxy_token"]),
            "openai_key_configured": bool(cfg["openai_api_key"])
        }
    )


@router.post("/api/openai/v1/responses")
async def proxy_openai_responses(
    payload: Dict[str, Any] = Body(...),
    x_proxy_token: Optional[str] = Header(default=None)
) -> Response:
    cfg = _settings()
    _authorize(cfg["proxy_token"], x_proxy_token)

    if not cfg["openai_api_key"]:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not configured")

    upstream_url = f"{cfg['upstream_base_url']}/responses"
    headers = {
        "Authorization": f"Bearer {cfg['openai_api_key']}",
        "Content-Type": "application/json"
    }

    try:
        async with httpx.AsyncClient(timeout=cfg["timeout_seconds"]) as client:
            upstream = await client.post(upstream_url, json=payload, headers=headers)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="OpenAI upstream timeout")
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"OpenAI upstream request failed: {type(exc).__name__}") from exc

    content_type = upstream.headers.get("content-type", "application/json")
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers={
            "Content-Type": content_type,
            "x-openai-proxy": "epilog-ai"
        }
    )
