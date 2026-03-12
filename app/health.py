import asyncio
import os
from typing import Any, Dict

from app import services

HEALTH_DEPENDENCY_TIMEOUT_SECONDS = 3.0


async def build_health_payload() -> Dict[str, Any]:
    mongo_reachable = False
    cache_ready = False

    if services.mongo_client is not None:
        try:
            await asyncio.wait_for(
                services.mongo_client.admin.command("ping"),
                timeout=HEALTH_DEPENDENCY_TIMEOUT_SECONDS,
            )
            mongo_reachable = True
        except Exception:
            mongo_reachable = False

    if services.db is not None:
        try:
            if getattr(services, "_cache_ttl_index_ready", False):
                cache_ready = True
            else:
                await asyncio.wait_for(
                    services._ensure_cache_ttl_index(),
                    timeout=HEALTH_DEPENDENCY_TIMEOUT_SECONDS,
                )
            cache_ready = True
        except Exception:
            cache_ready = False

    return {
        "ok": True,
        "service": "Epilogue API",
        "mongoReachable": mongo_reachable,
        "openaiConfigured": bool(services.OPENAI_API_KEY and services.openai_client),
        "vectorSearchEnabled": bool(services._vector_search_enabled),
        "cacheReady": cache_ready,
        "environment": os.getenv("SENTRY_ENVIRONMENT") or os.getenv("VERCEL_ENV") or os.getenv("ENVIRONMENT") or "production",
        "version": os.getenv("SENTRY_RELEASE") or os.getenv("VERCEL_GIT_COMMIT_SHA") or "dev",
    }
