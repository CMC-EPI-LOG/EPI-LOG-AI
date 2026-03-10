import os
from typing import Any, Mapping, Optional

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration

_SENTRY_INITIALIZED = False


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default

    try:
        return float(raw.strip())
    except Exception:
        return default


def initialize_sentry() -> bool:
    global _SENTRY_INITIALIZED

    if _SENTRY_INITIALIZED:
        return True

    dsn = (os.getenv("SENTRY_DSN") or "").strip()
    if not dsn:
        return False

    sentry_sdk.init(
        dsn=dsn,
        environment=(
            os.getenv("SENTRY_ENVIRONMENT")
            or os.getenv("VERCEL_ENV")
            or os.getenv("ENVIRONMENT")
            or "production"
        ),
        release=(os.getenv("SENTRY_RELEASE") or os.getenv("VERCEL_GIT_COMMIT_SHA") or None),
        traces_sample_rate=_float_env("SENTRY_TRACES_SAMPLE_RATE", 0.0),
        profiles_sample_rate=_float_env("SENTRY_PROFILES_SAMPLE_RATE", 0.0),
        integrations=[FastApiIntegration()],
        send_default_pii=False,
    )
    _SENTRY_INITIALIZED = True
    return True


def capture_exception(
    error: Exception,
    *,
    route: str,
    tags: Optional[Mapping[str, str]] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> None:
    if not _SENTRY_INITIALIZED:
        return

    with sentry_sdk.push_scope() as scope:
        scope.set_tag("api.route", route)

        for key, value in (tags or {}).items():
            scope.set_tag(key, value)

        for key, value in (extra or {}).items():
            scope.set_extra(key, value)

        sentry_sdk.capture_exception(error)
