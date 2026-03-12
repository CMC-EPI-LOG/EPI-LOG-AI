import pytest
from fastapi import HTTPException

from app.openai_proxy import _ensure_proxy_config


def test_proxy_config_requires_token_in_non_local_env():
    with pytest.raises(HTTPException) as error:
        _ensure_proxy_config(
            {
                "proxy_token_required": True,
                "proxy_token": None,
                "runtime_env": "production",
            }
        )

    assert error.value.status_code == 503


def test_proxy_config_allows_missing_token_in_local_env():
    _ensure_proxy_config(
        {
            "proxy_token_required": True,
            "proxy_token": None,
            "runtime_env": "development",
        }
    )
