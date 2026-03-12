import asyncio

from app import services
from app.health import build_health_payload


class _FakeAdmin:
    async def command(self, name: str):
        assert name == "ping"
        return {"ok": 1}


class _FakeClient:
    admin = _FakeAdmin()


def test_build_health_payload_reports_core_dependencies(monkeypatch):
    async def _fake_cache_ready():
        return None

    monkeypatch.setattr(services, "mongo_client", _FakeClient())
    monkeypatch.setattr(services, "db", object())
    monkeypatch.setattr(services, "OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(services, "openai_client", object())
    monkeypatch.setattr(services, "_vector_search_enabled", True)
    monkeypatch.setattr(services, "_ensure_cache_ttl_index", _fake_cache_ready)

    payload = asyncio.run(build_health_payload())

    assert payload["ok"] is True
    assert payload["mongoReachable"] is True
    assert payload["openaiConfigured"] is True
    assert payload["vectorSearchEnabled"] is True
    assert payload["cacheReady"] is True
