from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from inboxd.api.http import create_app, token_for
from inboxd.domain.records import Record
from inboxd.runtime.config import Settings
from inboxd.sources import registry
from inboxd.storage.archive import Store


@pytest.fixture
def discovery(monkeypatch):
    registry.plugins.cache_clear()
    points = []
    monkeypatch.setattr(registry, "entry_points", lambda **kwargs: points)
    yield points
    registry.plugins.cache_clear()


def definition(name="fixture", factory=Mock):
    return {
        "name": name,
        "description": "Test source",
        "collector": factory,
        "config": {"enabled": False},
        "content_kinds": ("custom_record",),
    }


def test_discovery_cached_and_broken_plugin_isolated(discovery):
    good = Mock(return_value=lambda: definition())
    broken = Mock(side_effect=ImportError("Missing test dependency"))
    discovery.extend(
        [SimpleNamespace(name="fixture", load=good), SimpleNamespace(name="broken", load=broken)]
    )

    for _ in range(100):
        assert "custom_record" in registry.content_kinds()

    good.assert_called_once()
    broken.assert_called_once()
    assert registry.plugins()["broken"]["error"] == "ImportError"
    settings = Settings(sources={"broken": {"enabled": False}})
    assert not settings.source_enabled("broken")


def test_duplicate_plugin_is_unavailable(discovery):
    point = SimpleNamespace(name="fixture", load=lambda: definition)
    discovery.extend([point, point])
    assert registry.plugins()["fixture"]["error"] == "Duplicate collector entry point"


def test_factory_failure_keeps_http_and_index_available(discovery, tmp_path):
    broken = Mock(side_effect=RuntimeError("Constructor failure"))
    disabled = Mock(side_effect=AssertionError("Disabled collector was constructed"))
    discovery.extend(
        [
            SimpleNamespace(name="broken", load=lambda: lambda: definition("broken", broken)),
            SimpleNamespace(name="disabled", load=lambda: lambda: definition("disabled", disabled)),
        ]
    )
    settings = Settings(data_dir=tmp_path, sources={"broken": {"enabled": True}})

    with TestClient(create_app(settings)) as client:
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)
        response = client.get("/status")
        assert response.status_code == 200
        states = response.json()["collectors"]
        assert states["broken"]["state"] == "initialization_failed"
        assert states["disabled"]["state"] == "disabled"
        assert states["index"]["state"] == "ready"

    disabled.assert_not_called()


def test_removed_plugin_keeps_queries_and_rebuilt_indexes(discovery, tmp_path):
    discovery.append(SimpleNamespace(name="fixture", load=lambda: definition))
    store = Store(tmp_path)

    try:
        store.ingest(
            Record(
                id="archived",
                source="fixture",
                source_id="1",
                account="test",
                conversation_id="test",
                timestamp="2026-01-01T00:00:00Z",
                kind="custom_record",
                text="Preserved archive content",
            )
        )
        store.index_pending()
        store.set("meta:search_schema", -1)
        store.delete("meta:query_indexes")
        store.delete("meta:record_counts")
    finally:
        store.close()

    discovery.clear()
    registry.plugins.cache_clear()
    store = Store(tmp_path)

    try:
        store.index_pending()
        assert store.search("Preserved", source="fixture")["total"] == 1
        assert store.recent(source="fixture")["items"][0]["id"] == "archived"
        source = store.sources("fixture")["items"][0]
        assert source["content_records"] == 1
        assert source["collector"]["state"] == "plugin_removed"
    finally:
        store.close()

    with TestClient(create_app(Settings(data_dir=tmp_path))) as client:
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)
        result = client.get("/recent", params={"source": "fixture"})
        assert result.status_code == 200
        assert result.json()["context"]["sources"][0]["state"] == "plugin_removed"


@pytest.mark.parametrize(
    "source, options",
    [
        ("slack", {"enabled": "false"}),
        ("slack", {"enabeld": False}),
        ("slack", {"poll_seconds": True}),
        ("slack", {"poll_seconds": float("nan")}),
        ("slack", {"poll_seconds": float("inf")}),
        ("calendar", {"start_year": 0}),
        ("calendar", {"future_years": 51}),
        ("redmine", {"reconcile_batch_size": 0}),
        ("redmine", {"reconcile_batch_size": 101}),
        ("redmine", {"url": 123}),
        ("missing", {"enabled": True}),
    ],
)
def test_invalid_configuration_fails_before_startup(source, options):
    with pytest.raises(ValidationError):
        Settings(sources={source: options})


def test_old_flat_settings_rejected():
    with pytest.raises(ValidationError):
        Settings(slack_enabled=False)


def test_removed_disabled_configuration_retained():
    settings = Settings(sources={"uninstalled": {"enabled": False, "custom_option": "saved"}})
    assert not settings.source_enabled("uninstalled")
    assert settings.sources["uninstalled"]["custom_option"] == "saved"
