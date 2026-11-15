import asyncio

import pytest
from fastapi.testclient import TestClient

from inboxd.api.http import create_app, token_for
from inboxd.api.mcp import mcp
from inboxd.domain.records import Record
from inboxd.runtime.config import Settings
from inboxd.sources.registry import plugins
from inboxd.storage.archive import Store


def record(identity, source="slack", **updates):
    return Record.model_validate(
        {
            "id": identity,
            "source": source,
            "account": "fixture",
            "source_id": identity,
            "conversation_id": source + ":conversation",
            "timestamp": "2026-01-01T00:00:00Z",
            **updates,
        }
    )


def test_recent_source_pagination_and_index_recovery(tmp_path):
    store = Store(tmp_path)

    try:
        for identity in ("a", "b", "c"):
            store.ingest(record(identity))

        store.ingest(record("mail", "mail"))
        store.ingest(record("user", kind="user"))
        store.ingest(record("deleted", deleted=True))
        store.ingest(record("future", timestamp="2030-01-01T00:00:00Z"))
        first = store.recent("slack", before="2026-01-01T09:00:00+09:00", limit=2)
        second = store.recent(
            "slack", before="2026-01-01T00:00:00Z", cursor=first["next_cursor"], limit=2
        )
        assert [item["id"] for item in first["items"] + second["items"]] == ["c", "b", "a"]
        assert second["next_cursor"] is None

        with pytest.raises(ValueError):
            store.recent("mail", cursor=first["next_cursor"])

        store.ingest(record("future", deleted=True))
        store.ingest(record("a", timestamp="2025-01-01T00:00:00Z"))
        assert [item["id"] for item in store.recent("slack")["items"]] == ["c", "b", "a"]
        store.delete("meta:query_indexes")

        for key, _ in store.scan("recent:"):
            store.delete(key)
    finally:
        store.close()

    recovered = Store(tmp_path)

    try:
        assert [item["id"] for item in recovered.recent("slack")["items"]] == ["c", "b", "a"]
        events = recovered.changes(source="mail")
        assert [item["message"]["id"] for item in events["items"]] == ["mail"]
        assert recovered.changes(cursor=events["cursor"], source="mail")["items"] == []
    finally:
        recovered.close()


def test_local_api_source_validation_and_auth(tmp_path):
    settings = Settings(
        data_dir=tmp_path,
        sources={
            source: {"enabled": False}
            for source in ("messages", "mail", "slack", "calendar", "notes", "reminders")
        },
    )

    with TestClient(create_app(settings)) as client:
        assert client.get("/sources").status_code == 401
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)
        assert len(client.get("/sources").json()["items"]) == len(plugins())
        assert client.get("/recent", params={"source": "unknown"}).status_code == 400
        assert client.get("/recent", params={"after": "2026-01-01"}).status_code == 400
        assert client.get("/recent", headers={"Origin": "https://example.com"}).status_code == 403
        assert client.get("/recent", headers={"Host": "example.com"}).status_code == 400


def test_mcp_source_discovery_and_enum_schema():
    tools = {tool.name: tool for tool in asyncio.run(mcp.list_tools())}
    assert "list_sources" in tools

    for name in ("get_recent_records", "search_messages", "get_changes"):
        schema = tools[name].inputSchema["properties"]["source"]
        assert schema["anyOf"][-1]["type"] == "null"
