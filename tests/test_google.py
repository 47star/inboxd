import json
import time
from datetime import datetime, timedelta
from unittest.mock import Mock

import httpx
import pytest

from inboxd.runtime.config import Settings
from inboxd.sources.google.auth import save_json
from inboxd.sources.google.client import GoogleClient, GoogleUnavailable
from inboxd.sources.google.collector import GoogleCollector
from inboxd.sources.google.records import normalize, record_id
from inboxd.storage.archive import Store

CONTAINER = {"id": "fixture", "summary": "Calendar", "timeZone": "Asia/Seoul"}
EVENT = {
    "id": "event",
    "summary": "Meeting",
    "description": "Details",
    "start": {"date": "2026-11-05"},
    "end": {"date": "2026-11-06"},
    "status": "confirmed",
}


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def reject(*args, **kwargs):
        pytest.fail("Offline tests must not use the network")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", reject)


@pytest.fixture
def collector(tmp_path):
    store = Store(tmp_path / "archive")
    value = GoogleCollector(Settings(data_dir=tmp_path), store, "google_calendar")
    value.client.close()
    value.client = Mock()
    value.account = "account"

    try:
        yield value
    finally:
        store.close()


def test_pagination_resumes_after_restart_and_connection_failure(collector):
    collector.client.get.side_effect = [
        {"items": [EVENT], "nextPageToken": "second"},
        GoogleUnavailable(0, "offline"),
    ]

    with pytest.raises(GoogleUnavailable):
        collector.sync_scope(CONTAINER)

    state = collector.checkpoint(collector.scope_key(CONTAINER))
    assert state["page_token"] == "second"
    assert len(state["seen"]) == 1
    assert not state.get("backfilled")
    replacement = GoogleCollector(collector.settings, collector.store, collector.name)
    replacement.client.close()
    replacement.client = Mock()
    replacement.account = collector.account
    replacement.client.get.return_value = {"items": [], "nextSyncToken": "complete"}
    result = replacement.sync_scope(CONTAINER)
    assert replacement.client.get.call_args.args[2]["pageToken"] == "second"
    assert result["backfilled"]
    assert result["sync_token"] == "complete"
    assert len(result["ids"]) == 1


def test_expired_sync_preserves_cache_until_full_reconciliation(collector):
    collector.client.get.return_value = {"items": [EVENT], "nextSyncToken": "old"}
    state = collector.sync_scope(CONTAINER)
    key = collector.scope_key(CONTAINER)
    collector.advance(key, {**state, "next_due": 0})
    collector.client.get.side_effect = GoogleUnavailable(410, "gone")

    with pytest.raises(GoogleUnavailable):
        collector.sync_scope(CONTAINER)

    identity = state["ids"][0]
    assert not collector.store.get("message:" + identity)["deleted"]
    collector.client.get.side_effect = None
    collector.client.get.return_value = {"items": [], "nextSyncToken": "new"}
    result = collector.sync_scope(CONTAINER)
    assert "timeMin" in collector.client.get.call_args.args[2]
    assert result["sync_token"] == "new"
    assert collector.store.get("message:" + identity)["deleted"]


def test_incremental_token_and_sparse_cancellation(collector):
    collector.client.get.return_value = {"items": [EVENT], "nextSyncToken": "first"}
    state = collector.sync_scope(CONTAINER)
    collector.advance(collector.scope_key(CONTAINER), {**state, "next_due": 0})
    collector.client.get.return_value = {
        "items": [{"id": EVENT["id"], "status": "cancelled"}],
        "nextSyncToken": "second",
    }
    collector.sync_scope(CONTAINER)
    params = collector.client.get.call_args.args[2]
    assert params["syncToken"] == "first"
    assert "timeMin" not in params
    saved = collector.store.get("message:" + state["ids"][0])
    assert saved["deleted"]
    assert saved["subject"] == "Meeting"
    assert saved["timestamp"] == "2026-11-04T15:00:00+00:00"


def test_tasks_flags_overlap_and_cleared_completion(collector):
    collector.name = "google_tasks"
    task = {
        "id": "task",
        "title": "Task",
        "notes": "Old notes",
        "status": "completed",
        "completed": "2026-11-05T00:00:00Z",
        "updated": "2026-11-05T01:00:00Z",
        "hidden": True,
    }
    collector.client.get.return_value = {"items": [task]}
    state = collector.sync_scope(CONTAINER)
    params = collector.client.get.call_args.args[2]
    assert all(
        params[key] == "true"
        for key in ("showCompleted", "showHidden", "showDeleted", "showAssigned")
    )
    assert datetime.fromisoformat(state["updated_min"]) == (
        datetime.fromisoformat(state["started_at"]) - timedelta(minutes=5)
    )
    collector.advance(collector.scope_key(CONTAINER), {**state, "next_due": 0})
    collector.client.get.return_value = {
        "items": [
            {"id": "task", "title": "Task", "status": "needsAction", "updated": task["updated"]}
        ]
    }
    collector.sync_scope(CONTAINER)
    assert collector.client.get.call_args.args[2]["updatedMin"] == state["updated_min"]
    saved = collector.store.get("message:" + state["ids"][0])
    assert "completed" not in saved["raw"]
    assert "notes" not in saved["raw"]
    assert not saved["deleted"]


def test_window_move_and_removed_container(collector):
    collector.client.get.return_value = {"items": [EVENT], "nextSyncToken": "first"}
    state = collector.sync_scope(CONTAINER)
    collector.advance(collector.scope_key(CONTAINER), {**state, "next_due": 0})
    collector.client.get.return_value = {
        "items": [{**EVENT, "start": {"date": "2099-01-01"}, "end": {"date": "2099-01-02"}}],
        "nextSyncToken": "second",
    }
    collector.sync_scope(CONTAINER)
    assert collector.store.get("message:" + state["ids"][0])["deleted"]
    collector.emit(normalize(collector.name, collector.account, CONTAINER, EVENT))
    collector.reconcile_containers([CONTAINER])
    collector.reconcile_containers([])
    assert collector.store.get("message:" + state["ids"][0])["deleted"]
    assert not collector.checkpoint(collector.scope_key(CONTAINER))["backfilled"]


def test_missing_final_token_does_not_advance_checkpoint(collector):
    collector.client.get.return_value = {"items": [EVENT]}

    with pytest.raises(GoogleUnavailable, match="no final sync token"):
        collector.sync_scope(CONTAINER)

    state = collector.checkpoint(collector.scope_key(CONTAINER))
    assert state["in_progress"]
    assert not state.get("sync_token")
    assert not state.get("backfilled")


def test_timezone_and_restored_fields():
    initial = normalize("google_calendar", "account", CONTAINER, EVENT)
    restored = normalize(
        "google_calendar",
        "account",
        CONTAINER,
        {**EVENT, "description": "", "start": {"dateTime": "2026-11-05T10:00:00+09:00"}},
        initial.model_dump(),
    )
    assert restored.timestamp == "2026-11-05T01:00:00+00:00"
    assert restored.id == record_id("google_calendar", "account", "fixture", "event")
    assert "Details" not in restored.text


def test_refresh_preserves_offline_token_and_identity(tmp_path):
    save_json(tmp_path / "client.json", {"client_id": "client", "client_secret": "secret"})
    save_json(
        tmp_path / "token.json",
        {
            "access_token": "old",
            "refresh_token": "offline",
            "account": "account",
            "email": "fixture@example.test",
            "expires_at": 0,
        },
    )

    def handler(request):
        assert request.url.path == "/token"
        assert b"refresh_token=offline" in request.content

        return httpx.Response(200, json={"access_token": "fresh", "expires_in": 3600})

    client = GoogleClient(tmp_path, httpx.MockTransport(handler))

    try:
        token = client.credentials()
        assert token["access_token"] == "fresh"
        assert token["refresh_token"] == "offline"
        assert token["account"] == "account"
        assert token["expires_at"] > time.time()
        assert json.loads((tmp_path / "token.json").read_text()) == token
        assert (tmp_path / "token.json").stat().st_mode & 0o777 == 0o600
    finally:
        client.close()
