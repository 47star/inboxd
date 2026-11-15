import asyncio
from importlib.metadata import distribution
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from inboxd.api.http import create_app, token_for
from inboxd.domain.records import Record
from inboxd.ingestion.attachments import AttachmentWorker
from inboxd.ingestion.collector import Collector
from inboxd.runtime.config import Settings, load_settings
from inboxd.sources.organizer import OrganizerCollector
from inboxd.sources.registry import plugins
from inboxd.storage.archive import Store


def test_independent_source_intervals(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        "[sources.messages]\npoll_seconds = 7\nreconcile_seconds = 400\n"
        "[sources.notes]\npoll_seconds = 25\n"
        "[sources.calendar]\npoll_seconds = 600\n"
        "[sources.google_calendar]\npoll_seconds = 90\n"
        "[sources.google_tasks]\npoll_seconds = 120\n"
    )
    settings = load_settings(str(path))
    assert settings.source_interval("messages") == 7
    assert settings.source_interval("messages", "reconcile") == 400
    assert settings.source_interval("mail", "reconcile") == 300
    assert settings.source_interval("notes") == 25
    assert settings.source_interval("calendar") == 600
    assert settings.source_interval("reminders") == 60
    assert settings.source_interval("google_calendar") == 90
    assert settings.source_interval("google_tasks") == 120


@pytest.mark.parametrize(
    "values",
    [
        {"sources": {"notes": {"poll_seconds": 0}}},
        {"sources": {"google_tasks": {"reconcile_seconds": -1}}},
    ],
)
def test_invalid_source_settings_rejected(values):
    with pytest.raises(ValidationError):
        Settings(**values)


def test_example_is_loadable_and_declares_every_source():
    path = Path(__file__).parents[1] / "config.example.toml"
    settings = load_settings(str(path))
    builtin_names = {
        point.name
        for point in distribution("inboxd").entry_points
        if point.group == "inboxd.collectors"
    }
    assert set(settings.sources) == builtin_names


def test_disabled_source_stops_collector_and_keeps_cached_queries(tmp_path, monkeypatch):
    started = []

    async def supervise(collector):
        started.append(collector.name)
        collector.status("watching")
        await asyncio.Event().wait()

    monkeypatch.setattr(Collector, "supervise", supervise)
    store = Store(tmp_path)
    store.ingest(
        Record(
            id="cached",
            source="slack",
            source_id="fixture",
            account="fixture",
            conversation_id="fixture",
            timestamp="2026-11-05T00:00:00Z",
            text="Cached body",
        )
    )
    store.set(
        "status:slack", {"state": "backfilling", "backfill_running": True, "socket_connected": True}
    )
    store.close()
    flags = {source: {"enabled": False} for source in plugins()}
    settings = Settings(data_dir=tmp_path, sources={**flags, "mail": {"enabled": True}})

    with TestClient(create_app(settings)) as client:
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)
        slack = client.get("/sources", params={"source": "slack"}).json()["items"][0]
        assert slack["collector"]["state"] == "disabled"
        assert not slack["collector"]["enabled"]
        assert not slack["collector"]["backfill_running"]
        assert not slack["collector"]["socket_connected"]
        recent = client.get("/recent", params={"source": "slack"}).json()
        assert recent["items"][0]["id"] == "cached"
        assert (
            "collection_disabled_cached_records_only" in recent["context"]["sources"][0]["issues"]
        )
        assert started == ["mail"]

    started.clear()
    settings = Settings(data_dir=tmp_path, sources={**flags, "slack": {"enabled": True}})

    with TestClient(create_app(settings)) as client:
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)
        slack = client.get("/sources", params={"source": "slack"}).json()["items"][0]
        assert slack["collector"]["enabled"]
        assert slack["stored_records"] == 1
        assert started == ["slack"]


def test_disabled_source_attachment_jobs_stay_pending(tmp_path, monkeypatch):
    store = Store(tmp_path)
    worker = AttachmentWorker(
        Settings(data_dir=tmp_path, sources={"mail": {"enabled": False}}), store
    )
    worker.process = AsyncMock()

    async def stop(seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr("inboxd.ingestion.attachments.asyncio.sleep", stop)

    try:
        for source in ["mail", "slack"]:
            store.set(
                "attachment:" + source, {"source": source, "state": "pending", "attachment": {}}
            )

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(worker.run())

        assert worker.process.await_count == 1
        assert worker.process.call_args.args[0] == "attachment:slack"
        assert store.get("attachment:mail")["state"] == "pending"
    finally:
        store.close()


def test_native_source_receives_its_own_timer(tmp_path):
    helper = tmp_path / "helper"
    helper.touch()
    settings = Settings(
        apple_helper=helper,
        sources={"calendar": {"poll_seconds": 600}, "reminders": {"poll_seconds": 120}},
    )

    for source, interval in [("calendar", "600.0"), ("reminders", "120.0")]:
        collector = OrganizerCollector(settings, None, source)
        assert collector.helper_arguments()[-2:] == ["--poll-seconds", str(int(float(interval)))]
