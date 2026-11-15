import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from slack_sdk.errors import SlackApiError

from inboxd.api.query_context import query_context
from inboxd.runtime.config import Settings
from inboxd.sources.slack.backfill import SlackBackfill
from inboxd.sources.slack.collector import SlackCollector
from inboxd.storage.archive import Store


@pytest.fixture
def backfill(tmp_path):
    store = Store(tmp_path / "archive")
    owner = SlackCollector(Settings(data_dir=tmp_path), store)
    owner.team = "team"
    owner.user = "user"
    owner.api = Mock(rate_limits=0)
    owner.message = Mock()
    value = SlackBackfill(owner)
    owner.backfill = value

    try:
        yield value
    finally:
        store.close()


def test_weekly_deadline_uses_per_channel_success(backfill):
    owner = backfill.collector
    current = time.time()
    owner.advance("last_full_reconcile", current - 2 * 86400)
    owner.advance("team:ready", {"complete": True})
    owner.advance("team:due", {"complete": True, "last_full_reconcile": current - 8 * 86400})
    assert not backfill.full_due("ready")
    assert backfill.full_due("due")
    assert backfill.full_due("new")
    assert owner.settings.reconcile_seconds == 300
    assert owner.settings.source_value("slack", "reconcile_seconds") == 3600


def test_recovery_preserves_incomplete_history_and_watermark(backfill):
    owner = backfill.collector
    owner.advance("team:channel", {"complete": False, "latest": "100"})
    parameters = []

    async def pages(method, **kwargs):
        parameters.append((method, kwargs))
        yield {"messages": [{"ts": "950", "text": "recovered"}]}

    owner.api.pages = pages
    backfill.request_recovery(900)
    asyncio.run(backfill.recover([{"id": "channel"}]))
    assert parameters[0][1]["oldest"] == "600"
    checkpoint = owner.checkpoint("team:channel")
    assert not checkpoint["complete"]
    assert checkpoint["latest"] == "100"
    assert owner.checkpoint("recovery_since") is None
    assert owner.message.call_args.args[2] == "reconcile"


def test_failed_channel_does_not_repeat_successful_full_scan(backfill):
    owner = backfill.collector
    calls = []

    async def pages(method, **kwargs):
        calls.append((kwargs["channel"], kwargs["oldest"]))

        if kwargs["channel"] == "failed":
            raise SlackApiError("unavailable", {"error": "channel_not_found"})

        yield {"messages": [{"ts": "1000"}]}

    owner.api.pages = pages
    backfill.channel_details = AsyncMock()
    channels = [{"id": "ready"}, {"id": "failed"}]
    started = time.time()
    asyncio.run(backfill.cycle(channels))
    assert owner.checkpoint("next_reconcile") >= started + 3600
    assert not backfill.full_due("ready")
    assert backfill.full_due("failed")
    asyncio.run(backfill.cycle(channels))
    assert calls == [("ready", "0"), ("failed", "0"), ("ready", "700.0"), ("failed", "0")]
    assert owner.store.get("status:slack")["state"] == "partial"


def test_recovery_failure_keeps_earliest_gap(backfill):
    backfill.history = AsyncMock(side_effect=SlackApiError("offline", {"error": "fatal_error"}))
    backfill.request_recovery(1000)
    backfill.request_recovery(2000)
    errors = asyncio.run(backfill.recover([{"id": "channel"}]))
    assert errors
    assert backfill.collector.checkpoint("recovery_since") == 1000
    assert not backfill.recovery_requested.is_set()
    assert backfill.collector.store.get("status:slack")["recovery_pending"]


@pytest.mark.parametrize("operation", ["recover", "cycle"])
def test_unavailable_slackbot_does_not_repeat_workspace_recovery(backfill, operation):
    owner = backfill.collector
    unavailable = True

    async def pages(method, **kwargs):
        if kwargs["channel"] == "bot" and unavailable:
            raise SlackApiError("unavailable", {"error": "channel_not_found"})

        yield {"messages": [{"ts": "1000"}]}

    owner.api.pages = pages
    backfill.channel_details = AsyncMock()
    channels = [{"id": "ready"}, {"id": "bot", "is_im": True, "user": "USLACKBOT"}]
    owner.advance("team:bot", {"complete": True, "latest": "100"})
    backfill.request_recovery(900)
    asyncio.run(getattr(backfill, operation)(channels))
    status = owner.store.get("status:slack")
    assert status["state"] == "watching"
    assert not status["recovery_pending"]
    assert owner.checkpoint("recovery_since") is None
    assert not owner.checkpoint("team:bot")["complete"]
    assert status["unavailable_channels"] == [{"channel": "bot", "error": "channel_not_found"}]
    context = query_context(owner.store, "slack", set(), False)["sources"][0]
    assert "some_conversations_unavailable" in context["issues"]
    assert context["unavailable_channels"] == status["unavailable_channels"]
    unavailable = False
    asyncio.run(backfill.cycle(channels))
    assert owner.store.get("status:slack")["unavailable_channels"] == []
    assert owner.checkpoint("team:bot")["complete"]
    assert owner.checkpoint("team:bot")["latest"] == "1000.0"


@pytest.mark.parametrize(
    "channel,error",
    [
        ({"id": "target", "is_im": True, "user": "ordinary"}, "channel_not_found"),
        ({"id": "target", "is_im": True, "user": "USLACKBOT"}, "fatal_error"),
    ],
)
def test_other_channel_failures_still_require_recovery(backfill, channel, error):
    async def pages(method, **kwargs):
        raise SlackApiError("unavailable", {"error": error})
        yield

    backfill.collector.api.pages = pages
    backfill.request_recovery(900)
    asyncio.run(backfill.recover([channel]))
    assert backfill.collector.checkpoint("recovery_since") == 900
    assert backfill.collector.store.get("status:slack")["state"] == "partial"


def test_recovery_precedes_full_history(backfill):
    order = []

    async def history(channel, full, since=None):
        order.append((channel, full, since))

    backfill.history = history
    backfill.channel_details = AsyncMock()
    backfill.request_recovery(1000)
    asyncio.run(backfill.cycle([{"id": "one"}, {"id": "two"}]))
    assert order == [
        ("one", False, 1000),
        ("two", False, 1000),
        ("one", True, None),
        ("two", True, None),
    ]


def test_reconnect_wakes_wait_without_running_scheduled_cycle(backfill):
    async def exercise():
        owner = backfill.collector
        owner.advance("next_reconcile", time.time() + 3600)
        backfill.directory = AsyncMock(return_value=[{"id": "channel"}])
        backfill.cycle = AsyncMock()
        recovered = asyncio.Event()

        async def recover(channels):
            backfill.recovery_requested.clear()
            recovered.set()

        backfill.recover = recover
        task = asyncio.create_task(backfill.run())

        try:
            await asyncio.sleep(0)
            assert not recovered.is_set()
            backfill.request_recovery(1000)
            await asyncio.wait_for(recovered.wait(), timeout=1)
            backfill.cycle.assert_not_awaited()
        finally:
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(exercise())


@pytest.mark.parametrize("disconnect", [True, False])
def test_socket_disconnect_or_session_replacement_triggers_recovery(
    backfill, monkeypatch, disconnect
):
    owner = backfill.collector
    socket = SimpleNamespace(current_session=object(), is_connected=AsyncMock(return_value=True))
    cycles = 0

    async def advance(seconds):
        nonlocal cycles
        cycles += 1

        if cycles == 1 and disconnect:
            socket.is_connected.return_value = False
        elif cycles <= 2:
            socket.is_connected.return_value = True
            socket.current_session = object()
        else:
            raise asyncio.CancelledError

    monkeypatch.setattr("inboxd.sources.slack.collector.asyncio.sleep", advance)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(owner.connection_health(socket))

    assert backfill.recovery_requested.is_set()
    assert owner.checkpoint("recovery_since") is not None
