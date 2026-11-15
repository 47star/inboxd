import asyncio
import sqlite3
from contextlib import suppress

import pytest
from watchfiles import Change

from inboxd.runtime.config import Settings
from inboxd.sources.messages import MessagesCollector
from inboxd.storage.archive import Store


@pytest.fixture
def messages(tmp_path):
    path = tmp_path / "chat.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE message (guid TEXT, text TEXT, date INTEGER);"
        "CREATE TABLE chat (guid TEXT);"
        "CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);"
        "CREATE TABLE handle (id TEXT);"
        "CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);"
        "CREATE TABLE attachment (guid TEXT);"
        "CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);"
    )
    settings = Settings(
        data_dir=tmp_path / "archive",
        messages_path=path,
        sources={"messages": {"enabled": True, "poll_seconds": 3, "reconcile_seconds": 300}},
    )
    store = Store(settings.data_dir)
    collector = MessagesCollector(settings, store)

    try:
        yield connection, collector, store
    finally:
        connection.close()
        store.close()


def insert_message(connection, identity, text):
    connection.execute(
        "INSERT INTO message (guid, text, date) VALUES (?, ?, 800000000)", (identity, text)
    )
    connection.commit()


def test_incremental_scan_and_full_reconciliation(messages, monkeypatch):
    connection, collector, store = messages
    insert_message(connection, "first", "Original body")
    collector.scan()
    connection.execute("UPDATE message SET text = 'Edited body' WHERE guid = 'first'")
    insert_message(connection, "second", "New arrival")
    visited = []
    normalize = collector.normalize

    def track_normalize(database, row):
        visited.append(row["guid"])

        return normalize(database, row)

    monkeypatch.setattr(collector, "normalize", track_normalize)
    collector.scan(False)
    assert visited == ["second"]
    assert collector.checkpoint("last_rowid") == 2
    store.index_pending()
    assert store.search("New arrival")["total"] == 1
    assert store.search("Edited body")["total"] == 0
    collector.scan(False)
    assert visited == ["second"]
    assert store.get("status:messages")["scanned"] == 0
    collector.scan()
    store.index_pending()
    assert store.search("Edited body")["total"] == 1
    assert store.search("Original body")["total"] == 0
    assert store.get("status:messages")["last_reconcile_success"]


def test_failed_incremental_scan_preserves_cursor(messages, monkeypatch):
    connection, collector, store = messages
    insert_message(connection, "first", "Original body")
    collector.scan()
    insert_message(connection, "second", "Second arrival")
    insert_message(connection, "third", "Third arrival")
    emit = collector.emit

    def fail_third(record, origin):
        if record.source_id == "third":
            raise RuntimeError("Simulated persistence failure")

        return emit(record, origin)

    monkeypatch.setattr(collector, "emit", fail_third)

    with pytest.raises(RuntimeError, match="Simulated persistence failure"):
        collector.scan(False)

    assert collector.checkpoint("last_rowid") == 1
    monkeypatch.setattr(collector, "emit", emit)
    collector.scan(False)
    assert collector.checkpoint("last_rowid") == 3
    store.index_pending()
    assert store.search("arrival")["total"] == 2
    assert len(store.changes()["items"]) == 3


def test_full_scan_recovers_lower_rowids(messages):
    connection, collector, store = messages
    insert_message(connection, "first", "Old record")
    insert_message(connection, "second", "Another old record")
    collector.scan()
    assert collector.checkpoint("last_rowid") == 2
    connection.execute("DELETE FROM message")
    insert_message(connection, "replacement", "Replacement record")
    collector.scan()
    assert collector.checkpoint("last_rowid") == 1
    insert_message(connection, "next", "Next arrival")
    collector.scan(False)
    store.index_pending()
    assert store.search("Replacement record")["total"] == 1
    assert store.search("Next arrival")["total"] == 1


async def wait_until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.mark.parametrize("unrelated_events", [False, True])
def test_polling_collects_without_relevant_events(messages, monkeypatch, unrelated_events):
    connection, collector, store = messages
    collector.settings.sources["messages"]["poll_seconds"] = 0.01
    collector.settings.sources["messages"]["reconcile_seconds"] = 0.15
    watcher_closed = []

    async def watch(*args, **kwargs):
        try:
            while True:
                if unrelated_events:
                    await asyncio.sleep(0.001)
                    yield {
                        (Change.modified, str(collector.settings.messages_path.parent / "other"))
                    }
                else:
                    await asyncio.Future()
        finally:
            watcher_closed.append(True)

    monkeypatch.setattr("inboxd.sources.messages.awatch", watch)

    async def exercise():
        task = asyncio.create_task(collector.run())

        try:
            await wait_until(lambda: collector.checkpoint("backfilled", False))
            insert_message(connection, "first", "Unsignalled arrival")
            await wait_until(lambda: collector.checkpoint("last_rowid", 0) == 1)
            events = store.changes()["items"]
            assert events[0]["origin"] == "live"
            store.index_pending()
            assert store.search("Unsignalled arrival")["total"] == 1
            connection.execute("UPDATE message SET text = 'Reconciled edit'")
            connection.commit()
            await wait_until(lambda: len(store.changes()["items"]) == 2)
            store.index_pending()
            assert store.search("Reconciled edit")["total"] == 1
        finally:
            task.cancel()

            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(exercise())
    assert watcher_closed == [True]


def test_file_event_still_reconciles_existing_rows(messages, monkeypatch):
    connection, collector, store = messages
    insert_message(connection, "first", "Original body")

    async def exercise():
        changes = asyncio.Queue()

        async def watch(*args, **kwargs):
            while True:
                yield await changes.get()

        monkeypatch.setattr("inboxd.sources.messages.awatch", watch)
        task = asyncio.create_task(collector.run())

        try:
            await wait_until(lambda: collector.checkpoint("backfilled", False))
            connection.execute("UPDATE message SET text = 'Event edit'")
            connection.commit()
            changes.put_nowait({(Change.modified, str(collector.settings.messages_path) + "-wal")})
            await wait_until(lambda: len(store.changes()["items"]) == 2)
            store.index_pending()
            assert store.search("Event edit")["total"] == 1
        finally:
            task.cancel()

            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(exercise())
