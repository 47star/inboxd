from pathlib import Path

from inboxd.domain.records import Message
from inboxd.storage.archive import Store


def record(identity: str, text: str, **fields) -> Message:
    return Message(
        id=identity,
        source="test",
        account="fixture",
        conversation_id="test:conversation",
        source_id=identity,
        timestamp="2026-01-01T00:00:00+00:00",
        text=text,
        **fields,
    )


def test_recovery_between_original_commit_and_index_commit(tmp_path: Path):
    store = Store(tmp_path)
    message = record("one", "DB생명 키 적재 오류")
    assert store.ingest(message)
    assert not store.ingest(message)
    store.close()

    recovered = Store(tmp_path)

    try:
        assert recovered.index_pending() == 1
        result = recovered.search("DB생명 적재")
        assert [item["id"] for item in result["items"]] == ["one"]
        assert recovered.changes()["cursor"] == 1
    finally:
        recovered.close()


def test_updates_and_deletes_replace_search_results(tmp_path: Path):
    store = Store(tmp_path)

    try:
        original = record("one", "기존 장애 분석")
        store.ingest(original)
        store.index_pending()
        store.ingest(original.model_copy(update={"text": "수정 완료"}))
        store.index_pending()
        assert store.search("기존")["total"] == 0
        assert store.search("수정")["total"] == 1
        store.ingest(original.model_copy(update={"deleted": True}))
        store.index_pending()
        assert store.search("수정")["total"] == 0
        events = store.changes()["items"]
        assert [item["operation"] for item in events] == ["create", "update", "delete"]
    finally:
        store.close()


def test_conversation_cursor_handles_equal_timestamps(tmp_path: Path):
    store = Store(tmp_path)

    try:
        for identity in ["one", "two", "three"]:
            store.ingest(record(identity, "동일 시각"))

        first = store.conversation("test:conversation", limit=2)
        second = store.conversation("test:conversation", first["next_cursor"], limit=2)
        identities = [item["id"] for item in first["items"] + second["items"]]
        assert len(set(identities)) == 3
        assert second["next_cursor"] is None
    finally:
        store.close()


def test_search_filters_and_substring_matching(tmp_path: Path):
    store = Store(tmp_path)

    try:
        store.ingest(record("one", "PostgreSQL 형변환 오류", sender="alice"))
        store.ingest(record("two", "PostgreSQL 형변환 검증", sender="bob"))
        store.index_pending()
        result = store.search("형변환", sender="alice")
        assert [item["id"] for item in result["items"]] == ["one"]
        assert store.search("없는문구")["total"] == 0
    finally:
        store.close()
