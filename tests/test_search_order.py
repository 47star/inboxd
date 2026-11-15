import pytest

from inboxd.domain.records import Record
from inboxd.storage.archive import Store


def item(identity, text, timestamp, source="messages"):
    return Record(
        id=identity,
        source=source,
        source_id=identity,
        account="fixture",
        conversation_id=source + ":conversation",
        text=text,
        timestamp=timestamp,
    )


def test_latest_multilingual_search_dates_and_scope(tmp_path):
    store = Store(tmp_path)

    try:
        records = [
            item("old", "Google verification code", "2025-01-01T00:00:00Z"),
            item("latest", "구글 인증 문자", "2026-11-03T16:03:39.339792+09:00"),
            item("unrelated", "다른 인증 문자", "2026-11-04T16:00:00+09:00"),
            item("mail", "Google verification code", "2026-11-05T00:00:00Z", source="mail"),
        ]

        for record in records:
            store.ingest(record)

        store.index_pending()

        result = store.search(
            "Google 구글", source="messages", order="newest", match="any", limit=1
        )
        assert result["total"] == 2
        assert result["items"][0]["id"] == "latest"
        assert "score" not in result["items"][0]
        assert (
            store.search("Google 구글", source="messages", order="oldest", match="any", limit=1)[
                "items"
            ][0]["id"]
            == "old"
        )
        exact = store.search(
            '"구글 인증"',
            source="messages",
            order="newest",
            before="2026-11-03T07:03:39.339792Z",
            after="2026-11-03T07:03:39.339792Z",
        )
        assert exact["total"] == 1
        assert store.search("Google 구글", source="messages", match="all")["total"] == 0

        with pytest.raises(ValueError):
            store.search("Google", before="2026-11-03")

        with pytest.raises(ValueError):
            store.search("Google", before="2025-01-01T00:00:00Z", after="2026-01-01T00:00:00Z")

        store.delete("meta:search_schema")
    finally:
        store.close()

    recovered = Store(tmp_path)

    try:
        assert recovered.index_pending() == 4
        assert (
            recovered.search(
                "Google 구글", source="messages", order="newest", match="any", limit=1
            )["items"][0]["id"]
            == "latest"
        )
    finally:
        recovered.close()
