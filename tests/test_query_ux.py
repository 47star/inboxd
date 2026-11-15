from fastapi.testclient import TestClient

from inboxd.api.http import create_app, token_for
from inboxd.domain.records import Record
from inboxd.runtime.config import Settings
from inboxd.sources.slack.records import entity_record, message_record


def test_previews_body_continuation_and_collection_context(tmp_path):
    settings = Settings(
        data_dir=tmp_path,
        sources={
            source: {"enabled": False}
            for source in ("messages", "mail", "slack", "calendar", "notes", "reminders")
        },
    )
    app = create_app(settings)

    with TestClient(app) as client:
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)
        store = app.state.store
        body = "가" * 2000 + "찾는단어" + "나" * 2000
        record = message_record(
            "TEAM", "ME", "CHAN", {"ts": "100.000001", "user": "USER", "text": body}
        )
        store.ingest(record)
        store.ingest(entity_record("TEAM", "channel", "CHAN", {"name": "개발팀"}))
        store.ingest(
            entity_record("TEAM", "user", "USER", {"profile": {"display_name": "찾는단어"}})
        )
        store.set("status:slack", {"state": "backfilling", "backfill_running": True})
        store.index_pending()
        page = client.get("/search", params={"source": "slack", "q": "찾는단어"}).json()
        assert page["total"] == 1
        item = page["items"][0]
        assert "찾는단어" in item["text_preview"]
        assert item["text_truncated"]
        assert item["text_length"] == len(body)
        assert item["conversation_name"] == "개발팀"
        assert item["thread_id"] == "100.000001"
        assert "raw" not in item
        assert "text" not in item
        assert "backfill_in_progress" in page["context"]["sources"][0]["issues"]

        request = {**item["read_request"]["arguments"], "length": 700}
        segments = []

        while request:
            detail = client.get("/message", params=request).json()
            segments.append(detail["text"])
            assert "raw" not in detail
            request = detail["next_request"]["arguments"] if detail["next_request"] else None

        assert "".join(segments) == body

        empty = client.get("/search", params={"source": "mail", "q": "없는단어"}).json()
        assert empty["context"]["result_state"] == "no_local_matches"
        assert (
            "local_files_only_not_full_server_mailbox" in empty["context"]["sources"][0]["issues"]
        )

        store.set("attachment:fixture", {"state": "unavailable", "text": ""})
        attachment = client.get("/attachment-text", params={"id": "fixture"}).json()
        assert not attachment["text_available"]
        assert attachment["empty_reason"] == "content_not_archived"


def test_recent_next_request_retains_scope_and_mail_sender(tmp_path):
    settings = Settings(
        data_dir=tmp_path,
        sources={
            source: {"enabled": False}
            for source in ("messages", "mail", "slack", "calendar", "notes", "reminders")
        },
    )
    app = create_app(settings)

    with TestClient(app) as client:
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)

        for identity in ("a", "b"):
            app.state.store.ingest(
                Record(
                    id=identity,
                    source="mail",
                    account="fixture",
                    source_id=identity,
                    conversation_id="mail:thread",
                    timestamp="2026-01-01T00:00:00Z",
                    sender="author@example.test",
                    text="body",
                    raw={"headers": [["From", "Mail Author <author@example.test>"]]},
                )
            )

        first = client.get(
            "/recent", params={"source": "mail", "conversation_id": "mail:thread", "limit": 1}
        ).json()
        assert first["items"][0]["sender_name"] == "Mail Author"
        assert first["has_more"]
        args = {
            key: value
            for key, value in first["next_request"]["arguments"].items()
            if value is not None
        }
        second = client.get("/recent", params=args).json()
        assert not second["has_more"]
        assert first["items"][0]["id"] != second["items"][0]["id"]
