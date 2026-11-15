from fastapi.testclient import TestClient

from inboxd.api.http import create_app, token_for
from inboxd.runtime.config import Settings
from inboxd.sources.slack.records import entity_record, message_record


def test_sender_names_on_all_message_views(tmp_path):
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
        user = entity_record(
            "TEAM", "user", "USER", {"profile": {"display_name": "표시 이름", "real_name": "실명"}}
        )
        record = message_record(
            "TEAM", "ME", "CHAN", {"ts": "100.000001", "user": "USER", "text": "테스트 메시지"}
        )
        store.ingest(user)
        store.ingest(record)
        store.index_pending()

        requests = [
            ("/message", {"id": record.id}),
            ("/search", {"source": "slack", "q": "테스트"}),
            ("/recent", {"source": "slack"}),
            ("/conversation", {"id": record.conversation_id}),
            (
                "/slack/thread",
                {"conversation_id": record.conversation_id, "thread_id": record.source_id},
            ),
            ("/changes", {"source": "slack", "cursor": 1}),
        ]

        for path, params in requests:
            response = client.get(path, params=params)
            assert response.status_code == 200
            data = response.json()
            item = data if path == "/message" else data["items"][0]
            item = item["message"] if path == "/changes" else item
            assert item["sender"] == "USER"
            assert item["sender_name"] == "표시 이름"
            assert "sender_profile" not in item

        store.ingest(user.model_copy(update={"raw": {"profile": {"real_name": "새 이름"}}}))
        assert client.get("/message", params={"id": record.id}).json()["sender_name"] == "새 이름"

        for sender, extra, expected in (
            ("BOT", {"bot_profile": {"name": "알림 봇"}}, "알림 봇"),
            ("UNKNOWN", {}, None),
        ):
            item = message_record(
                "TEAM",
                "ME",
                "CHAN",
                {"ts": "200.000001", "bot_id": sender, "text": "알림", **extra},
            )
            store.ingest(item)
            result = client.get("/recent", params={"source": "slack", "limit": 1}).json()["items"][
                0
            ]
            assert result["sender"] == sender
            assert result["sender_name"] == expected
