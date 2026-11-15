import pytest

from inboxd.sources.slack.records import entity_record, message_record
from inboxd.storage.archive import Store
from inboxd.storage.slack import SlackQueries


def message(channel: str, ts: str, **values):
    return message_record(
        "TEAM", "ME", channel, {"ts": ts, "text": "thread content", "user": "ALICE", **values}
    )


def test_thread_scope_pagination_edits_and_recovery(tmp_path):
    store = Store(tmp_path)

    try:
        root = message("CHAN", "100.000001", reply_count=2)
        reply = message("CHAN", "101.000001", thread_ts="100.000001")
        store.ingest(root)
        store.ingest(reply)
        store.ingest(message("CHAN", "102.000001", thread_ts="100.000001"))
        store.ingest(message("OTHER", "101.000001", thread_ts="100.000001"))
        store.ingest(message("CHAN", "103.000001"))
        queries = SlackQueries(store)
        first = queries.thread("slack:TEAM:CHAN", "100.000001", limit=2)
        second = queries.thread("slack:TEAM:CHAN", "100.000001", first["next_cursor"], limit=2)
        assert [item["source_id"] for item in first["items"] + second["items"]] == [
            "100.000001",
            "101.000001",
            "102.000001",
        ]
        assert first["reported_reply_count"] == 2
        assert second["next_cursor"] is None

        with pytest.raises(ValueError):
            queries.thread("slack:TEAM:OTHER", "100.000001", first["next_cursor"])

        store.ingest(reply.model_copy(update={"thread_id": "103.000001"}))
        store.ingest(root.model_copy(update={"deleted": True}))
        result = queries.thread("slack:TEAM:CHAN", "100.000001")
        assert len(result["items"]) == 2
        assert result["items"][0]["deleted"]
        assert result["items"][0]["text"] == ""

        recent = store.recent("slack", conversation_id="slack:TEAM:CHAN", limit=2)
        assert [item["source_id"] for item in recent["items"]] == ["103.000001", "102.000001"]
        assert store.recent("mail", conversation_id="slack:TEAM:CHAN")["items"] == []
        store.delete("meta:conversation_indexes")

        for key, _ in store.scan("slack-thread:"):
            store.delete(key)
    finally:
        store.close()

    recovered = Store(tmp_path)

    try:
        result = SlackQueries(recovered).thread("slack:TEAM:CHAN", "103.000001")
        assert {item["source_id"] for item in result["items"]} == {"101.000001", "103.000001"}
    finally:
        recovered.close()


def test_find_channels_and_dms_by_name_and_membership(tmp_path):
    store = Store(tmp_path)

    try:
        store.ingest(
            entity_record(
                "TEAM",
                "user",
                "ALICE",
                {
                    "name": "alice",
                    "profile": {"real_name": "김개발", "email": "alice@example.test"},
                },
            )
        )

        for identity, raw in (
            ("C1", {"name": "개발팀", "is_channel": True}),
            ("D1", {"is_im": True, "user": "ALICE"}),
            ("G1", {"is_mpim": True, "member_ids": ["ALICE", "ME"]}),
            ("C2", {"name": "개발팀-과거", "is_archived": True}),
        ):
            store.ingest(entity_record("TEAM", "channel", identity, raw))

        queries = SlackQueries(store)
        assert [item["channel_id"] for item in queries.conversations("#개발팀")["items"]] == ["C1"]
        assert len(queries.conversations("개발팀", include_archived=True)["items"]) == 2
        dm = queries.conversations("김개발", kind="dm")["items"]
        assert len(dm) == 1
        assert dm[0]["conversation_id"] == "slack:TEAM:D1"
        assert not dm[0]["backfill"]["complete"]
        first = queries.conversations(user_id="ALICE", limit=1)
        second = queries.conversations(user_id="ALICE", cursor=first["next_cursor"], limit=1)
        assert {item["channel_id"] for item in first["items"] + second["items"]} == {"D1", "G1"}

        with pytest.raises(ValueError):
            queries.conversations(user_id="ME", cursor=first["next_cursor"])
    finally:
        store.close()
