import asyncio
import xml.etree.ElementTree as ET

import httpx
import pytest
from fastapi.testclient import TestClient

from inboxd.api.http import create_app, token_for
from inboxd.runtime.config import Settings
from inboxd.sources.redmine.client import RedmineReader
from inboxd.sources.redmine.collector import RedmineCollector
from inboxd.sources.redmine.records import api_item, feed_items
from inboxd.sources.redmine.synchronization import IssueSynchronizer
from inboxd.storage.archive import Store

BASE = "https://redmine.example.test/redmine"
DATE = "Thu, 05 Nov 2026 00:00:00 GMT"
UPDATED = "2026-11-04T09:00:00Z"


def issue(identity, **updates):
    return {
        "id": identity,
        "subject": f"일감 {identity}",
        "description": "본문",
        "created_on": "2025-01-01T00:00:00Z",
        "updated_on": UPDATED,
        "project": {"id": 1, "name": "개발팀"},
        "tracker": {"id": 1, "name": "결함"},
        "status": {"id": 1, "name": "진행"},
        "author": {"id": 77, "name": "작성자"},
        "assigned_to": {"id": 78, "name": "담당자"},
        **updates,
    }


def feed(issues):
    root = ET.Element("feed", xmlns="http://www.w3.org/2005/Atom")

    for row in issues:
        entry = ET.SubElement(root, "entry")
        ET.SubElement(
            entry, "title"
        ).text = f"개발팀 - 결함 #{row['id']} ({row['status']['name']}): {row['subject']}"
        ET.SubElement(entry, "link", rel="alternate", href=f"{BASE}/issues/{row['id']}")
        ET.SubElement(entry, "updated").text = row["updated_on"]
        author = ET.SubElement(entry, "author")
        ET.SubElement(author, "name").text = "작성자"
        ET.SubElement(entry, "content", type="html").text = f"<p>{row['description']}</p>"

    return ET.tostring(root)


def settings(path):
    return Settings(
        data_dir=path,
        sources={
            "redmine": {"enabled": True, "url": BASE},
            "messages": {"enabled": False},
            "mail": {"enabled": False},
            "slack": {"enabled": False},
            "calendar": {"enabled": False},
            "notes": {"enabled": False},
            "reminders": {"enabled": False},
        },
    )


def test_api_and_feed_share_identity_and_full_api_refresh(tmp_path):
    store = Store(tmp_path)

    try:
        collector = RedmineCollector(settings(tmp_path), store)
        assert collector.apply(api_item(issue(1), BASE), "backfill")
        update = issue(1, subject="수정된 일감", description="변경 본문", updated_on=UPDATED)
        signal = feed_items(feed([update]), BASE)[0]
        item = api_item(update, BASE)
        assert (
            signal.record("redmine", collector.account).id
            == item.record("redmine", collector.account).id
        )
        assert collector.apply(item, "rss")
        assert not collector.apply(item, "rss")
        record = store.get("message:" + item.record("redmine", collector.account).id)
        assert record["subject"] == "수정된 일감"
        assert record["text"] == "변경 본문"
        fields = record["metadata"]["work_item"]
        assert fields["created_at"] == "2025-01-01T00:00:00.000000+00:00"
        assert fields["fields"]["assigned_to"]["name"] == "담당자"
        assert fields["description_format"] == "redmine_markup"
        store.index_pending()
        assert store.search("변경", source="redmine")["total"] == 1
        assert len(store.recent(source="redmine")["items"]) == 1
    finally:
        store.close()


def test_restart_resumes_backfill_then_only_signaled_issue_api(tmp_path):
    calls = []
    fail_page = True
    records = [issue(identity) for identity in range(1, 5)]

    def respond(request):
        nonlocal fail_page
        params = request.url.params
        path = request.url.path
        calls.append((path, params))

        if path.endswith(".json"):
            assert request.headers["X-Redmine-API-Key"] == "api-key"

            if "/issues/" in path:
                identity = int(path.rsplit("/", 1)[1].removesuffix(".json"))

                return httpx.Response(200, json={"issue": records[identity - 1]})

            if params.get("sort") == "id:desc":
                return httpx.Response(200, json={"issues": [records[-1]]})

            lower, upper = map(int, params.get_list("v[issue_id][]"))

            if lower == 3 and fail_page:
                fail_page = False

                return httpx.Response(503)

            selected = [row for row in records if lower <= row["id"] <= upper][:2]

            return httpx.Response(200, json={"issues": selected})

        assert params["key"] == "feed-key"
        upper = int(params.get("v[issue_id][]", "999"))
        selected = [row for row in reversed(records) if row["id"] <= upper][:2]

        return httpx.Response(200, content=feed(selected), headers={"Date": DATE})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            reader = RedmineReader(client, BASE, "api-key", "feed-key")
            store = Store(tmp_path)

            try:
                collector = RedmineCollector(settings(tmp_path), store)

                with pytest.raises(RuntimeError, match="HTTP 503"):
                    await collector.backfill(reader)

                assert collector.state()["cursor_id"] == 2
                assert not collector.state().get("backfill_complete")
            finally:
                store.close()

            store = Store(tmp_path)

            try:
                collector = RedmineCollector(settings(tmp_path), store)
                await collector.backfill(reader)
                assert collector.state()["backfill_complete"]
                assert collector.state()["backfilled"] == 4
            finally:
                store.close()

            store = Store(tmp_path)

            try:
                calls.clear()
                collector = RedmineCollector(settings(tmp_path), store)
                await collector.backfill(reader)
                await collector.poll(reader)
                assert not any(path.endswith("/issues.json") for path, _ in calls)
                assert len([path for path, _ in calls if path.endswith(".atom")]) == 2
                assert len([path for path, _ in calls if path.endswith(".json")]) == 4
                assert store.get("status:redmine")["feed_received"] == 4
                sequence = store.sequence
                await collector.poll(reader)
                assert store.sequence == sequence
            finally:
                store.close()

    asyncio.run(run())


def test_failed_or_unfiltered_feed_does_not_advance_checkpoint(tmp_path):
    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)
            collector.save(backfill_complete=True, feed_since="2026-01-01T00:00:00+00:00")

            def respond(request):
                if request.url.path.endswith(".json"):
                    return httpx.Response(200, json={"issue": issue(2)})

                return httpx.Response(200, content=feed([issue(2)]), headers={"Date": DATE})

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                reader = RedmineReader(client, BASE, "api-key", "feed-key")

                with pytest.raises(ValueError, match="ignored the ID range"):
                    await collector.poll(reader)

            assert collector.state()["feed_since"] == "2026-01-01T00:00:00+00:00"
        finally:
            store.close()

    asyncio.run(run())


def test_feed_authentication_errors_do_not_expose_keys():
    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(302))
        ) as client:
            reader = RedmineReader(client, BASE, "private-api", "private-feed")

            with pytest.raises(RuntimeError, match="HTTP 302") as error:
                await reader.feed("2026-01-01")

            assert "private" not in str(error.value)

    asyncio.run(run())


def test_offline_window_and_partial_feed_failure_are_replayed(tmp_path):
    calls = []
    interrupted = True

    def respond(request):
        nonlocal interrupted
        params = request.url.params
        calls.append(request.url.path)

        if request.url.path.endswith(".json"):
            return httpx.Response(200, json={"issue": issue(8)})

        assert params["v[updated_on][]"] == "2026-09-29"

        if "v[issue_id][]" not in params:
            return httpx.Response(200, content=feed([issue(8)]), headers={"Date": DATE})

        if interrupted:
            interrupted = False

            return httpx.Response(503)

        return httpx.Response(200, content=feed([]), headers={"Date": DATE})

    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)
            collector.save(backfill_complete=True, feed_since="2026-10-01T00:00:00+00:00")

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                reader = RedmineReader(client, BASE, "api-key", "feed-key")

                with pytest.raises(RuntimeError, match="HTTP 503"):
                    await collector.poll(reader)

                sequence = store.sequence
                assert collector.state()["feed_since"] == "2026-10-01T00:00:00+00:00"
                await collector.backfill(reader)
                await collector.poll(reader)
                assert store.sequence == sequence
                assert collector.state()["feed_since"] == "2026-11-05T00:00:00+00:00"
                assert not any(path.endswith("/issues.json") for path in calls)
        finally:
            store.close()

    asyncio.run(run())


def test_feed_rejects_login_page_and_bad_entry():
    with pytest.raises(ValueError, match="not an Atom"):
        feed_items(b"<html><body>login</body></html>", BASE)

    content = feed([issue(1)]).replace(b"/issues/1", b"/projects/1")

    with pytest.raises(ValueError, match="unexpected issue link"):
        feed_items(content, BASE)


def test_work_items_are_exposed_through_existing_query_api(tmp_path):
    app = create_app(settings(tmp_path))

    with TestClient(app) as client:
        client.headers["Authorization"] = "Bearer " + token_for(tmp_path)
        collector = RedmineCollector(settings(tmp_path), app.state.store)
        collector.apply(api_item(issue(1), BASE), "backfill")
        app.state.store.index_pending()
        source = client.get("/sources", params={"source": "redmine"}).json()["items"][0]
        assert source["content_records"] == 1
        assert source["timestamp_meaning"] == "latest_issue_or_journal_timestamp"
        result = client.get("/search", params={"source": "redmine", "q": "일감"}).json()
        record = result["items"][0]
        assert record["sender_name"] == "작성자"
        assert record["work_item"]["status_name"] == "진행"
        assert record["work_item"]["url"] == BASE + "/issues/1"
        compact = client.get("/message", params={"id": record["id"], "length": 2}).json()
        assert "metadata" not in compact
        assert len(compact["text"]) == 2
        detail = client.get("/message", params=compact["metadata_request"]["arguments"]).json()
        assert detail["metadata"]["work_item"]["fields"]["assigned_to"]["id"] == 78


def test_same_timestamp_metadata_changes_and_relation_removal_refresh_both_ends(tmp_path):
    relation = {"id": 100, "issue_id": 1, "issue_to_id": 2, "relation_type": "blocks"}
    records = {
        1: issue(1, relations=[relation], watchers=[{"id": 7, "name": "참조자"}]),
        2: issue(2, relations=[relation]),
    }
    calls = []

    def respond(request):
        identity = int(request.url.path.rsplit("/", 1)[1].removesuffix(".json"))
        calls.append(identity)
        assert request.url.params["include"] == (
            "children,attachments,relations,changesets,journals,watchers"
        )

        return httpx.Response(200, json={"issue": records[identity]})

    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                sync = IssueSynchronizer(collector, RedmineReader(client, BASE, "api", "feed"))
                assert await sync.refresh([1], "rss", follow_relations=True) == 2
                assert calls == [1, 2]
                calls.clear()
                records[1]["relations"] = []
                records[1]["watchers"] = []
                records[1]["assigned_to"] = None
                records[1]["status"] = {"id": 5, "name": "종료"}
                records[1]["custom_fields"] = [{"id": 3, "value": "변경"}]
                records[1]["plugin_field"] = {"value": "확장 메타데이터"}
                records[2]["relations"] = []
                assert await sync.refresh([1], "rss", follow_relations=True) == 2
                assert calls == [1, 2]
                fields = store.get(sync.key(1))["metadata"]["work_item"]["fields"]
                assert fields["relations"] == []
                assert fields["watchers"] == []
                assert fields["assigned_to"] is None
                assert fields["status"]["name"] == "종료"
                assert fields["plugin_field"]["value"] == "확장 메타데이터"
                assert store.get(sync.key(2))["metadata"]["work_item"]["fields"]["relations"] == []
                assert await sync.refresh([1], "rss") == 0
        finally:
            store.close()

    asyncio.run(run())


def test_activity_journal_triggers_refresh_once_and_indexes_notes(tmp_path):
    row = issue(
        4,
        journals=[
            {
                "id": 99,
                "created_on": "2026-11-05T00:00:00Z",
                "notes": "관계 수정 내역",
                "details": [{"property": "relation", "name": "blocks", "old_value": "3"}],
            }
        ],
    )
    content = feed([row]).replace(b"/issues/4", b"/issues/4#change-99")
    calls = []

    def respond(request):
        calls.append(request.url.path)

        if request.url.path.endswith("activity.atom"):
            return httpx.Response(200, content=content)

        return httpx.Response(200, json={"issue": row})

    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                sync = IssueSynchronizer(collector, RedmineReader(client, BASE, "api", "feed"))
                assert await sync.activity() == 1
                assert await sync.activity() == 0
                assert sum(path.endswith(".json") for path in calls) == 1
                record = store.get(sync.key(4))
                assert record["timestamp"] == "2026-11-05T00:00:00.000000+00:00"
                assert "관계 수정 내역" in record["text"]
                store.index_pending()
                assert store.search("관계 수정", source="redmine")["total"] == 1
        finally:
            store.close()

    asyncio.run(run())


def test_unavailable_issue_is_not_deleted_and_existing_records_are_reconciled(tmp_path):
    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)
            collector.apply(api_item(issue(7), BASE), "backfill")

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(404))
            ) as client:
                sync = IssueSynchronizer(collector, RedmineReader(client, BASE, "api", "feed"))
                await sync.reconcile()
                assert not store.get(sync.key(7))["deleted"]
                access = store.get(f"redmine-access:{collector.account}:7")
                assert access["state"] == "unavailable"
                assert access["http_status"] == 404
                await sync.reconcile()
                assert collector.state()["reconcile_cursor"] is None
                assert collector.state()["metadata_cycle_completed_at"]
        finally:
            store.close()

    asyncio.run(run())


def test_metadata_enrichment_is_once_and_resumable(tmp_path):
    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)
            old = api_item(issue(5), BASE).record("redmine", collector.account)
            old.metadata["work_item"].pop("schema_version")
            store.ingest(old)
            collector.save(backfill_complete=True)
            calls = []

            def respond(request):
                calls.append(request.url.path)

                return httpx.Response(200, json={"issue": issue(5, watchers=[{"id": 7}])})

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                reader = RedmineReader(client, BASE, "api", "feed")
                sync = IssueSynchronizer(collector, reader)
                await sync.enrich_once()
                assert collector.state()["metadata_backfill_complete"]
                assert len(calls) == 1
                store.close()
                store = Store(tmp_path)
                restarted = RedmineCollector(settings(tmp_path), store)
                await restarted.backfill(reader)
                await IssueSynchronizer(restarted, reader).enrich_once()
                assert len(calls) == 1
        finally:
            store.close()

    asyncio.run(run())


def test_feed_creation_time_does_not_override_api_state(tmp_path):
    row = issue(9, updated_on="2026-11-04T10:00:00Z")
    signal = issue(9, updated_on="2025-01-01T00:00:00Z")

    def respond(request):
        if request.url.path.endswith(".json"):
            return httpx.Response(200, json={"issue": row})

        entries = [] if "v[issue_id][]" in request.url.params else [signal]

        return httpx.Response(200, content=feed(entries), headers={"Date": DATE})

    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)
            collector.apply(api_item(issue(9), BASE), "backfill")
            collector.save(backfill_complete=True, feed_since="2026-11-03T00:00:00+00:00")
            row["status"] = {"id": 5, "name": "종료"}

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                await collector.poll(RedmineReader(client, BASE, "api", "feed"))

            record = store.get(
                "message:" + api_item(row, BASE).record("redmine", collector.account).id
            )
            assert record["timestamp"] == "2026-11-04T10:00:00.000000+00:00"
            assert record["metadata"]["work_item"]["status_name"] == "종료"
        finally:
            store.close()

    asyncio.run(run())


def test_metadata_order_does_not_create_false_changes(tmp_path):
    store = Store(tmp_path)

    try:
        collector = RedmineCollector(settings(tmp_path), store)
        row = issue(3, watchers=[{"id": 2}, {"id": 1}], custom_fields=[{"id": 4}, {"id": 3}])
        assert collector.apply(api_item(row, BASE), "backfill")
        row["watchers"].reverse()
        row["custom_fields"].reverse()
        assert not collector.apply(api_item(row, BASE), "rss")
    finally:
        store.close()


def test_removed_relation_target_survives_failed_refresh(tmp_path):
    relation = {"id": 1, "issue_id": 1, "issue_to_id": 2, "relation_type": "relates"}
    fail = True
    calls = []

    def respond(request):
        nonlocal fail
        identity = int(request.url.path.rsplit("/", 1)[1].removesuffix(".json"))
        calls.append(identity)

        if identity == 2 and fail:
            fail = False

            return httpx.Response(503)

        return httpx.Response(200, json={"issue": issue(identity, relations=[])})

    async def run():
        store = Store(tmp_path)

        try:
            collector = RedmineCollector(settings(tmp_path), store)

            for identity in (1, 2):
                collector.apply(api_item(issue(identity, relations=[relation]), BASE), "backfill")

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                reader = RedmineReader(client, BASE, "api", "feed")
                sync = IssueSynchronizer(collector, reader)

                with pytest.raises(RuntimeError, match="HTTP 503"):
                    await sync.refresh([1], "rss", follow_relations=True)

                assert store.get(sync.key(1))["metadata"]["work_item"]["fields"]["relations"] == []
                assert store.scan(f"redmine-related:{collector.account}:")
                calls.clear()
                store.close()
                store = Store(tmp_path)
                restarted = RedmineCollector(settings(tmp_path), store)
                sync = IssueSynchronizer(restarted, reader)
                await sync.refresh([1], "rss", follow_relations=True)
                assert calls == [1, 2]
                assert store.get(sync.key(2))["metadata"]["work_item"]["fields"]["relations"] == []
                assert not store.scan(f"redmine-related:{collector.account}:")
        finally:
            store.close()

    asyncio.run(run())
