from inboxd.api.presentation import RecordPresenter
from inboxd.domain.records import Record
from inboxd.domain.search_content import readable_body, rendered_text
from inboxd.storage.archive import Store


def work_item(identity, description, notes="", title="일감"):
    return Record(
        id=identity,
        source="redmine",
        source_id=identity,
        conversation_id=identity,
        account="fixture",
        kind="work_item",
        subject=title,
        text=description + "\n\n" + notes,
        timestamp="2026-01-01T00:00:00Z",
        metadata={
            "work_item": {
                "description": description,
                "description_format": "redmine_markup",
                "fields": {"journals": [{"id": 1, "notes": notes}]},
            }
        },
    )


def test_rendered_phrases_originals_and_field_matches(tmp_path):
    store = Store(tmp_path)

    try:
        description = "<p><span>진행 </span><span>상황</span>&nbsp;확인</p>"
        notes = "<p>자체 <strong>SSL</strong> 인증서 keystore</p>"
        record = work_item("html", description, notes, "서버 TLS")
        store.ingest(record)
        store.index_pending()

        for query in ['"진행 상황"', '"자체 SSL 인증서"', "KEYSTORE", "서버 keystore"]:
            result = store.search(query)
            assert result["total"] == 1

        presenter = RecordPresenter(store)
        page = presenter.page(store.search('"자체 SSL 인증서"'), query='"자체 SSL 인증서"')
        preview = page["items"][0]
        assert preview["matched_fields"] == ["comments"]
        assert "자체 SSL 인증서" in preview["text_preview"]
        assert "<strong>" not in preview["text_preview"]
        detail = presenter.detail(record.model_dump(), 0, 5)
        assert "metadata" not in detail
        assert not detail["metadata_included"]
        assert detail["metadata_request"]["arguments"]["include_metadata"]
        parts = [detail["text"]]

        while detail["next_request"]:
            args = detail["next_request"]["arguments"]
            detail = presenter.detail(record.model_dump(), args["offset"], args["length"])
            parts.append(detail["text"])

        assert "".join(parts) == readable_body(record.model_dump())
        full = presenter.detail(record.model_dump(), 0, 1, include_metadata=True)
        assert full["metadata"]["work_item"]["description"] == description
        assert store.get("message:html")["text"] == record.text
    finally:
        store.close()


def test_phrase_positions_reject_scattered_grams_and_reversed_order(tmp_path):
    store = Store(tmp_path)

    try:
        store.ingest(work_item("exact", "keystore abcdef 진행 상황"))
        store.ingest(work_item("scattered", "key eys yst sto tor ore ab bc cd de ef 상황 진행"))
        store.ingest(work_item("reversed", "ef de cd bc ab"))
        store.index_pending()

        for query in ["keystore", "abcdef", '"진행 상황"']:
            assert [item["id"] for item in store.search(query)["items"]] == ["exact"]

        assert store.search("ab")["total"] == 3
        assert store.search('"서버 keystore"')["total"] == 0
    finally:
        store.close()


def test_title_priority_and_comments_independent_of_description_length(tmp_path):
    store = Store(tmp_path)

    try:
        store.ingest(work_item("title", "설명 " * 2000, title="keystore"))
        store.ingest(work_item("body", "keystore"))
        store.ingest(work_item("long", "설명 " * 2000, notes="keystore 인증서"))
        store.ingest(work_item("short", "설명", notes="keystore 인증서"))
        store.index_pending()
        rows = store.search("keystore")["items"]
        assert rows[0]["id"] == "title"
        scores = {row["id"]: row["score"] for row in rows}
        assert scores["long"] == scores["short"]
    finally:
        store.close()


def test_schema_upgrade_resumes_without_source_events(tmp_path):
    store = Store(tmp_path)
    record = work_item("html", "<span>진행 </span><span>상황</span>")
    store.ingest(record)
    store.index_pending()
    sequence = store.sequence
    store.set("meta:search_schema", 2)
    store.close()
    store = Store(tmp_path)
    assert store.get("pending:html") == "html"
    store.close()
    store = Store(tmp_path)

    try:
        assert store.index_pending() == 1
        assert store.search('"진행 상황"')["total"] == 1
        assert store.sequence == sequence
        assert store.get("message:html")["text"] == record.text
    finally:
        store.close()


def test_attachment_excerpt_and_literal_non_html_text(tmp_path):
    assert rendered_text("vector<T> &amp; <user>", "redmine_markup") == "vector<T> &amp; <user>"
    assert rendered_text("<p>앞</p>뒤", "html") == "앞\n뒤"
    assert rendered_text("<p>앞<script>secret</script>뒤</p>", "html") == "앞뒤"
    store = Store(tmp_path)

    try:
        record = work_item("attachment", "본문")
        store.ingest(record)
        store.set("extracted:attachment", "첨부 시작 " * 300 + "유일한 검색구문")
        store.index_pending()
        page = RecordPresenter(store).page(
            store.search('"유일한 검색구문"'), query='"유일한 검색구문"'
        )
        preview = page["items"][0]
        assert preview["matched_fields"] == ["attachment"]
        assert preview["preview_field"] == "attachment"
        assert "유일한 검색구문" in preview["text_preview"]
        assert preview["preview_offset"] > 0
    finally:
        store.close()


def test_other_sources_preserve_literal_text_and_ordered_substrings(tmp_path):
    store = Store(tmp_path)
    sources = ["mail", "slack", "messages", "calendar", "notes", "reminders"]

    try:
        for source in sources:
            record = Record(
                id=source,
                source=source,
                source_id=source,
                account="fixture",
                conversation_id=source,
                subject="공통 제목",
                text="vector<T> keystore 진단 abc bcd cde def",
                timestamp="2026-01-01T00:00:00Z",
            )
            store.ingest(record)

        store.index_pending()

        for source in sources:
            assert store.search('"vector<T>" KEYSTORE', source=source)["total"] == 1
            assert store.search("abcdef", source=source)["total"] == 0

        assert store.search("keystore")["total"] == len(sources)
    finally:
        store.close()
