import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from inboxd.domain.records import Attachment, Record
from inboxd.ingestion.attachments import AttachmentWorker
from inboxd.runtime.config import Settings
from inboxd.storage.archive import Store


def test_pdf_surrogate_recovery_and_size_limit_state(tmp_path, monkeypatch):
    settings = Settings(
        data_dir=tmp_path / "archive",
        attachment_max_bytes=100,
        sources={"messages": {"enabled": True}},
    )
    store = Store(settings.data_dir)
    source = tmp_path / "fixture.pdf"
    source.write_bytes(b"%PDF-fixture")
    item = Record(
        id="fixture:1",
        source="messages",
        account="fixture",
        source_id="1",
        conversation_id="fixture",
        kind="message",
        timestamp="2026-11-05T00:00:00Z",
        subject="게시물",
        text="본문",
        attachments=[
            Attachment(id="pdf", name="fixture.pdf", source_path=str(source)),
            Attachment(id="large", name="large.zip", size=101),
        ],
    )
    reader = Mock(return_value=SimpleNamespace(pages=[Mock()]))
    reader.return_value.pages[0].extract_text.return_value = "검색대상\udf2b복구본문"
    monkeypatch.setattr("inboxd.ingestion.attachments.PdfReader", reader)

    async def finish(seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr("inboxd.ingestion.attachments.asyncio.sleep", finish)

    try:
        store.ingest(item)

        for key, job in store.scan("attachment:"):
            if job["attachment"]["id"] == "pdf":
                store.set(
                    key,
                    {
                        **job,
                        "state": "retrying",
                        "error": "UnicodeEncodeError: fixture",
                        "retry_at": 9999999999,
                    },
                )

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(AttachmentWorker(settings, store).run())

        jobs = {job["attachment"]["id"]: job for _, job in store.scan("attachment:")}
        assert jobs["large"]["state"] == "size_limit"
        assert jobs["pdf"]["state"] == "ready"
        assert jobs["pdf"]["text"] == "검색대상\ufffd복구본문"
        store.index_pending()
        assert store.search("복구본문", source="messages")["total"] == 1
    finally:
        store.close()
