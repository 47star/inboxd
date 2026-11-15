from email import policy
from email.parser import BytesParser

from inboxd.domain.records import digest
from inboxd.ingestion.attachments import AttachmentWorker
from inboxd.runtime.config import Settings
from inboxd.sources.mail import MailCollector
from inboxd.sources.organizer import OrganizerCollector
from inboxd.storage.archive import Store


def test_mail_external_mime_part_recovery(tmp_path):
    root = tmp_path / "source"
    path = root / "Data/Messages/42.partial.emlx"
    path.parent.mkdir(parents=True)
    payload = (
        b'MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="parts"\r\n\r\n'
        b"--parts\r\nContent-Type: text/plain\r\n\r\nBody\r\n"
        b"--parts\r\nContent-Type: text/plain\r\n"
        b'Content-Disposition: attachment; filename="damaged.txt"\r\n\r\n\r\n--parts--\r\n'
    )
    path.write_bytes(str(len(payload)).encode() + b"\n" + payload)
    separate = root / "Data/Attachments/42/2/original.txt"
    separate.parent.mkdir(parents=True)
    separate.write_bytes(b"Recovered attachment")
    settings = Settings(data_dir=tmp_path / "archive", mail_path=root)
    store = Store(settings.data_dir)

    try:
        collector = MailCollector(settings, store)
        assert collector.parse_file(path, "backfill")
        key, job = store.scan("attachment:")[0]
        assert job["attachment"]["source_path"] == str(separate)
        AttachmentWorker(settings, store).copy_attachment(key, job)
        assert store.get(key)["bytes"] == len(b"Recovered attachment")
        assert store.get(key)["text"] == "Recovered attachment"
        assert not collector.parse_file(path, "reconcile")
        separate.write_bytes(b"Updated attachment")
        assert collector.parse_file(path, "reconcile")
        assert store.get(key)["state"] == "pending"
        assert (
            collector.attachments(BytesParser(policy=policy.default).parsebytes(payload), path)[
                0
            ].name
            == "original.txt"
        )
    finally:
        store.close()


def test_calendar_order_and_conservative_deletion(tmp_path):
    settings = Settings(data_dir=tmp_path)
    store = Store(tmp_path)

    try:
        collector = OrganizerCollector(settings, store, "calendar")
        item = {"id": "event", "attendees": [{"url": "b"}, {"url": "a"}]}
        ordered = {**item, "attendees": list(reversed(item["attendees"]))}
        assert collector.normalize(item).fingerprint() == collector.normalize(ordered).fingerprint()
        apply = collector.reconciler.apply
        apply([collector.normalize(item)], "scope", True, "one")
        apply([], "scope", True, "two")
        apply([], "scope", False, "three")
        identity = "message:calendar:" + digest("event")
        assert not store.get(identity)["deleted"]
        apply([], "scope", True, "four")
        assert store.get(identity)["deleted"]
    finally:
        store.close()
