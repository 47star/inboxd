import asyncio
import base64
import logging
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import typedstream
from watchfiles import awatch

from inboxd.domain.records import Attachment, Record, digest, now
from inboxd.ingestion.collector import Collector, run_blocking

logger = logging.getLogger(__name__)
APPLE_EPOCH = datetime(2001, 1, 1, tzinfo=UTC)


def plugin():
    return {
        "name": "messages",
        "collector": MessagesCollector,
        "description": "Locally available iMessage and SMS history",
        "timestamp_meaning": "message_sent_at",
        "content_kinds": ("message",),
        "config": {"enabled": True, "poll_seconds": 3, "reconcile_seconds": 300},
    }


def apple_timestamp(value: float) -> str:
    seconds = value / 1_000_000_000 if abs(value) > 100_000_000_000 else value

    return (APPLE_EPOCH + timedelta(seconds=seconds)).isoformat()


def serializable(row: dict) -> dict:
    return {
        key: {"base64": base64.b64encode(value).decode()} if isinstance(value, bytes) else value
        for key, value in row.items()
    }


def decode_body(value: bytes) -> str:
    try:
        decoded = typedstream.unarchive_from_data(value)

        if hasattr(decoded, "string"):
            string = decoded.string

            return string.value if hasattr(string, "value") else str(string)

        for group in getattr(decoded, "contents", []):
            for item in getattr(group, "values", []):
                if isinstance(getattr(item, "value", None), str):
                    return item.value
    except Exception:
        logger.exception("Unable to decode attributed message body; original bytes retained")

    return ""


class MessagesCollector(Collector):
    name = "messages"

    def read_attachments(self, connection: sqlite3.Connection, row_id: int) -> list[Attachment]:
        rows = connection.execute(
            "SELECT a.* FROM attachment a JOIN message_attachment_join j "
            "ON a.ROWID = j.attachment_id WHERE j.message_id = ?",
            (row_id,),
        )
        attachments = []

        for raw in rows:
            item = dict(raw)
            filename = item.get("filename") or ""

            attachments.append(
                Attachment(
                    id=item.get("guid") or digest(filename),
                    name=item.get("transfer_name") or Path(filename).name,
                    source_path=str(Path(filename).expanduser()) if filename else None,
                    content_type=item.get("mime_type"),
                    size=item.get("total_bytes"),
                )
            )

        return attachments

    def normalize(self, connection: sqlite3.Connection, row: dict) -> Record:
        row_id = row["local_id"]
        guid = row.get("guid") or str(row_id)
        chats = connection.execute(
            "SELECT c.* FROM chat c JOIN chat_message_join j "
            "ON c.ROWID = j.chat_id WHERE j.message_id = ?",
            (row_id,),
        ).fetchall()
        sender_row = connection.execute(
            "SELECT id FROM handle WHERE ROWID = ?",
            (row.get("handle_id", 0),),
        ).fetchone()
        sender = sender_row[0] if sender_row else ""
        conversation = chats[0]["guid"] if chats else "unassigned:" + sender
        recipients = []

        for chat in chats:
            handles = connection.execute(
                "SELECT h.id FROM handle h JOIN chat_handle_join j ON h.ROWID = j.handle_id "
                "JOIN chat c ON c.ROWID = j.chat_id WHERE c.guid = ?",
                (chat["guid"],),
            )
            recipients.extend(handle[0] for handle in handles)

        text = row.get("text") or ""

        if not text and row.get("attributedBody"):
            text = decode_body(row["attributedBody"])

        metadata = {
            "service": row.get("service"),
            "body_decode_status": "decoded" if text else "empty_or_unsupported",
            "chats": [serializable(dict(chat)) for chat in chats],
            "date_edited": row.get("date_edited"),
            "date_retracted": row.get("date_retracted"),
        }

        return Record(
            id="messages:" + guid,
            source=self.name,
            account=row.get("account") or "macos",
            conversation_id="messages:" + conversation,
            source_id=guid,
            timestamp=apple_timestamp(row.get("date", 0) or 0),
            sender=sender,
            recipients=sorted(set(recipients)),
            subject=row.get("subject") or "",
            text=text,
            thread_id=row.get("thread_originator_guid") or row.get("reply_to_guid"),
            outgoing=bool(row.get("is_from_me")),
            deleted=bool(row.get("is_deleted", False)),
            attachments=self.read_attachments(connection, row_id),
            metadata=metadata,
            raw=serializable(row),
        )

    def scan(self, full: bool = True) -> None:
        uri = f"{self.settings.messages_path.as_uri()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row

        try:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            details = {"scan_mode": "full" if full else "incremental"}
            origin = "live"
            after = self.checkpoint("last_rowid", 0)
            changed = 0
            scanned = 0

            if full:
                details["source_count"] = connection.execute(
                    "SELECT count(*) FROM message"
                ).fetchone()[0]
                origin = "reconcile" if self.checkpoint("backfilled", False) else "backfill"
                after = 0

            while rows := connection.execute(
                "SELECT ROWID AS local_id, * FROM message WHERE ROWID > ? ORDER BY ROWID LIMIT 250",
                (after,),
            ).fetchall():
                for raw in rows:
                    row = dict(raw)
                    after = row["local_id"]
                    changed += self.emit(self.normalize(connection, row), origin)
                    scanned += 1

                self.status(
                    "scanning", scanned_rowid=after, scanned=scanned, changed=changed, **details
                )

            self.advance("last_rowid", after)

            if full:
                self.advance("backfilled", True)
                details["last_reconcile_success"] = now()

            self.status(
                "watching",
                last_success=now(),
                scanned=scanned,
                changed=changed,
                error=None,
                coverage=(
                    "Mac-synchronized messages; "
                    "absent local rows are not inferred as remote deletion"
                ),
                **details,
            )
        finally:
            connection.close()

    async def watch(self, changed: asyncio.Event) -> None:
        async for changes in awatch(
            self.settings.messages_path.parent,
            recursive=False,
            yield_on_timeout=True,
            rust_timeout=int(self.settings.source_interval(self.name, "reconcile") * 1000),
            debounce=300,
            step=100,
        ):
            relevant = any(Path(path).name.startswith("chat.db") for _, path in changes)

            if relevant:
                changed.set()

    async def run(self) -> None:
        changed = asyncio.Event()

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.watch(changed))
            await run_blocking(self.scan)
            reconciled = time.monotonic()

            while True:
                try:
                    await asyncio.wait_for(
                        changed.wait(), timeout=self.settings.source_interval(self.name)
                    )
                except TimeoutError:
                    pass

                full = (
                    changed.is_set()
                    or time.monotonic() - reconciled
                    >= self.settings.source_interval(self.name, "reconcile")
                )
                changed.clear()
                await run_blocking(self.scan, full)

                if full:
                    reconciled = time.monotonic()
