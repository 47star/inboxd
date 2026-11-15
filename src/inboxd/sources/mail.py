import os
from datetime import UTC, datetime
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from hashlib import sha256
from pathlib import Path

from watchfiles import awatch

from inboxd.domain.records import Attachment, Record, digest, now
from inboxd.domain.text import plain_html
from inboxd.ingestion.collector import Collector, run_blocking

MAX_MESSAGE_BYTES = 100 * 1024 * 1024


def plugin():
    return {
        "name": "mail",
        "collector": MailCollector,
        "description": "Apple Mail local EMLX files; server-only mail may be absent",
        "timestamp_meaning": "email_sent_at",
        "content_kinds": ("message",),
        "config": {"enabled": True, "reconcile_seconds": 300},
    }


def mime_paths(mail: EmailMessage, prefix: str = "") -> dict[int, str]:
    result = {id(mail): prefix or "1"}

    if mail.is_multipart():
        for number, part in enumerate(mail.iter_parts(), 1):
            path = f"{prefix}.{number}" if prefix else str(number)
            result.update(mime_paths(part, path))

    return result


def attachment_directory(path: Path) -> Path:
    return path.parent.parent / "Attachments" / path.name.split(".")[0]


def attachment_signature(path: Path) -> str:
    directory = attachment_directory(path)
    entries = []

    if directory.exists():
        for item in sorted(directory.rglob("*")):
            if item.is_file():
                stat = item.stat()
                entries.append((str(item.relative_to(directory)), stat.st_size, stat.st_mtime_ns))

    return digest(repr(entries))


def read_emlx(path: Path) -> bytes:
    with path.open("rb") as stream:
        length = int(stream.readline().strip())

        if not 0 <= length <= MAX_MESSAGE_BYTES:
            raise ValueError("Invalid EMLX message length")

        payload = stream.read(length)

    if len(payload) != length:
        raise OSError("EMLX file is not fully written")

    return payload


def mail_timestamp(mail: EmailMessage, fallback: float) -> str:
    try:
        date = parsedate_to_datetime(str(mail.get("Date")))

        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
    except (TypeError, ValueError):
        date = datetime.fromtimestamp(fallback, UTC)

    return date.astimezone(UTC).isoformat()


def body_text(mail: EmailMessage) -> str:
    body = mail.get_body(preferencelist=("plain", "html"))

    if body is None:
        return ""

    text = body.get_content()

    if not isinstance(text, str):
        text = text.decode("utf-8", errors="replace")

    return plain_html(text) if body.get_content_type() == "text/html" else text


class MailCollector(Collector):
    name = "mail"

    def preserve(self, payload: bytes, category: str) -> Path:
        content_hash = sha256(payload).hexdigest()
        directory = self.store.path / category / content_hash[:2]
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination = directory / content_hash

        if not destination.exists():
            temporary = destination.with_suffix(".tmp")

            with temporary.open("wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())

            temporary.replace(destination)

        return destination

    def attachments(self, mail: EmailMessage, source: Path) -> list[Attachment]:
        attachments = []
        locations = mime_paths(mail)
        parts = list(mail.iter_attachments())
        selected = {id(part) for part in parts}

        for part in mail.walk():
            if id(part) not in selected and not part.is_multipart() and part.get_filename():
                parts.append(part)

        for index, part in enumerate(parts):
            payload = part.get_payload(decode=True)
            filename = part.get_filename() or f"attachment-{index}"
            path = self.preserve(payload, "mail-parts") if payload else None
            size = len(payload) if payload else None

            if path is None:
                directory = attachment_directory(source) / locations[id(part)]
                candidates = sorted(item for item in directory.glob("*") if item.is_file())

                if len(candidates) == 1 and candidates[0].stat().st_size > 0:
                    path = candidates[0]
                    size = path.stat().st_size
                    filename = path.name

            attachments.append(
                Attachment(
                    id=str(index),
                    name=filename,
                    source_path=str(path) if path else None,
                    content_type=part.get_content_type(),
                    size=size,
                )
            )

        return attachments

    def parse_file(self, path: Path, origin: str) -> bool:
        stat = path.stat()
        relative = str(path.relative_to(self.settings.mail_path))
        signature = f"v3:{stat.st_mtime_ns}:{stat.st_size}:{attachment_signature(path)}"
        checkpoint = "mailfile:" + digest(relative)

        if self.store.get(checkpoint) == signature:
            return False

        payload = read_emlx(path)
        raw_path = self.preserve(payload, "mail-raw")
        mail = BytesParser(policy=policy.default).parsebytes(payload)
        source_id = str(mail.get("Message-ID") or sha256(payload).hexdigest()).strip()
        references = str(mail.get("References") or mail.get("In-Reply-To") or "").split()
        root = references[0] if references else source_id
        parts = Path(relative).parts

        record = Record(
            id="mail:" + digest(relative),
            source=self.name,
            account=parts[1] if len(parts) > 1 else "apple-mail",
            source_id=source_id,
            conversation_id="mail:" + digest(root),
            thread_id=root,
            timestamp=mail_timestamp(mail, stat.st_mtime),
            sender=", ".join(address for _, address in getaddresses(mail.get_all("From", []))),
            recipients=[
                address
                for _, address in getaddresses(mail.get_all("To", []) + mail.get_all("Cc", []))
            ],
            subject=str(mail.get("Subject", "")),
            text=body_text(mail),
            outgoing=any("sent" in part.lower() for part in parts),
            attachments=self.attachments(mail, path),
            metadata={
                "path": relative,
                "message_id": source_id,
                "references": references,
                "partial": path.name.endswith(".partial.emlx"),
                "raw_path": str(raw_path),
            },
            raw={"headers": list(mail.raw_items()), "mime_sha256": raw_path.name},
        )

        changed = self.emit(record, origin)
        self.store.set(checkpoint, signature)

        return changed

    def scan(self) -> None:
        root = self.settings.mail_path
        list(root.iterdir())
        origin = "reconcile" if self.checkpoint("backfilled", False) else "backfill"
        count = changed = partial = 0
        errors = []

        for path in root.rglob("*.emlx"):
            try:
                changed += self.parse_file(path, origin)
            except (FileNotFoundError, ValueError, OSError) as error:
                errors.append({"path": str(path), "error": str(error)})

            count += 1
            partial += path.name.endswith(".partial.emlx")

            if count % 250 == 0:
                self.status("backfilling", scanned=count, changed=changed)

        if not errors:
            self.advance("backfilled", True)

        self.status(
            "watching" if not errors else "partial",
            scanned=count,
            changed=changed,
            partial_files=partial,
            file_errors=errors[:100],
            last_success=now(),
            error=None,
            coverage="Apple Mail local EMLX; server-only content is not yet archived",
        )

    async def run(self) -> None:
        await run_blocking(self.scan)

        async for changes in awatch(
            self.settings.mail_path,
            yield_on_timeout=True,
            rust_timeout=int(self.settings.source_interval(self.name, "reconcile") * 1000),
            debounce=500,
        ):
            if not changes:
                await run_blocking(self.scan)
                continue

            for _, filename in changes:
                path = Path(filename)

                if path.suffix != ".emlx" or not path.is_file():
                    continue

                try:
                    await run_blocking(self.parse_file, path, "live")
                except (FileNotFoundError, ValueError, OSError) as error:
                    self.status("partial", error=str(error), retry_on_reconcile=True)

            self.status("watching", last_success=now())
