import asyncio
import hashlib
import os
import re
import time
from pathlib import Path

import httpx
from pypdf import PdfReader
from pypdf.errors import PyPdfError

from inboxd.domain.records import now
from inboxd.ingestion.collector import run_blocking
from inboxd.runtime.config import Settings, load_secrets
from inboxd.storage.archive import Store


class AttachmentWorker:
    def __init__(self, settings: Settings, store: Store):
        self.settings = settings
        self.store = store
        self.directory = store.path / "attachments"
        self.directory.mkdir(exist_ok=True, mode=0o700)

    async def process(self, key: str, job: dict):
        await run_blocking(self.copy_attachment, key, job)

    def copy_attachment(self, key: str, job: dict) -> None:
        attachment = job["attachment"]
        size = attachment.get("size")

        if size and size > self.settings.attachment_max_bytes:
            self.store.set(
                key,
                {**job, "state": "size_limit", "error": None, "retry_at": 0, "updated_at": now()},
            )

            return

        temporary = self.directory / (key.removeprefix("attachment:") + ".part")
        hasher = hashlib.sha256()
        total = 0

        try:
            with temporary.open("wb") as output:
                if attachment.get("source_path"):
                    with Path(attachment["source_path"]).open("rb") as source:
                        while chunk := source.read(256 * 1024):
                            total += len(chunk)

                            if total > self.settings.attachment_max_bytes:
                                raise ValueError("Attachment exceeds configured size limit")

                            output.write(chunk)
                            hasher.update(chunk)
                elif attachment.get("source_url"):
                    token = load_secrets(self.settings).get("slack_user_token")

                    if not token:
                        raise ValueError("Slack user token is not configured")

                    url = httpx.URL(attachment["source_url"])

                    if url.scheme != "https" or not (
                        url.host == "slack.com" or url.host.endswith(".slack.com")
                    ):
                        raise ValueError("Unexpected attachment origin")

                    with (
                        httpx.Client(timeout=60, follow_redirects=False, trust_env=False) as client,
                        client.stream(
                            "GET", str(url), headers={"Authorization": f"Bearer {token}"}
                        ) as response,
                    ):
                        response.raise_for_status()

                        for chunk in response.iter_bytes():
                            total += len(chunk)

                            if total > self.settings.attachment_max_bytes:
                                raise ValueError("Attachment exceeds configured size limit")

                            output.write(chunk)
                            hasher.update(chunk)
                else:
                    raise FileNotFoundError(
                        "Attachment content is not downloaded by its source yet"
                    )

                output.flush()
                os.fsync(output.fileno())

            content_hash = hasher.hexdigest()

            if total == 0 and attachment.get("size") != 0:
                raise FileNotFoundError("Attachment contains an unresolved empty source payload")

            destination = self.directory / content_hash
            temporary.replace(destination)
            text = ""
            suffix = Path(attachment["name"]).suffix.lower()
            textual = (attachment.get("content_type") or "").startswith("text/")

            if textual or suffix in {
                ".txt",
                ".log",
                ".sql",
                ".json",
                ".xml",
                ".csv",
                ".md",
                ".conf",
                ".sh",
            }:
                with destination.open("rb") as source:
                    text = source.read(2 * 1024 * 1024).decode("utf-8", errors="replace")

            completed = {
                **job,
                "state": "ready",
                "error": None,
                "retry_at": 0,
                "path": str(destination),
                "sha256": content_hash,
                "bytes": total,
                "text": text,
                "updated_at": now(),
                "extraction": "text" if text else "not_supported",
            }

            if suffix == ".pdf":
                try:
                    reader = PdfReader(destination)
                    pages = []
                    characters = 0

                    for page in reader.pages[:200]:
                        content = page.extract_text() or ""
                        pages.append(content)
                        characters += len(content)

                        if characters >= 2 * 1024 * 1024:
                            break

                    text = "\n".join(pages)[: 2 * 1024 * 1024]
                    text = re.sub(r"[\ud800-\udfff]", "\ufffd", text)
                    completed.update(text=text, extraction="pdf_text" if text else "no_pdf_text")
                except (PyPdfError, OSError, ValueError, TypeError) as error:
                    completed.update(
                        extraction="pdf_text_failed", extraction_error=type(error).__name__
                    )

            if self.store.get(key, {}).get("attachment") != attachment:
                return

            self.store.set(key, completed)

            if text:
                texts = [
                    value.get("text", "")
                    for _, value in self.store.scan("attachment:")
                    if value["message_id"] == job["message_id"] and value.get("state") == "ready"
                ]
                self.store.set("extracted:" + job["message_id"], "\n".join(texts))
                self.store.set("pending:" + job["message_id"], job["message_id"])
        finally:
            temporary.unlink(missing_ok=True)

    async def run(self):
        for key, job in self.store.scan("attachment:"):
            if not self.settings.source_enabled(job.get("source", "")):
                continue

            if job.get("state") == "retrying" and job.get("error", "").startswith(
                "UnicodeEncodeError:"
            ):
                self.store.set(key, {**job, "retry_at": 0})

        while True:
            for key, job in self.store.scan("attachment:"):
                if not self.settings.source_enabled(job.get("source", "")):
                    continue

                if (
                    job["state"] == "ready"
                    and job.get("bytes", 0) == 0
                    and job.get("source") == "mail"
                ):
                    job = {
                        **job,
                        "state": "unavailable",
                        "error": "Legacy empty Mail payload requires source reconciliation",
                        "retry_at": 0,
                    }
                    self.store.set(key, job)

                if (
                    job["state"] not in {"pending", "retrying", "unavailable"}
                    or job.get("retry_at", 0) > time.time()
                ):
                    continue

                try:
                    await self.process(key, job)
                except asyncio.CancelledError:
                    raise
                except (OSError, ValueError, httpx.HTTPError) as error:
                    attempts = job.get("attempts", 0) + 1

                    if self.store.get(key, {}).get("attachment") != job["attachment"]:
                        continue

                    self.store.set(
                        key,
                        {
                            **job,
                            "state": "unavailable"
                            if isinstance(error, FileNotFoundError)
                            else "retrying",
                            "attempts": attempts,
                            "retry_at": time.time() + min(3600, 5 * 2 ** min(attempts, 10)),
                            "error": f"{type(error).__name__}: {error}",
                        },
                    )

            await asyncio.sleep(3)
