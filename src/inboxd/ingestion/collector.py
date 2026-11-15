import asyncio
import logging
import random
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Callable

from inboxd.domain.records import Record, now
from inboxd.runtime.config import Settings
from inboxd.storage.archive import Store

logger = logging.getLogger(__name__)


class MissingConfiguration(Exception):
    pass


async def run_blocking[Result](function: Callable[..., Result], *arguments) -> Result:
    task = asyncio.create_task(asyncio.to_thread(function, *arguments))

    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class Collector(ABC):
    name = "collector"

    def __init__(self, settings: Settings, store: Store):
        self.settings = settings
        self.store = store

    def emit(self, record: Record, origin: str = "live") -> bool:
        if record.source != self.name:
            raise ValueError(f"Collector {self.name} cannot emit source {record.source}")

        changed = self.store.ingest(record, origin)

        if changed:
            logger.info(
                "Source record persisted",
                extra={
                    "event": "record_ingested",
                    "details": {
                        "source": self.name,
                        "record_id": record.id,
                        "kind": record.kind,
                        "origin": origin,
                        "source_timestamp": record.timestamp,
                        "deleted": record.deleted,
                        "attachment_count": len(record.attachments),
                        "text_length": len(record.text),
                    },
                },
            )

        return changed

    def checkpoint(self, name: str, default=None):
        return self.store.get(f"checkpoint:{self.name}:{name}", default)

    def advance(self, name: str, value) -> None:
        self.store.set(f"checkpoint:{self.name}:{name}", value)

    def status(self, state: str, **values) -> None:
        previous = self.store.get("status:" + self.name, {})

        self.store.set(
            "status:" + self.name,
            {**previous, "state": state, "updated_at": now(), **values},
        )

        if previous.get("state") != state or previous.get("error") != values.get(
            "error", previous.get("error")
        ):
            logger.info(
                "Collector state changed",
                extra={
                    "event": "collector_state",
                    "details": {
                        "source": self.name,
                        "previous_state": previous.get("state"),
                        "state": state,
                        "error": values.get("error"),
                        "retry_seconds": values.get("retry_seconds"),
                    },
                },
            )

    async def supervise(self) -> None:
        delay = 1

        while True:
            try:
                self.status("starting", error=None)
                await self.run()
                delay = 1
            except asyncio.CancelledError:
                self.status("stopped")
                raise
            except Exception as error:
                logger.exception("Collector %s interrupted", self.name)
                self.status(
                    self.error_state(error),
                    error=f"{type(error).__name__}: {error}",
                    retry_seconds=delay,
                )
                await asyncio.sleep(delay + random.uniform(0, min(delay, 5)))
                delay = min(delay * 2, 60)

    @staticmethod
    def error_state(error: Exception) -> str:
        if isinstance(error, MissingConfiguration):
            return "configuration_required"

        if isinstance(error, (PermissionError, sqlite3.OperationalError)):
            return "source_unavailable"

        return "retrying"

    @abstractmethod
    async def run(self) -> None:
        raise NotImplementedError
