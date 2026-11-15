import asyncio
from datetime import datetime, timedelta
from urllib.parse import urlsplit

import httpx

from inboxd.domain.records import digest, now
from inboxd.domain.work_items import WorkItem
from inboxd.ingestion.collector import Collector, MissingConfiguration
from inboxd.runtime.config import load_secrets
from inboxd.sources.redmine.client import RedmineReader
from inboxd.sources.redmine.synchronization import IssueSynchronizer


class RedmineCollector(Collector):
    name = "redmine"

    def __init__(self, settings, store):
        super().__init__(settings, store)
        self.base_url = settings.source_value("redmine", "url", "").rstrip("/")
        self.account = digest(self.base_url)[:16]

    def state(self) -> dict:
        return self.checkpoint(self.account, {})

    def save(self, **changes) -> dict:
        state = {**self.state(), **changes}
        self.advance(self.account, state)

        return state

    def apply(self, item: WorkItem, origin: str) -> bool:
        record = item.record(self.name, self.account)

        return self.emit(record, origin)

    async def backfill(self, reader: RedmineReader) -> None:
        state = self.state()

        if state.get("backfill_complete"):
            return

        if "upper_id" not in state:
            _, boundary = await reader.feed("1970-01-01")
            upper_id = await reader.newest_id()
            state = self.save(
                upper_id=upper_id,
                cursor_id=0,
                backfilled=0,
                feed_since=boundary,
                backfill_started_at=now(),
            )

        self.status("backfilling", backfill_running=True, backfill_complete=False)

        while state["cursor_id"] < state["upper_id"]:
            items = await reader.history(state["cursor_id"], state["upper_id"])

            if not items:
                break

            identities = [int(item.external_id) for item in items]

            if (
                identities != sorted(set(identities))
                or min(identities) <= state["cursor_id"]
                or max(identities) > state["upper_id"]
            ):
                raise ValueError("Redmine API ignored the backfill ID range or ordering")

            await IssueSynchronizer(self, reader).refresh(identities, "backfill")

            state = self.save(
                cursor_id=max(identities), backfilled=state["backfilled"] + len(items)
            )
            self.status(
                "backfilling",
                backfilled=state["backfilled"],
                cursor_id=state["cursor_id"],
                upper_id=state["upper_id"],
            )

        self.save(backfill_complete=True, backfill_completed_at=now())
        self.status("watching", backfill_running=False, backfill_complete=True)

    async def poll(self, reader: RedmineReader) -> None:
        state = self.state()
        since = (datetime.fromisoformat(state["feed_since"]) - timedelta(days=2)).date().isoformat()
        upper = None
        boundary = None
        changed = 0
        received = 0
        pages = 0

        while True:
            items, server_time = await reader.feed(since, upper)
            boundary = boundary or server_time
            pages += 1

            if not items:
                break

            identities = [int(item.external_id) for item in items]

            if (
                identities != sorted(set(identities), reverse=True)
                or (upper is not None and max(identities) > upper)
                or min(identities) < 1
            ):
                raise ValueError("Redmine feed ignored the ID range or ordering")

            changed += await IssueSynchronizer(self, reader).refresh(
                identities, "rss", follow_relations=True
            )

            received += len(items)
            upper = min(identities) - 1

            if upper == 0:
                break

        self.save(feed_since=max(state["feed_since"], boundary), last_feed_success=now())
        self.status(
            "watching",
            error=None,
            last_success=now(),
            backfill_complete=True,
            backfill_running=False,
            backfilled=state.get("backfilled", 0),
            feed_received=received,
            feed_changed=changed,
            feed_pages=pages,
            feed_since=self.state()["feed_since"],
            mode="rss_triggered_api",
            coverage=(
                "Initial accessible issues; Atom triggers complete API issue refresh. "
                "Existing issues are reconciled in batches for changes absent from feeds. "
                "Only API-visible fields and journals; unavailable does not prove deletion."
            ),
        )

    async def run(self) -> None:
        credentials = load_secrets(self.settings)
        api_key = credentials.get("redmine_api_key", "")
        feed_key = credentials.get("redmine_feed_key") or api_key
        url = urlsplit(self.base_url)

        if url.scheme not in {"http", "https"} or not url.netloc or url.query or url.fragment:
            raise MissingConfiguration("Configure sources.redmine.url as the Redmine root URL")

        if not feed_key or not api_key:
            raise MissingConfiguration("Redmine API and feed access keys are required")

        async with httpx.AsyncClient(timeout=60, follow_redirects=False, trust_env=False) as client:
            reader = RedmineReader(client, self.base_url, api_key, feed_key)
            await self.backfill(reader)
            synchronizer = IssueSynchronizer(self, reader)
            await synchronizer.enrich_once()

            while True:
                await self.poll(reader)
                activity_changed = await synchronizer.activity()
                changed = await synchronizer.reconcile()
                self.status(
                    "watching",
                    metadata_changed=changed,
                    activity_changed=activity_changed,
                    metadata_backfill_complete=True,
                    metadata_cycle_completed_at=self.state().get("metadata_cycle_completed_at"),
                )
                await asyncio.sleep(self.settings.source_interval("redmine"))
