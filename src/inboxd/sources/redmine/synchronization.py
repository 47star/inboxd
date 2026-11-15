import asyncio

from inboxd.domain.records import now
from inboxd.ingestion.collector import run_blocking
from inboxd.sources.redmine.client import IssueUnavailable, RedmineReader


def related_ids(fields: dict) -> set[int]:
    result = set()

    for relation in fields.get("relations", []):
        result.update(int(relation[key]) for key in ("issue_id", "issue_to_id"))

    if fields.get("parent"):
        result.add(int(fields["parent"]["id"]))

    for child in fields.get("children", []):
        result.add(int(child["id"]))
        result.update(related_ids(child))

    return result


class IssueSynchronizer:
    def __init__(self, collector, reader: RedmineReader):
        self.collector = collector
        self.reader = reader

    def key(self, identity: int) -> str:
        return f"message:redmine:{self.collector.account}:issue:{identity}"

    async def activity(self) -> int:
        owner = self.collector
        previous = set(owner.state().get("activity_signals", []))
        signals = await self.reader.activity()
        identities = sorted(
            {identity for identity, signature in signals if signature not in previous}
        )
        changed = await self.refresh(identities, "rss", follow_relations=True)
        owner.save(activity_signals=[signature for _, signature in signals])

        return changed

    async def refresh(
        self, identities: list[int], origin: str, follow_relations: bool = False
    ) -> int:
        changed = 0
        owner = self.collector
        related_prefix = f"redmine-related:{owner.account}:"

        for start in range(0, len(identities), 4):
            batch = identities[start : start + 4]
            results = await asyncio.gather(
                *(self.reader.issue(identity) for identity in batch), return_exceptions=True
            )

            for identity, result in zip(batch, results, strict=True):
                access_key = f"redmine-access:{owner.account}:{identity}"

                if isinstance(result, IssueUnavailable):
                    owner.store.set(
                        access_key,
                        {"state": "unavailable", "http_status": result.status, "checked_at": now()},
                    )
                    continue

                if isinstance(result, BaseException):
                    raise result

                previous = owner.store.get(self.key(identity), {})
                old_fields = previous.get("metadata", {}).get("work_item", {}).get("fields", {})

                if follow_relations and old_fields != result.fields:
                    for target in related_ids(old_fields) | related_ids(result.fields):
                        if target != identity:
                            owner.store.set(related_prefix + str(target), target)

                updated = await run_blocking(owner.apply, result, origin)
                changed += updated
                owner.store.set(access_key, {"state": "accessible", "checked_at": now()})

        if follow_relations:
            pending = owner.store.scan(related_prefix)
            targets = sorted({target for _, target in pending} - set(identities))
            changed += await self.refresh(targets, origin)

            for key, _ in pending:
                owner.store.delete(key)

        return changed

    async def enrich_once(self) -> None:
        owner = self.collector
        state = owner.state()

        if state.get("metadata_backfill_complete"):
            return

        prefix = f"message:redmine:{owner.account}:issue:"
        cursor = state.get("metadata_cursor")
        count = state.get("metadata_backfilled", 0)
        owner.status("backfilling", backfill_running=True, metadata_backfill_complete=False)

        while rows := owner.store.scan(prefix, after=cursor, limit=40):
            identities = [
                int(record["source_id"])
                for _, record in rows
                if record.get("metadata", {}).get("work_item", {}).get("schema_version", 1) < 2
            ]
            await self.refresh(identities, "backfill")
            count += len(identities)
            cursor = rows[-1][0]
            owner.save(metadata_cursor=cursor, metadata_backfilled=count)
            owner.status("backfilling", metadata_backfilled=count)

        owner.save(metadata_backfill_complete=True)
        owner.status("watching", backfill_running=False, metadata_backfill_complete=True)

    async def reconcile(self) -> int:
        owner = self.collector
        prefix = f"message:redmine:{owner.account}:issue:"
        state = owner.state()
        rows = owner.store.scan(
            prefix,
            after=state.get("reconcile_cursor"),
            limit=owner.settings.source_value("redmine", "reconcile_batch_size", 40),
        )

        if not rows:
            owner.save(reconcile_cursor=None, metadata_cycle_completed_at=now())

            return 0

        changed = await self.refresh(
            [int(record["source_id"]) for _, record in rows], "reconcile", follow_relations=True
        )
        owner.save(reconcile_cursor=rows[-1][0])

        return changed
