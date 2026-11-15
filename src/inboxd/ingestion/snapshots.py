from dataclasses import dataclass

from inboxd.domain.records import Record
from inboxd.ingestion.collector import Collector


@dataclass(frozen=True)
class SnapshotResult:
    records: int
    changed: int
    missing: int
    deleted: int
    complete: bool


class SnapshotReconciler:
    def __init__(self, collector: Collector) -> None:
        self.collector = collector

    def apply(
        self,
        records: list[Record],
        scope: str,
        complete: bool,
        snapshot_id: str,
    ) -> SnapshotResult:
        owner = self.collector
        state = owner.checkpoint("snapshot", {})

        if state.get("snapshot_id") == snapshot_id:
            return SnapshotResult(len(records), 0, len(state.get("missing", [])), 0, complete)

        origin = "reconcile" if owner.checkpoint("backfilled", False) else "backfill"
        identities = {record.id for record in records}
        changed = sum(owner.emit(record, origin) for record in records)

        if not complete:
            return SnapshotResult(len(records), changed, 0, 0, False)

        previous_ids = set(state.get("ids", []))
        previous_missing = set(state.get("missing", []))
        same_scope = state.get("scope") == scope
        missing = previous_ids - identities if same_scope else set()
        confirmed = missing & previous_missing

        for identity in confirmed:
            previous = owner.store.get("message:" + identity)

            if previous is not None and not previous["deleted"]:
                record = Record.model_validate({**previous, "deleted": True})
                changed += owner.emit(record, "reconcile")

        pending_missing = missing - confirmed
        owner.advance(
            "snapshot",
            {
                "snapshot_id": snapshot_id,
                "scope": scope,
                "ids": sorted(identities | pending_missing),
                "missing": sorted(pending_missing),
            },
        )
        owner.advance("backfilled", True)

        return SnapshotResult(len(records), changed, len(pending_missing), len(confirmed), True)
