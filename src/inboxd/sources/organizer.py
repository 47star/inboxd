import asyncio
import json
from datetime import UTC, datetime

from inboxd.domain.records import Record, digest, now
from inboxd.domain.text import plain_html
from inboxd.ingestion.collector import Collector, MissingConfiguration, run_blocking
from inboxd.ingestion.snapshots import SnapshotReconciler


def calendar_plugin():
    return organizer_plugin(
        "calendar",
        "EventKit occurrences within the configured calendar interval",
        "occurrence_start_including_future",
        {"enabled": True, "poll_seconds": 60, "start_year": 1970, "future_years": 5},
    )


def notes_plugin():
    return organizer_plugin(
        "notes",
        "Notes available through the local Notes application",
        "note_created_at",
        {"enabled": True, "poll_seconds": 60},
    )


def reminders_plugin():
    return organizer_plugin(
        "reminders",
        "Locally accessible EventKit reminders",
        "reminder_created_at",
        {"enabled": True, "poll_seconds": 60},
    )


def organizer_plugin(source, description, timestamp_meaning, config):
    return {
        "name": source,
        "collector": lambda settings, store: OrganizerCollector(settings, store, source),
        "description": description,
        "timestamp_meaning": timestamp_meaning,
        "content_kinds": (source,),
        "config_constraints": {
            "start_year": {"ge": 1900, "le": 2100},
            "future_years": {"ge": 1, "le": 50},
        },
        "config": config,
    }


class OrganizerCollector(Collector):
    def __init__(self, settings, store, source: str):
        super().__init__(settings, store)
        self.name = source
        self.reconciler = SnapshotReconciler(self)

    def helper_arguments(self) -> list[str]:
        helper = self.settings.apple_helper

        if not helper.exists():
            raise MissingConfiguration("Native Apple source helper is not installed")

        arguments = [str(helper), self.name]

        if self.name == "notes":
            arguments.append(str(helper.parents[1] / "Resources/notes.js"))
        elif self.name == "calendar":
            arguments.extend(
                [
                    str(self.settings.source_value(self.name, "start_year", 1970)),
                    str(
                        datetime.now(UTC).year
                        + self.settings.source_value(self.name, "future_years", 5)
                        + 1
                    ),
                    "--watch",
                ]
            )
        else:
            arguments.append("--watch")

        if self.name != "notes":
            arguments.extend(["--poll-seconds", str(self.settings.source_interval(self.name))])

        return arguments

    def normalize(self, raw: dict) -> Record:
        raw = dict(raw)

        for field in ("attendees", "alarms", "recurrence"):
            if isinstance(raw.get(field), list):
                raw[field] = sorted(raw[field], key=lambda item: json.dumps(item, sort_keys=True))

        identity = self.name + ":" + digest(raw["id"])
        group = raw.get("folder_id") or raw.get("calendar_id") or "default"
        text = raw.get("text", "")

        if self.name == "notes":
            text = plain_html(text)

        return Record(
            id=identity,
            source=self.name,
            account=raw.get("account", "macos"),
            source_id=raw["id"],
            conversation_id=self.name + ":" + group,
            timestamp=raw.get("start") or raw.get("created") or "1970-01-01T00:00:00+00:00",
            subject=raw.get("title", ""),
            text=text,
            kind=self.name,
            raw=raw,
            metadata={key: value for key, value in raw.items() if key not in {"text", "title"}},
        )

    def apply(self, snapshot: dict) -> None:
        records = [self.normalize(raw) for raw in snapshot["items"]]
        calendars = snapshot.get("calendars", [])
        scope = {
            "calendars": sorted(calendar["id"] for calendar in calendars),
            "start_year": snapshot.get("start_year"),
            "end_year": snapshot.get("end_year_exclusive"),
            "folders": sorted(snapshot.get("folder_ids", [])),
        }
        snapshot_id = snapshot.get("snapshot_id") or now()
        scope_id = digest(json.dumps(scope, sort_keys=True))
        result = self.reconciler.apply(
            records, scope_id, snapshot.get("complete", False), snapshot_id
        )
        coverage = {
            key: value for key, value in snapshot.items() if key not in {"items", "snapshot_id"}
        }

        self.status(
            "watching" if result.complete else "partial",
            count=result.records,
            changed=result.changed,
            pending_deletions=result.missing,
            confirmed_deletions=result.deleted,
            last_success=now(),
            error=None,
            coverage=coverage,
        )

    async def run_helper(self) -> None:
        process = await asyncio.create_subprocess_exec(
            *self.helper_arguments(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=64 * 1024 * 1024,
        )

        async def read_errors() -> bytes:
            return await process.stderr.read(64 * 1024)

        error_task = asyncio.create_task(read_errors())

        try:
            while True:
                timeout = max(360, self.settings.source_interval(self.name) * 2 + 60)
                line = await asyncio.wait_for(process.stdout.readline(), timeout=timeout)

                if not line:
                    break

                snapshot = json.loads(line)

                if snapshot.get("error"):
                    raise PermissionError(snapshot["error"])

                if snapshot.get("type") == "heartbeat":
                    continue

                await run_blocking(self.apply, snapshot)

            code = await process.wait()

            if code:
                details = (await error_task).decode(errors="replace")
                raise RuntimeError(f"Apple helper exited {code}: {details[:1000]}")
        finally:
            if process.returncode is None:
                process.terminate()

                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except TimeoutError:
                    process.kill()
                    await process.wait()

            error_task.cancel()
            await asyncio.gather(error_task, return_exceptions=True)

    async def run(self) -> None:
        while True:
            await self.run_helper()
            await asyncio.sleep(self.settings.source_interval(self.name))
