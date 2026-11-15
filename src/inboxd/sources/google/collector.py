import asyncio
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

from inboxd.domain.records import Record, digest, now
from inboxd.ingestion.collector import Collector, run_blocking
from inboxd.sources.google.auth import GoogleAuthenticationRequired
from inboxd.sources.google.client import GoogleClient, GoogleUnavailable
from inboxd.sources.google.records import normalize, record_id, timestamp


class GoogleCollector(Collector):
    def __init__(self, settings, store, source):
        super().__init__(settings, store)
        self.name = source
        self.client = GoogleClient(settings.data_dir / "google")
        self.account = ""

    def scope_key(self, container):
        return f"scope:{self.account}:{digest(container['id'])}"

    def containers(self):
        path = "/users/me/calendarList" if self.name == "google_calendar" else "/users/@me/lists"
        params = {"maxResults": 250 if self.name == "google_calendar" else 100}

        if self.name == "google_calendar":
            params["showHidden"] = "true"

        result = []

        while True:
            response = self.client.get(self.name, path, params)
            result.extend(response.get("items", []))

            if not response.get("nextPageToken"):
                return result

            params["pageToken"] = response["nextPageToken"]

    def begin(self, container, state):
        current = datetime.now(UTC)
        bounds = [
            self.settings.source_value("google_calendar", "start_year", 1970),
            current.year + self.settings.source_value("google_calendar", "future_years", 5) + 1,
        ]
        full = not state.get("backfilled")

        if self.name == "google_calendar":
            full = full or not state.get("sync_token") or state.get("bounds") != bounds
        else:
            full = full or time.time() >= state.get("full_due", 0)

        return {
            **state,
            "in_progress": True,
            "full": full,
            "bounds": bounds,
            "seen": [],
            "page_token": None,
            "started_at": current.isoformat(),
            "error": None,
        }

    def sync_scope(self, container):
        key = self.scope_key(container)
        state = self.checkpoint(key, {})

        if not state.get("in_progress"):
            if time.time() < state.get("next_due", 0):
                return state

            state = self.begin(container, state)
            self.advance(key, state)

        identity = quote(container["id"], safe="")
        path = (
            f"/calendars/{identity}/events"
            if self.name == "google_calendar"
            else f"/lists/{identity}/tasks"
        )

        for _ in range(5):
            params = {
                "maxResults": 2500 if self.name == "google_calendar" else 100,
                "showDeleted": "true",
            }

            if self.name == "google_calendar":
                params.update(singleEvents="true", showHiddenInvitations="true")

                if state["full"]:
                    params.update(
                        timeMin=f"{state['bounds'][0]}-01-01T00:00:00Z",
                        timeMax=f"{state['bounds'][1]}-01-01T00:00:00Z",
                    )
                else:
                    params["syncToken"] = state["sync_token"]
            else:
                params.update(showCompleted="true", showHidden="true", showAssigned="true")

                if not state["full"] and state.get("updated_min"):
                    params["updatedMin"] = state["updated_min"]

            if state.get("page_token"):
                params["pageToken"] = state["page_token"]

            try:
                response = self.client.get(self.name, path, params)
            except GoogleUnavailable as error:
                if error.status == 410 or (error.status == 400 and state.get("page_token")):
                    self.advance(
                        key,
                        {
                            **state,
                            "in_progress": False,
                            "sync_token": None,
                            "backfilled": False,
                            "next_due": 0,
                        },
                    )

                raise

            seen = set(state.get("seen", []))
            known = set(state.get("ids", []))

            for raw in response.get("items", []):
                record_identity = record_id(self.name, self.account, container["id"], raw["id"])
                previous = self.store.get("message:" + record_identity)
                item = normalize(self.name, self.account, container, raw, previous)

                if self.name == "google_calendar" and not item.deleted:
                    end = raw.get("end", {})
                    finish = timestamp(
                        end.get("dateTime") or end.get("date") or item.timestamp,
                        end.get("timeZone") or container.get("timeZone") or "UTC",
                    )
                    lower = f"{state['bounds'][0]}-01-01T00:00:00+00:00"
                    upper = f"{state['bounds'][1]}-01-01T00:00:00+00:00"

                    outside = datetime.fromisoformat(item.timestamp) >= datetime.fromisoformat(
                        upper
                    )
                    outside = outside or datetime.fromisoformat(finish) <= datetime.fromisoformat(
                        lower
                    )

                    if outside:
                        if previous and not previous["deleted"]:
                            self.emit(
                                Record.model_validate({**previous, "deleted": True}), "reconcile"
                            )

                        continue

                self.emit(item, "backfill" if state["full"] else "live")
                seen.add(item.id)
                known.add(item.id)

            state.update(
                seen=sorted(seen), ids=sorted(known), page_token=response.get("nextPageToken")
            )

            if state["page_token"]:
                self.advance(key, state)
                continue

            if self.name == "google_calendar" and not response.get("nextSyncToken"):
                raise GoogleUnavailable(200, "Calendar response has no final sync token")

            if state["full"]:
                for missing in known - seen:
                    previous = self.store.get("message:" + missing)

                    if previous and not previous["deleted"]:
                        self.emit(Record.model_validate({**previous, "deleted": True}), "reconcile")

                state["ids"] = sorted(seen)
                state["full_due"] = time.time() + self.settings.source_value(
                    "google_tasks", "reconcile_seconds", 21600
                )

            started = datetime.fromisoformat(state["started_at"])
            state.update(
                sync_token=response.get("nextSyncToken", state.get("sync_token")),
                updated_min=(started - timedelta(minutes=5)).isoformat(),
                backfilled=True,
                in_progress=False,
                seen=[],
                last_success=now(),
                next_due=time.time() + self.settings.source_interval(self.name),
                error=None,
            )
            self.advance(key, state)

            return state

        return state

    def reconcile_containers(self, containers):
        key = "containers:" + self.account
        current = {
            container["id"]: container for container in containers if not container.get("deleted")
        }
        previous = self.checkpoint(key, {})

        for identity in previous.keys() - current.keys():
            scope = self.scope_key(previous[identity])
            state = self.checkpoint(scope, {})

            for record_identity in state.get("ids", []):
                record = self.store.get("message:" + record_identity)

                if record and not record["deleted"]:
                    self.emit(Record.model_validate({**record, "deleted": True}), "reconcile")

            self.advance(scope, {"ids": state.get("ids", []), "backfilled": False})

        self.advance(key, current)

    def cycle(self):
        credentials = self.client.credentials()
        self.account = credentials["account"]
        containers = self.containers()
        self.reconcile_containers(containers)
        scopes = []

        for container in containers:
            if container.get("deleted"):
                continue

            key = self.scope_key(container)

            try:
                state = self.sync_scope(container)
            except GoogleUnavailable as error:
                state = {**self.checkpoint(key, {}), "error": str(error)}
                self.advance(key, state)

            scopes.append(
                {
                    "id": container["id"],
                    "name": container.get("summary") or container.get("title", ""),
                    "backfilled": state.get("backfilled", False),
                    "in_progress": state.get("in_progress", False),
                    "last_success": state.get("last_success"),
                    "records": len(state.get("ids", [])),
                    "error": state.get("error"),
                }
            )

        errors = [scope["error"] for scope in scopes if scope.get("error")]
        complete = all(scope["backfilled"] and not scope["error"] for scope in scopes)
        active = any(scope["in_progress"] for scope in scopes)
        self.status(
            "partial" if errors else "backfilling" if not complete else "watching",
            account=credentials.get("email"),
            scopes=scopes,
            backfill_complete=complete,
            backfill_running=not complete,
            last_success=now()
            if not errors and not active
            else self.store.get("status:" + self.name, {}).get("last_success"),
            error="; ".join(errors) or None,
            poll_seconds=self.settings.source_interval(self.name),
            cache_only=True,
            coverage={
                "calendar_start_year": self.settings.source_value(
                    "google_calendar", "start_year", 1970
                ),
                "calendar_end_year_exclusive": datetime.now(UTC).year
                + self.settings.source_value("google_calendar", "future_years", 5)
                + 1,
                "recurring_instances": True,
            }
            if self.name == "google_calendar"
            else {"completed": True, "hidden": True, "deleted": True, "assigned": True},
        )

        return active and not errors

    async def run(self):
        if self.client.http.is_closed:
            self.client = GoogleClient(self.settings.data_dir / "google")

        try:
            while True:
                delay = self.settings.source_interval(self.name)

                try:
                    if await run_blocking(self.cycle):
                        delay = self.settings.source_value("google_calendar", "backfill_seconds", 1)
                except GoogleAuthenticationRequired as error:
                    self.status("authentication_required", error=str(error))
                    delay = 60
                except GoogleUnavailable as error:
                    self.status("retrying", error=str(error))

                await asyncio.sleep(delay)
        finally:
            self.client.close()
