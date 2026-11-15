import asyncio
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from slack_sdk.errors import SlackApiError

from inboxd.domain.records import now

if TYPE_CHECKING:
    from inboxd.sources.slack.collector import SlackCollector


class SlackBackfill:
    def __init__(self, collector: "SlackCollector") -> None:
        self.collector = collector
        self.api = collector.api
        self.recovery_requested = asyncio.Event()
        self.unavailable = {
            item["channel"]: item
            for item in collector.store.get("status:slack", {}).get("unavailable_channels", [])
        }

    def record_error(self, channel: dict, error: SlackApiError, errors: list[dict]) -> None:
        failure = {"channel": channel["id"], "error": error.response.get("error")}

        if (
            channel.get("is_im")
            and channel.get("user") == "USLACKBOT"
            and failure["error"] == "channel_not_found"
        ):
            self.unavailable[channel["id"]] = failure
            owner = self.collector
            key = f"{owner.team}:{channel['id']}"
            owner.advance(key, {**owner.checkpoint(key, {}), "complete": False})
        else:
            errors.append(failure)

    def request_recovery(self, since: float) -> None:
        previous = self.collector.checkpoint("recovery_since")
        self.collector.advance("recovery_since", min(previous, since) if previous else since)
        self.recovery_requested.set()

    def full_due(self, channel: str) -> bool:
        owner = self.collector
        checkpoint = owner.checkpoint(f"{owner.team}:{channel}", {})
        previous = checkpoint.get("last_full_reconcile", owner.checkpoint("last_full_reconcile", 0))

        return not checkpoint.get("complete") or (
            time.time() - previous
            >= owner.settings.source_value("slack", "full_reconcile_seconds", 604800)
        )

    async def directory(self) -> list[dict]:
        owner = self.collector
        identity = await self.api.call("team_info")
        owner.entity("workspace", owner.team, identity["team"])

        async for page in self.api.pages("users_list", limit=200):
            for user in page.get("members", []):
                owner.entity("user", user["id"], user)

        channels = {}

        async for page in self.api.pages(
            "users_conversations",
            types="public_channel,private_channel,im,mpim",
            limit=200,
            exclude_archived=False,
        ):
            for channel in page.get("channels", []):
                channels[channel["id"]] = channel

        async for page in self.api.pages(
            "conversations_list",
            types="public_channel",
            limit=200,
            exclude_archived=False,
        ):
            for channel in page.get("channels", []):
                channels.setdefault(channel["id"], channel)

        for channel in channels.values():
            previous = owner.store.get(f"message:slack:{owner.team}:channel:{channel['id']}", {})
            owner.entity("channel", channel["id"], {**previous.get("raw", {}), **channel})

        owner.status("reconciling", discovered_channels=len(channels), backfill_running=False)

        return list(channels.values())

    async def channel_details(self, channel_id: str) -> None:
        result = await self.api.call(
            "conversations_info", channel=channel_id, include_num_members=True
        )
        channel = result["channel"]
        members = []

        if channel.get("is_im") and channel.get("user"):
            members = [self.collector.user, channel["user"]]
        else:
            async for page in self.api.pages(
                "conversations_members", channel=channel_id, limit=200
            ):
                members.extend(page.get("members", []))

        channel["member_ids"] = sorted(set(members))
        self.collector.entity("channel", channel_id, channel)

    async def replies(self, channel: str, timestamp: str, origin: str) -> None:
        async for page in self.api.pages(
            "conversations_replies",
            channel=channel,
            ts=timestamp,
            limit=100,
        ):
            for message in page.get("messages", []):
                self.collector.message(channel, message, origin)

    async def history(self, channel: str, full: bool, since: float | None = None) -> None:
        owner = self.collector
        checkpoint_name = f"{owner.team}:{channel}"
        checkpoint = owner.checkpoint(checkpoint_name, {})
        initial = not checkpoint.get("complete", False)
        origin = "backfill" if initial and since is None else "reconcile"
        latest = checkpoint.get("latest", "0")
        oldest = "0" if full or initial else str(max(0, float(latest) - 300))

        if since is not None:
            oldest = str(max(0, since - 300))

        historical = since is None and (full or initial)
        state = "backfilling" if historical else "reconciling"
        mode = "recovery" if since is not None else "full" if historical else "incremental"
        owner.status(state, current_channel=channel, backfill_running=historical, sync_mode=mode)

        async for page in self.api.pages(
            "conversations_history",
            channel=channel,
            oldest=oldest,
            inclusive=True,
            limit=100,
        ):
            for raw in page.get("messages", []):
                owner.message(channel, raw, origin)
                latest = str(max(float(latest), float(raw["ts"])))

                if raw.get("reply_count", 0):
                    await self.replies(channel, raw["ts"], origin)

            owner.status(state, current_channel=channel, last_backfill_page_at=now())

        completed = {
            **checkpoint,
            "latest": checkpoint.get("latest", "0") if since is not None else latest,
            "complete": not initial if since is not None else True,
            "updated_at": now(),
        }

        if historical:
            completed["last_full_reconcile"] = time.time()

        owner.advance(checkpoint_name, completed)
        self.unavailable.pop(channel, None)

    async def recover(self, channels: list[dict]) -> list[dict]:
        owner = self.collector
        since = owner.checkpoint("recovery_since")
        self.recovery_requested.clear()

        if since is None:
            return []

        errors = []

        for channel in channels:
            try:
                await self.history(channel["id"], False, since=since)
            except SlackApiError as error:
                self.record_error(channel, error, errors)

        if not errors and not self.recovery_requested.is_set():
            owner.advance("recovery_since", None)

        previous = owner.store.get("status:slack", {})
        owner.status(
            "partial" if errors else "watching",
            backfill_running=False,
            current_channel=None,
            sync_mode="idle",
            recovery_pending=owner.checkpoint("recovery_since") is not None,
            recovery_errors=errors,
            channel_errors=errors,
            unavailable_channels=list(self.unavailable.values()),
            last_recovery_success=now() if not errors else previous.get("last_recovery_success"),
        )

        return errors

    async def cycle(self, channels: list[dict]) -> None:
        owner = self.collector
        errors = []

        if owner.checkpoint("recovery_since") is not None:
            errors.extend(await self.recover(channels))

        for full in (False, True):
            selected = [channel for channel in channels if self.full_due(channel["id"]) == full]
            completed = 0

            for channel in selected:
                if self.recovery_requested.is_set():
                    errors.extend(await self.recover(channels))

                channel_id = channel["id"]

                try:
                    await self.channel_details(channel_id)
                    await self.history(channel_id, full)
                    completed += 1
                except SlackApiError as error:
                    self.record_error(channel, error, errors)

                owner.status(
                    "backfilling" if full else "reconciling",
                    completed_channels=completed,
                    scheduled_channels=len(selected),
                    channel_errors=errors,
                    rate_limit_waits=self.api.rate_limits,
                )

        next_due = time.time() + owner.settings.source_value("slack", "reconcile_seconds", 3600)
        previous = owner.store.get("status:slack", {})
        owner.advance("next_reconcile", next_due)
        owner.status(
            "partial" if errors else "watching",
            last_success=now() if not errors else previous.get("last_success"),
            backfill_running=False,
            current_channel=None,
            sync_mode="idle",
            channel_errors=errors,
            unavailable_channels=list(self.unavailable.values()),
            next_reconcile_at=datetime.fromtimestamp(next_due, UTC).isoformat(),
            coverage=(
                "Authorized user conversations and public channels; source retention applies"
            ),
        )

    async def run(self) -> None:
        owner = self.collector
        owner.status(
            "reconciling",
            backfill_running=False,
            reconcile_seconds=owner.settings.source_value("slack", "reconcile_seconds", 3600),
            full_reconcile_seconds=owner.settings.source_value(
                "slack", "full_reconcile_seconds", 604800
            ),
        )
        channels = await self.directory()

        while True:
            next_due = owner.checkpoint("next_reconcile", 0)

            if time.time() >= next_due:
                await self.cycle(channels)
            elif self.recovery_requested.is_set():
                await self.recover(channels)

            delay = max(0, owner.checkpoint("next_reconcile", 0) - time.time())

            try:
                await asyncio.wait_for(self.recovery_requested.wait(), timeout=delay)
            except TimeoutError:
                channels = await self.directory()
