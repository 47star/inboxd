import asyncio
import logging
import time
from datetime import UTC, datetime

from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse

from inboxd.domain.records import Record, now
from inboxd.ingestion.collector import Collector, MissingConfiguration
from inboxd.runtime.config import load_secrets
from inboxd.sources.slack.backfill import SlackBackfill
from inboxd.sources.slack.client import SlackReader
from inboxd.sources.slack.records import entity_record, message_record

logger = logging.getLogger(__name__)


class SlackCollector(Collector):
    name = "slack"

    def entity(self, kind: str, identity: str, raw: dict) -> None:
        self.emit(entity_record(self.team, kind, identity, raw), "metadata")

    def message(self, channel: str, raw: dict, origin: str) -> None:
        if raw.get("subtype") == "message_deleted":
            self.mark_deleted(channel, raw["deleted_ts"])

            return

        if raw.get("subtype") == "message_changed":
            raw = raw["message"]

        if not raw.get("ts"):
            raise ValueError("Slack message has no timestamp")

        record = message_record(self.team, self.user, channel, raw)
        self.emit(record, origin)

        for file in raw.get("files", []):
            if "id" in file:
                self.entity("file", file["id"], file)

    def mark_deleted(self, channel: str, timestamp: str) -> None:
        identity = f"slack:{self.team}:{channel}:{timestamp}"
        previous = self.store.get("message:" + identity)

        if previous:
            record = Record.model_validate({**previous, "deleted": True})
        else:
            record = Record(
                id=identity,
                source=self.name,
                account=self.team,
                source_id=timestamp,
                conversation_id=f"slack:{self.team}:{channel}",
                timestamp=datetime.fromtimestamp(float(timestamp), UTC).isoformat(),
                deleted=True,
            )

        self.emit(record, "live")

    async def receive(self, client, request) -> None:
        if request.type == "events_api":
            payload = request.payload
            identity = payload.get("event_id") or request.envelope_id

            if not self.store.get("slack-processed:" + identity):
                self.store.set("slack-raw:" + identity, payload)
                self.store.set("slack-event:" + identity, payload)

        await client.send_socket_mode_response(
            SocketModeResponse(envelope_id=request.envelope_id),
        )

    async def refresh_message(self, channel: str, timestamp: str) -> None:
        async for page in self.api.pages(
            "conversations_replies",
            channel=channel,
            ts=timestamp,
            limit=100,
        ):
            for raw in page.get("messages", []):
                self.message(channel, raw, "live")

    async def apply_event(self, event: dict) -> None:
        event_type = event.get("type", "")

        if event_type == "message":
            self.message(event["channel"], event, "live")

            return

        if event_type in {"user_change", "team_join"}:
            self.entity("user", event["user"]["id"], event["user"])

            return

        if event_type in {"reaction_added", "reaction_removed"}:
            item = event.get("item", {})

            if item.get("type") == "message":
                await self.refresh_message(item["channel"], item["ts"])

            return

        if event_type in {"file_change", "file_shared", "file_deleted"}:
            await self.apply_file_event(event)

            return

        channel = event.get("channel")
        channel_id = channel.get("id") if isinstance(channel, dict) else channel

        if channel_id and event_type in {
            "channel_rename",
            "group_rename",
            "member_joined_channel",
            "member_left_channel",
        }:
            await self.backfill.channel_details(channel_id)

    async def apply_file_event(self, event: dict) -> None:
        file = event.get("file", {})
        identity = event.get("file_id") or (file.get("id") if isinstance(file, dict) else file)

        if not identity:
            return

        if event["type"] == "file_deleted":
            record_id = f"slack:{self.team}:file:{identity}"
            previous = self.store.get("message:" + record_id)

            if previous:
                self.emit(Record.model_validate({**previous, "deleted": True}), "live")

            return

        response = await self.api.call("files_info", file=identity)
        self.entity("file", identity, response["file"])

    async def drain(self) -> None:
        while True:
            for key, payload in self.store.scan("slack-event:", limit=100):
                identity = key.removeprefix("slack-event:")

                try:
                    await self.apply_event(payload.get("event", {}))
                except SlackApiError as error:
                    if error.response.get("error") not in {
                        "message_not_found",
                        "file_not_found",
                        "channel_not_found",
                    }:
                        raise

                    self.store.set(
                        "slack-unresolved:" + identity,
                        {
                            "error": error.response.get("error"),
                            "raw_event_key": "slack-raw:" + identity,
                        },
                    )
                except (KeyError, ValueError, TypeError) as error:
                    logger.exception("Slack event normalization failed: %s", identity)
                    self.store.set(
                        "slack-unresolved:" + identity,
                        {
                            "error": str(error),
                            "raw_event_key": "slack-raw:" + identity,
                        },
                    )

                self.store.set("slack-processed:" + identity, now())
                self.store.delete(key)
                self.status_event()

            await asyncio.sleep(self.settings.source_value("slack", "event_poll_seconds", 0.2))

    def status_event(self) -> None:
        state = self.store.get("status:slack", {}).get("state", "watching")
        self.status(state, socket_connected=True, last_event_at=now())

    async def connection_health(self, socket: SocketModeClient) -> None:
        previous_session = socket.current_session
        last_connected = time.time()
        was_connected = True

        while True:
            connected = await socket.is_connected()
            current = self.store.get("status:slack", {}).get("state", "watching")

            if connected:
                if not was_connected or socket.current_session is not previous_session:
                    self.backfill.request_recovery(last_connected)

                last_connected = time.time()
                previous_session = socket.current_session

            was_connected = connected
            self.status(
                current,
                socket_connected=connected,
                socket_checked_at=now(),
                socket_last_connected_at=last_connected,
            )

            await asyncio.sleep(self.settings.source_value("slack", "connection_check_seconds", 10))

    async def run(self) -> None:
        credentials = load_secrets(self.settings)
        app_token = credentials.get("slack_app_token")
        user_token = credentials.get("slack_user_token")

        if not app_token or not user_token:
            raise MissingConfiguration("Slack app token and authorized user token are required")

        self.api = SlackReader(user_token)
        identity = await self.api.call("auth_test")

        if identity.get("bot_id"):
            raise MissingConfiguration("Slack collection requires a user token, not a bot token")

        self.team = identity["team_id"]
        self.user = identity["user_id"]
        self.backfill = SlackBackfill(self)
        previous = self.store.get("status:slack", {})
        since = previous.get("socket_last_connected_at")

        if since is None:
            checked = previous.get("socket_checked_at") or previous.get("last_success")
            since = datetime.fromisoformat(checked).timestamp() if checked else time.time()

        self.backfill.request_recovery(since)
        socket = SocketModeClient(app_token=app_token, web_client=self.api.client)
        socket.socket_mode_request_listeners.append(self.receive)

        try:
            await socket.connect()
            self.status("reconciling", socket_connected=True, backfill_running=False)

            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self.drain())
                tasks.create_task(self.backfill.run())
                tasks.create_task(self.connection_health(socket))
        finally:
            await socket.close()
