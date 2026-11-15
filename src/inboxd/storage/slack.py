import re
import unicodedata
from typing import TYPE_CHECKING, Literal

from inboxd.domain.records import digest
from inboxd.storage.timeline import decode_cursor, encode_cursor, summary, timestamp

if TYPE_CHECKING:
    from inboxd.storage.archive import Store


ConversationKind = Literal["public_channel", "private_channel", "dm", "group_dm"]
SLACK_TIMESTAMP = re.compile(r"^[0-9]+\.[0-9]+$")


def thread_prefix(conversation_id: str, thread_id: str) -> str:
    return "slack-thread:" + digest(conversation_id + ":" + thread_id) + ":"


def index_keys(record: dict) -> list[str]:
    if record["source"] != "slack":
        return []

    kind = record.get("kind", "message")

    if kind in {"channel", "user"}:
        return [f"slack-entity:{kind}:{record['account']}:{record['source_id']}"]

    if kind != "message":
        return []

    root = record.get("thread_id") or record["source_id"]
    prefix = thread_prefix(record["conversation_id"], root)

    return [prefix + timestamp(record["timestamp"]) + ":" + record["id"]]


def folded(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


def conversation_kind(raw: dict) -> ConversationKind:
    if raw.get("is_im"):
        return "dm"

    if raw.get("is_mpim"):
        return "group_dm"

    return "private_channel" if raw.get("is_private") else "public_channel"


class SlackQueries:
    def __init__(self, store: "Store") -> None:
        self.store = store

    def person(self, team: str, user_id: str) -> dict:
        record = self.store.get(f"message:slack:{team}:user:{user_id}", {})
        raw = record.get("raw", {})
        profile = raw.get("profile", {})

        return {
            "id": user_id,
            "name": profile.get("display_name")
            or profile.get("real_name")
            or raw.get("real_name")
            or raw.get("name")
            or user_id,
            "username": raw.get("name"),
            "real_name": profile.get("real_name") or raw.get("real_name"),
            "email": profile.get("email"),
        }

    def conversations(
        self,
        query: str = "",
        kind: ConversationKind | None = None,
        user_id: str | None = None,
        account: str | None = None,
        include_archived: bool = False,
        cursor: str | None = None,
        limit: int = 20,
    ) -> dict:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be 1-100")

        prefix = "slack-entity:channel:"
        scope = {
            "query": query,
            "kind": kind,
            "user_id": user_id,
            "account": account,
            "include_archived": include_archived,
        }
        position = decode_cursor(cursor, scope, prefix) if cursor else None
        terms = folded(query.lstrip("#@")).split()
        matches = []

        while rows := self.store.scan(prefix, after=position, limit=100):
            for key, record_id in rows:
                position = key
                record = self.store.get("message:" + record_id)
                raw = record["raw"]
                team = record["account"]
                channel = record["source_id"]
                category = conversation_kind(raw)

                if record.get("deleted") or (account and team != account):
                    continue

                if kind and category != kind:
                    continue

                if raw.get("is_archived") and not include_archived:
                    continue

                members = set(raw.get("member_ids", raw.get("members", [])))

                if raw.get("user"):
                    members.add(raw["user"])

                if user_id and user_id not in members:
                    continue

                people = [self.person(team, member) for member in sorted(members)]
                names = " ".join(str(value or "") for person in people for value in person.values())
                searchable = folded(" ".join((channel, raw.get("name", ""), names)))

                if not all(term in searchable for term in terms):
                    continue

                conversation_id = f"slack:{team}:{channel}"
                latest = self.store.recent(
                    source="slack", conversation_id=conversation_id, limit=1
                )["items"]
                display = raw.get("name") or channel

                if category in {"dm", "group_dm"} and people:
                    display = ", ".join(person["name"] for person in people)

                matches.append(
                    (
                        key,
                        {
                            "conversation_id": conversation_id,
                            "channel_id": channel,
                            "account": team,
                            "kind": category,
                            "name": display,
                            "slack_name": raw.get("name"),
                            "archived": bool(raw.get("is_archived")),
                            "members": people[:50],
                            "known_member_count": len(people),
                            "membership_collected": "member_ids" in raw
                            or category == "dm"
                            and bool(raw.get("user")),
                            "latest_message_timestamp": latest[0]["timestamp"] if latest else None,
                            "backfill": self.store.get(
                                f"checkpoint:slack:{team}:{channel}", {"complete": False}
                            ),
                        },
                    )
                )

                if len(matches) > limit:
                    break

            if len(matches) > limit:
                break

        has_more = len(matches) > limit
        selected = matches[:limit]

        return {
            "items": [item for _, item in selected],
            "next_cursor": encode_cursor(selected[-1][0], scope) if has_more else None,
            "coverage": (
                "Collected Slack channel and user metadata; "
                "unknown membership cannot match a member filter"
            ),
        }

    def thread(
        self, conversation_id: str, thread_id: str, cursor: str | None = None, limit: int = 50
    ) -> dict:
        parts = conversation_id.split(":")

        if len(parts) != 3 or parts[0] != "slack" or not all(parts[1:]):
            raise ValueError("Use the Slack conversation_id returned by list_slack_conversations")

        if not SLACK_TIMESTAMP.fullmatch(thread_id):
            raise ValueError("thread_id must be a Slack root timestamp, not a local message ID")

        if not 1 <= limit <= 100:
            raise ValueError("limit must be 1-100")

        prefix = thread_prefix(conversation_id, thread_id)
        scope = {"conversation_id": conversation_id, "thread_id": thread_id}
        position = decode_cursor(cursor, scope, prefix) if cursor else None
        rows = self.store.scan(prefix, after=position, limit=limit + 1)
        selected = rows[:limit]
        items = []

        for _, identity in selected:
            record = self.store.get("message:" + identity)
            item = summary(record)
            item.pop("text_preview")
            item["deleted"] = record["deleted"]
            item["text"] = "" if record["deleted"] else record["text"]
            item["attachments"] = record["attachments"]
            items.append(item)

        root_id = conversation_id + ":" + thread_id
        root = self.store.get("message:" + root_id)

        return {
            "conversation_id": conversation_id,
            "thread_id": thread_id,
            "root_message_id": root_id,
            "root_available": root is not None,
            "reported_reply_count": root.get("raw", {}).get("reply_count") if root else None,
            "items": items,
            "next_cursor": encode_cursor(selected[-1][0], scope) if len(rows) > limit else None,
            "order": "source_timestamp_asc",
            "coverage": (
                "Locally collected root and replies; "
                "pagination exhaustion does not prove source completeness"
            ),
        }
