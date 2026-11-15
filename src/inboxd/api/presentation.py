import re
from email.header import decode_header, make_header
from email.utils import getaddresses

from inboxd.api.query_context import query_context
from inboxd.domain.query import search_terms
from inboxd.domain.search_content import readable_body, search_content
from inboxd.storage.archive import Store
from inboxd.storage.timeline import summary


class RecordPresenter:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.names: dict[tuple[str, str], str | None] = {}
        self.conversations: dict[str, str | None] = {}

    def record(self, record: dict) -> dict:
        if record.get("source") in {"google_calendar", "google_tasks"}:
            original = self.store.get("message:" + record["id"], record)
            fields = original.get("metadata", {}).get("google", {})

            return {
                **record,
                "conversation_name": fields.get("container_name"),
                "google": {
                    key: fields.get(key)
                    for key in (
                        "container_name",
                        "htmlLink",
                        "webViewLink",
                        "status",
                        "updated",
                        "start",
                        "end",
                        "due",
                        "completed",
                        "iCalUID",
                    )
                    if fields.get(key) is not None
                },
            }

        original = self.store.get("message:" + record["id"], record)
        source_metadata = original.get("metadata", {}).get(record.get("source"), {})

        if source_metadata.get("url") and source_metadata.get("status_name") is not None:
            return {
                **record,
                "sender_name": source_metadata.get("author_name"),
                "conversation_name": source_metadata.get("container_name"),
                record.get("source"): {
                    "type": record.get("kind"),
                    **{
                        key: source_metadata.get(key)
                        for key in (
                            "url",
                            "author_name",
                            "container_name",
                            "status_name",
                            "created_at",
                            "updated_at",
                        )
                    },
                    "comment_count": len(source_metadata.get("comments", [])),
                },
            }

        if record.get("kind") == "work_item":
            original = self.store.get("message:" + record["id"], record)
            item = original.get("metadata", {}).get("work_item", {})
            fields = item.get("fields", {})
            access = (
                self.store.get(f"redmine-access:{record['account']}:{record['source_id']}", {})
                if record.get("source") == "redmine"
                else {}
            )

            return {
                **record,
                "sender_name": item.get("author_name"),
                "conversation_name": item.get("title"),
                "work_item": {
                    **{
                        key: item.get(key)
                        for key in (
                            "external_id",
                            "url",
                            "project_name",
                            "tracker_name",
                            "status_name",
                            "updated_at",
                            "created_at",
                            "description_format",
                        )
                    },
                    "assigned_to": fields.get("assigned_to"),
                    "priority": fields.get("priority"),
                    "done_ratio": fields.get("done_ratio"),
                    "parent": fields.get("parent"),
                    "relation_count": len(fields.get("relations", [])),
                    "metadata_complete": item.get("schema_version", 1) >= 2,
                    "access": access,
                },
            }

        if record.get("source") != "slack" or record.get("kind", "message") != "message":
            original = (
                record if "raw" in record else self.store.get("message:" + record["id"], record)
            )
            raw = original.get("raw", {})
            metadata = original.get("metadata", {})
            name = None

            if record.get("source") == "mail":
                headers = [value for key, value in raw.get("headers", []) if key.lower() == "from"]
                names = [value for value, _ in getaddresses(headers) if value]

                try:
                    name = (
                        ", ".join(str(make_header(decode_header(value))) for value in names) or None
                    )
                except (LookupError, UnicodeError):
                    name = ", ".join(names) or None

            title = raw.get("calendar") or raw.get("folder") or record.get("subject") or None

            if record.get("source") == "messages":
                title = next(
                    (
                        chat.get("display_name")
                        for chat in metadata.get("chats", [])
                        if chat.get("display_name")
                    ),
                    None,
                )

            return {**record, "sender_name": name, "conversation_name": title}

        team = record["account"]
        sender = record.get("sender", "")
        key = (team, sender)

        if key not in self.names:
            user = self.store.get(f"message:slack:{team}:user:{sender}", {}).get("raw", {})
            profile = user.get("profile", {})
            self.names[key] = (
                profile.get("display_name")
                or profile.get("real_name")
                or user.get("real_name")
                or user.get("name")
                or None
            )

        name = self.names[key]

        if name is None:
            original = record if "raw" in record else self.store.get("message:" + record["id"], {})
            raw = original.get("raw", {})
            embedded = raw.get("user_profile") or raw.get("bot_profile") or {}
            name = embedded.get("display_name") or embedded.get("real_name") or embedded.get("name")

        conversation_id = record["conversation_id"]

        if conversation_id not in self.conversations:
            channel_id = conversation_id.rsplit(":", 1)[-1]
            channel = self.store.get(f"message:slack:{team}:channel:{channel_id}", {}).get(
                "raw", {}
            )
            title = channel.get("name")

            if channel.get("is_im") and channel.get("user"):
                user = self.store.get(f"message:slack:{team}:user:{channel['user']}", {}).get(
                    "raw", {}
                )
                profile = user.get("profile", {})
                title = (
                    profile.get("display_name")
                    or profile.get("real_name")
                    or user.get("name")
                    or title
                )

            self.conversations[conversation_id] = title

        return {
            **record,
            "sender_name": name,
            "conversation_name": self.conversations[conversation_id],
            "thread_id": record.get("thread_id") or record["source_id"],
        }

    def preview(self, record: dict, query: str = "") -> dict:
        if "text" not in record and record.get("kind") == "work_item":
            record = self.store.get("message:" + record["id"], record)

        value = (
            summary({**record, "text": readable_body(record)}) if "text" in record else dict(record)
        )

        if "score" in record:
            value["score"] = record["score"]

        if query and "text" in record:
            text = readable_body(record)
            terms = search_terms(query)
            content = search_content(record, self.store.get("extracted:" + record["id"], ""))
            value["matched_fields"] = [
                field
                for field, body in content.items()
                if any(re.search(re.escape(term), body, re.IGNORECASE) for term in terms)
            ]
            value["preview_field"] = "text"

            if "attachment" in value["matched_fields"]:
                value["attachments_request"] = {
                    "tool": "get_attachments",
                    "arguments": {"message_id": record["id"]},
                }

            if value["matched_fields"] == ["attachment"]:
                text = content["attachment"]
                value["preview_field"] = "attachment"

            positions = [
                found.start()
                for term in terms
                if (found := re.search(re.escape(term), text, re.IGNORECASE))
            ]
            start = max(0, min(positions) - 150) if positions else 0
            value["text_preview"] = text[start : start + 1000]
            value["preview_offset"] = start
            value["text_truncated"] = start > 0 or start + 1000 < len(text)
            value["preview_text_length"] = len(text)

        value = self.record(value)
        value["read_request"] = {"tool": "get_message", "arguments": {"id": record["id"]}}

        return value

    def detail(
        self, record: dict, offset: int, length: int, include_metadata: bool = False
    ) -> dict:
        text = readable_body(record)
        source_metadata = record.get("metadata", {}).get(record.get("source"), {})
        value = self.record({key: item for key, item in record.items() if key != "raw"})

        if (
            record.get("kind") == "work_item"
            or source_metadata.get("url")
            and source_metadata.get("status_name") is not None
            or record.get("source")
            in {
                "google_calendar",
                "google_tasks",
            }
        ):
            value["text_format"] = "plain_text"
            value["metadata_included"] = include_metadata

            if not include_metadata:
                value.pop("metadata", None)
                value["metadata_request"] = {
                    "tool": "get_message",
                    "arguments": {"id": record["id"], "length": 1, "include_metadata": True},
                }

        if record["source"] == "messages":
            value["metadata"] = {
                key: item
                for key, item in record.get("metadata", {}).items()
                if key in {"service", "body_decode_status", "date_edited", "date_retracted"}
            }

        value["text"] = text[offset : offset + length]
        value["text_length"] = len(text)
        value["text_offset"] = offset
        value["text_truncated"] = offset > 0 or offset + length < len(text)
        next_offset = offset + length if offset + length < len(text) else None
        value["next_offset"] = next_offset
        value["next_request"] = (
            {
                "tool": "get_message",
                "arguments": {
                    "id": record["id"],
                    "offset": next_offset,
                    "length": length,
                    "include_metadata": include_metadata,
                },
            }
            if next_offset is not None
            else None
        )

        return value

    def page(self, page: dict, source: str | None = None, query: str = "") -> dict:
        items = [self.preview(record, query) for record in page["items"]]

        return {
            **page,
            "items": items,
            "context": query_context(
                self.store, source, {item["source"] for item in items}, not items
            ),
        }

    def changes(self, page: dict) -> dict:
        return {
            **page,
            "items": [
                {**event, "message": self.preview(event["message"])} for event in page["items"]
            ],
        }
