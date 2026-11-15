import base64
import binascii
import json
from datetime import UTC, datetime

from inboxd.domain.records import digest
from inboxd.sources.registry import content_kinds


def timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        raise ValueError("Timestamp requires a timezone offset")

    return parsed.astimezone(UTC).isoformat(timespec="microseconds")


def timeline_keys(record: dict, kinds: set[str] | None = None) -> list[str]:
    kinds = content_kinds() if kinds is None else kinds

    if record.get("deleted") or record.get("kind", "message") not in kinds:
        return []

    suffix = timestamp(record["timestamp"]) + ":" + record["id"]

    scopes = ("all", record["source"], "conversation:" + digest(record["conversation_id"]))

    return [f"recent:{scope}:{suffix}" for scope in scopes]


def encode_cursor(key: str, scope: dict) -> str:
    payload = json.dumps({"key": key, "scope": scope}, separators=(",", ":"))

    return base64.urlsafe_b64encode(payload.encode()).decode()


def decode_cursor(cursor: str, scope: dict, prefix: str) -> str:
    try:
        payload = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
        key = payload["key"]

        if payload["scope"] != scope or not isinstance(key, str) or not key.startswith(prefix):
            raise ValueError("Cursor does not belong to this query")

        return key
    except (ValueError, KeyError, TypeError, binascii.Error) as error:
        raise ValueError("Invalid cursor or changed query filters") from error


def summary(record: dict) -> dict:
    fields = (
        "id",
        "source",
        "kind",
        "account",
        "source_id",
        "conversation_id",
        "thread_id",
        "timestamp",
        "sender",
        "recipients",
        "subject",
        "outgoing",
    )
    result = {field: record.get(field) for field in fields}
    result["kind"] = record.get("kind", "message")
    result["text_preview"] = record.get("text", "")[:1000]
    result["text_length"] = len(record.get("text", ""))
    result["text_truncated"] = result["text_length"] > 1000
    result["preview_offset"] = 0
    result["deleted"] = record.get("deleted", False)
    result["attachment_count"] = len(record.get("attachments", []))

    return result
