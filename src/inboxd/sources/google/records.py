from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from inboxd.domain.records import Record, digest
from inboxd.domain.text import plain_html


def timestamp(value: str | None, zone="UTC") -> str:
    if not value:
        return "1970-01-01T00:00:00+00:00"

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(zone))

    return parsed.astimezone(UTC).isoformat()


def record_id(source, account, container, identity):
    return f"{source}:{account}:{digest(container)}:{identity}"


def normalize(source, account, container, raw, previous=None):
    if previous and (raw.get("status") == "cancelled" or raw.get("deleted")):
        raw = {**previous.get("raw", {}), **raw}

    identity = record_id(source, account, container["id"], raw["id"])
    fields = dict(raw)
    fields["container_id"] = container["id"]
    fields["container_name"] = container.get("summary") or container.get("title", "")

    if source == "google_calendar":
        start = raw.get("start", {})
        end = raw.get("end", {})
        zone = start.get("timeZone") or container.get("timeZone") or "UTC"
        when = timestamp(start.get("dateTime") or start.get("date") or raw.get("updated"), zone)
        deleted = raw.get("status") == "cancelled"
        title = raw.get("summary", "")
        sender = raw.get("organizer", {}).get("email", "")
        recipients = sorted(item["email"] for item in raw.get("attendees", []) if item.get("email"))
        body = "\n".join(
            value
            for value in [
                plain_html(raw.get("description", "")),
                "Location: " + raw.get("location", "") if raw.get("location") else "",
                "Start: " + (start.get("dateTime") or start.get("date", "")),
                "End: " + (end.get("dateTime") or end.get("date", "")),
                "Status: " + raw.get("status", ""),
            ]
            if value
        )
        kind = "calendar"
    else:
        when = timestamp(raw.get("updated"))
        deleted = raw.get("deleted", False)
        title = raw.get("title", "")
        sender = ""
        recipients = []
        body = "\n".join(
            value
            for value in [
                raw.get("notes", ""),
                "Status: " + raw.get("status", ""),
                "Due: " + raw["due"][:10] if raw.get("due") else "",
                "Completed: " + raw["completed"] if raw.get("completed") else "",
            ]
            if value
        )
        kind = "task"

    return Record(
        id=identity,
        source=source,
        account=account,
        conversation_id=f"{source}:{account}:{digest(container['id'])}",
        source_id=raw["id"],
        timestamp=previous["timestamp"] if deleted and previous else when,
        subject=title,
        text=body,
        sender=sender,
        recipients=recipients,
        deleted=deleted,
        kind=kind,
        metadata={"google": fields},
        raw=raw,
    )
