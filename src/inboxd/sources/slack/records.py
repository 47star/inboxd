import json
from datetime import UTC, datetime

from inboxd.domain.records import Attachment, Record


def message_record(team: str, user: str, channel: str, raw: dict) -> Record:
    timestamp = raw["ts"]
    attachments = [
        Attachment(
            id=file["id"],
            name=file.get("name", file["id"]),
            source_url=file.get("url_private_download") or file.get("url_private"),
            content_type=file.get("mimetype"),
            size=file.get("size"),
        )
        for file in raw.get("files", [])
        if "id" in file
    ]

    return Record(
        id=f"slack:{team}:{channel}:{timestamp}",
        source="slack",
        account=team,
        source_id=timestamp,
        conversation_id=f"slack:{team}:{channel}",
        thread_id=raw.get("thread_ts"),
        timestamp=datetime.fromtimestamp(float(timestamp), UTC).isoformat(),
        sender=raw.get("user") or raw.get("bot_id", ""),
        text=raw.get("text", ""),
        outgoing=raw.get("user") == user,
        attachments=attachments,
        metadata={
            "channel": channel,
            "reactions": raw.get("reactions", []),
            "edited": raw.get("edited"),
            "subtype": raw.get("subtype"),
            "user_entity_id": f"slack:{team}:user:{raw.get('user', '')}",
            "channel_entity_id": f"slack:{team}:channel:{channel}",
        },
        raw=raw,
    )


def entity_record(team: str, kind: str, identity: str, raw: dict) -> Record:
    profile = raw.get("profile", {})
    title = profile.get("display_name") or profile.get("real_name") or raw.get("name") or identity

    return Record(
        id=f"slack:{team}:{kind}:{identity}",
        source="slack",
        account=team,
        source_id=identity,
        conversation_id=f"slack:{team}:{kind}",
        timestamp="1970-01-01T00:00:00+00:00",
        subject=title,
        text=json.dumps(raw, ensure_ascii=False, sort_keys=True),
        kind=kind,
        raw=raw,
    )
