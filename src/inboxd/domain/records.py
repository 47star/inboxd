import json
from datetime import UTC, datetime
from hashlib import sha256

from pydantic import BaseModel, Field


def now() -> str:
    return datetime.now(UTC).isoformat()


def digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


class Attachment(BaseModel):
    id: str
    name: str
    source_path: str | None = None
    source_url: str | None = None
    content_type: str | None = None
    size: int | None = None


class Record(BaseModel):
    id: str
    source: str
    account: str
    conversation_id: str
    source_id: str
    timestamp: str
    sender: str = ""
    recipients: list[str] = Field(default_factory=list)
    subject: str = ""
    text: str = ""
    thread_id: str | None = None
    outgoing: bool = False
    deleted: bool = False
    attachments: list[Attachment] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)
    kind: str = "message"
    raw: dict = Field(default_factory=dict)

    def fingerprint(self) -> str:
        return digest(json.dumps(self.model_dump(), ensure_ascii=False, sort_keys=True))


Message = Record
