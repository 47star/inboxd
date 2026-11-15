from pydantic import BaseModel, Field

from inboxd.domain.records import Record


class WorkItem(BaseModel):
    external_id: str
    title: str
    description: str
    url: str
    updated_at: str
    created_at: str | None = None
    project_name: str | None = None
    tracker_name: str | None = None
    status_name: str | None = None
    author_name: str | None = None
    fields: dict = Field(default_factory=dict)
    schema_version: int = 2
    description_format: str = "plain_text"

    def record(self, source: str, account: str) -> Record:
        identity = f"{source}:{account}:issue:{self.external_id}"
        notes = [
            journal["notes"] for journal in self.fields.get("journals", []) if journal.get("notes")
        ]
        text = "\n\n".join([self.description, *notes])

        return Record(
            id=identity,
            source=source,
            account=account,
            source_id=self.external_id,
            conversation_id=identity,
            timestamp=self.updated_at,
            subject=self.title,
            text=text,
            kind="work_item",
            metadata={"work_item": self.model_dump()},
        )
