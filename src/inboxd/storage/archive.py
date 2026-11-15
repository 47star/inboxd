from __future__ import annotations

import threading
from pathlib import Path

import rocksdict

from inboxd.domain.query import MatchMode, SearchOrder
from inboxd.domain.records import Message, digest, now
from inboxd.sources.registry import plugins
from inboxd.storage.database import durable_writes, open_database
from inboxd.storage.search import SEARCH_SCHEMA_VERSION, SearchIndex
from inboxd.storage.slack import index_keys as slack_index_keys
from inboxd.storage.timeline import decode_cursor, encode_cursor, summary, timeline_keys, timestamp


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.mkdir(parents=True, exist_ok=True, mode=0o700)

        self.lock = threading.RLock()
        self.index_lock = threading.Lock()
        self.db = open_database(path / "rocksdb")
        self.write_options = durable_writes()
        self.sequence = self.db.get("meta:sequence", 0)
        self.source_descriptors = dict(self.db.get("meta:source_descriptors", {}))

        for name, plugin in plugins().items():
            if not plugin.get("error"):
                previous = self.source_descriptors.get(name, {})
                self.source_descriptors[name] = {
                    "description": plugin["description"],
                    "timestamp_meaning": plugin.get("timestamp_meaning", "source_timestamp"),
                    "content_kinds": sorted(
                        set(previous.get("content_kinds", ()))
                        | set(plugin.get("content_kinds", ()))
                    ),
                }

        self.set("meta:source_descriptors", self.source_descriptors)
        self.content_kinds = {"message"}.union(
            *(item.get("content_kinds", ()) for item in self.source_descriptors.values())
        )

        self.search_index = SearchIndex(path / "index", self.content_kinds)
        self.recover_index()
        self.recover_query_indexes()
        self.recover_record_counts()
        self.recover_conversation_indexes()
        self.recover_content_search()

    def recover_content_search(self) -> None:
        if self.get("meta:content_search") == 1:
            return

        cursor = None

        while rows := self.scan("message:", after=cursor, limit=500):
            batch = rocksdict.WriteBatch()

            for _, record in rows:
                if record.get("kind", "message") not in self.content_kinds:
                    batch["pending:" + record["id"]] = record["id"]

            self.db.write(batch, self.write_options)
            cursor = rows[-1][0]

        self.set("meta:content_search", 1)

    def recover_conversation_indexes(self) -> None:
        if self.get("meta:conversation_indexes") == 1:
            return

        cursor = None

        while rows := self.scan("message:", after=cursor, limit=500):
            batch = rocksdict.WriteBatch()

            for _, record in rows:
                for key in timeline_keys(record, self.content_kinds) + slack_index_keys(record):
                    batch[key] = record["id"]

            self.db.write(batch, self.write_options)
            cursor = rows[-1][0]

        self.set("meta:conversation_indexes", 1)

    def count_category(self, record: dict) -> str:
        if record.get("deleted"):
            return "deleted"

        return "content" if record.get("kind", "message") in self.content_kinds else "metadata"

    def recover_record_counts(self) -> None:
        if self.get("meta:record_counts") == 1:
            return

        counts = {}
        cursor = None

        while rows := self.scan("message:", after=cursor, limit=500):
            for _, record in rows:
                key = f"record-count:{record['source']}:{self.count_category(record)}"
                counts[key] = counts.get(key, 0) + 1

            cursor = rows[-1][0]

        batch = rocksdict.WriteBatch()

        for key, value in counts.items():
            batch[key] = value

        batch["meta:record_counts"] = 1
        self.db.write(batch, self.write_options)

    def recover_query_indexes(self) -> None:
        if self.get("meta:query_indexes") == 1:
            return

        for prefix in ("message:", "event:"):
            cursor = None

            while rows := self.scan(prefix, after=cursor, limit=500):
                batch = rocksdict.WriteBatch()

                for key, value in rows:
                    if prefix == "message:":
                        for index_key in timeline_keys(value, self.content_kinds):
                            batch[index_key] = value["id"]
                    else:
                        batch[
                            f"source-event:{value['message']['source']}:{value['sequence']:020d}"
                        ] = key

                self.db.write(batch, self.write_options)
                cursor = rows[-1][0]

        self.set("meta:query_indexes", 1)

    def recover_index(self) -> None:
        initialized = (
            self.db.get("meta:index_initialized", False)
            and self.db.get("meta:search_schema") == SEARCH_SCHEMA_VERSION
        )

        if initialized and not self.search_index.created:
            return

        cursor = None

        while rows := self.scan("message:", after=cursor, limit=500):
            batch = rocksdict.WriteBatch()

            for _, record in rows:
                batch["pending:" + record["id"]] = record["id"]

            self.db.write(batch, self.write_options)
            cursor = rows[-1][0]

        self.set("meta:index_initialized", True)
        self.set("meta:search_schema", SEARCH_SCHEMA_VERSION)

    def get(self, key: str, default=None):
        with self.lock:
            return self.db.get(key, default)

    def set(self, key: str, value):
        with self.lock:
            batch = rocksdict.WriteBatch()
            batch[key] = value
            self.db.write(batch, self.write_options)

    def scan(self, prefix: str, after: str | None = None, limit: int | None = None):
        with self.lock:
            iterator = self.db.iter()
            iterator.seek(after or prefix)
            rows = []

            while iterator.valid():
                key = iterator.key()

                if not key.startswith(prefix):
                    break

                if after is None or key > after:
                    rows.append((key, iterator.value()))

                    if limit is not None and len(rows) >= limit:
                        break

                iterator.next()

            return rows

    def delete(self, key: str):
        with self.lock:
            batch = rocksdict.WriteBatch()
            batch.delete(key)
            self.db.write(batch, self.write_options)

    def ingest(self, message: Message, origin: str = "live") -> bool:
        value = message.model_dump()
        fingerprint = message.fingerprint()
        key = "message:" + message.id

        with self.lock:
            if self.db.get("fingerprint:" + message.id) == fingerprint:
                return False

            old = self.db.get(key)
            self.sequence += 1
            batch = rocksdict.WriteBatch()
            batch[key] = value
            batch["fingerprint:" + message.id] = fingerprint
            batch["meta:sequence"] = self.sequence
            batch[f"event:{self.sequence:020d}"] = {
                "sequence": self.sequence,
                "received_at": now(),
                "origin": origin,
                "operation": "delete" if message.deleted else "update" if old else "create",
                "message": value,
            }
            batch[f"source-event:{message.source}:{self.sequence:020d}"] = (
                f"event:{self.sequence:020d}"
            )

            if old:
                batch.delete(self.conversation_key(old))

                for index_key in timeline_keys(old, self.content_kinds) + slack_index_keys(old):
                    batch.delete(index_key)

            for index_key in timeline_keys(value, self.content_kinds) + slack_index_keys(value):
                batch[index_key] = message.id

            batch[self.conversation_key(value)] = message.id
            batch["pending:" + message.id] = message.id

            for attachment in message.attachments:
                job_key = "attachment:" + digest(message.id + ":" + attachment.id)
                existing = self.db.get(job_key)
                attachment_value = attachment.model_dump()

                if existing is None or existing.get("attachment") != attachment_value:
                    batch[job_key] = {
                        "message_id": message.id,
                        "source": message.source,
                        "attachment": attachment_value,
                        "state": "pending",
                        "attempts": 0,
                        "retry_at": 0,
                    }

            if old is None:
                count_key = "count:" + message.source
                batch[count_key] = self.db.get(count_key, 0) + 1

            category = self.count_category(value)
            previous_category = self.count_category(old) if old else None

            if category != previous_category:
                count_key = f"record-count:{message.source}:{category}"
                batch[count_key] = self.db.get(count_key, 0) + 1

                if previous_category:
                    previous_key = f"record-count:{message.source}:{previous_category}"
                    batch[previous_key] = self.db.get(previous_key, 0) - 1

            self.db.write(batch, self.write_options)

            return True

    @staticmethod
    def conversation_key(value: dict) -> str:
        return (
            "conversation:"
            + digest(value["conversation_id"])
            + ":"
            + value["timestamp"]
            + ":"
            + value["id"]
        )

    def index_pending(self, limit: int = 500) -> int:
        with self.index_lock:
            with self.lock:
                pending = self.scan("pending:", limit=limit)
                records = [
                    (
                        key,
                        message_id,
                        self.db.get("message:" + message_id),
                        self.db.get("extracted:" + message_id, ""),
                    )
                    for key, message_id in pending
                ]

            if not pending:
                return 0

            for _, message_id, message, attachment_text in records:
                self.search_index.replace(message_id, message, attachment_text)

            self.search_index.commit()

            with self.lock:
                batch = rocksdict.WriteBatch()

                for key, message_id, message, attachment_text in records:
                    if (
                        self.db.get("message:" + message_id) == message
                        and self.db.get("extracted:" + message_id, "") == attachment_text
                    ):
                        batch.delete(key)

                self.db.write(batch, self.write_options)

            return len(pending)

    def search(
        self,
        query: str,
        source: str | None = None,
        sender: str | None = None,
        conversation_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
        order: SearchOrder = "relevance",
        before: str | None = None,
        after: str | None = None,
        match: MatchMode = "all",
    ) -> dict:
        if not 1 <= limit <= 100 or not 0 <= offset <= 1000:
            raise ValueError("limit must be 1-100 and offset must be 0-1000")

        with self.lock:
            filters = {
                "source": source,
                "sender": sender,
                "conversation_id": conversation_id,
            }
            total, hits = self.search_index.find(
                query, filters, limit, offset, order, before, after, match
            )
            messages = []

            for message_id, score in hits:
                message = self.db.get("message:" + message_id)

                if message and not message["deleted"]:
                    messages.append(
                        {**message, **({"score": score} if order == "relevance" else {})}
                    )

            return {
                "total": total,
                "items": messages,
                "offset": offset,
                "order": order,
                "match": match,
            }

    def conversation(self, conversation_id: str, cursor: str | None = None, limit: int = 50):
        prefix = "conversation:" + digest(conversation_id) + ":"

        if cursor and not cursor.startswith(prefix):
            raise ValueError("Cursor does not belong to this conversation")

        rows = self.scan(prefix, after=cursor, limit=min(max(limit, 1), 100) + 1)
        selected = rows[:limit]

        return {
            "items": [self.get("message:" + message_id) for _, message_id in selected],
            "next_cursor": selected[-1][0] if len(rows) > limit else None,
        }

    def changes(self, cursor: int = 0, limit: int = 50, source: str | None = None):
        if source is None:
            rows = self.scan("event:", after=f"event:{cursor:020d}", limit=min(max(limit, 1), 100))
            items = [value for _, value in rows]
        else:
            prefix = f"source-event:{source}:"
            rows = self.scan(prefix, after=f"{prefix}{cursor:020d}", limit=min(max(limit, 1), 100))
            items = [self.get(key) for _, key in rows]

        return {"items": items, "cursor": items[-1]["sequence"] if items else cursor}

    def recent(
        self,
        source: str | None = None,
        before: str | None = None,
        after: str | None = None,
        cursor: str | None = None,
        limit: int = 20,
        conversation_id: str | None = None,
    ) -> dict:
        if source is not None and source not in self.known_sources():
            raise ValueError("Unknown source")

        if not 1 <= limit <= 100:
            raise ValueError("limit must be 1-100")

        before = timestamp(before) if before else None
        after = timestamp(after) if after else None

        if before and after and after > before:
            raise ValueError("after must not exceed before")

        scope = {"source": source, "before": before, "after": after}

        if conversation_id:
            scope["conversation_id"] = conversation_id

        prefix = (
            f"recent:conversation:{digest(conversation_id)}:"
            if conversation_id
            else f"recent:{source or 'all'}:"
        )
        upper = prefix + (before + ":" if before else "") + "\uffff"
        lower = prefix + after + ":" if after else prefix
        position = decode_cursor(cursor, scope, prefix) if cursor else upper
        selected = []

        with self.lock:
            iterator = self.db.iter()
            iterator.seek_for_prev(position)

            while iterator.valid() and len(selected) <= limit:
                key = iterator.key()

                if not key.startswith(prefix) or key < lower:
                    break

                if key <= upper and (not cursor or key < position):
                    record = self.db.get("message:" + iterator.value())

                    if source is None or record["source"] == source:
                        selected.append((key, summary(record)))

                iterator.prev()

        has_more = len(selected) > limit
        selected = selected[:limit]

        return {
            "items": [value for _, value in selected],
            "next_cursor": encode_cursor(selected[-1][0], scope) if has_more else None,
            "source": source,
            "conversation_id": conversation_id,
            "order": "source_timestamp_desc",
        }

    def sources(self, source_filter: str | None = None) -> dict:
        items = []
        available = plugins()

        if source_filter and source_filter not in self.known_sources():
            raise ValueError("Unknown source")

        selected = [source_filter] if source_filter else sorted(self.known_sources())

        for source in selected:
            descriptor = self.source_descriptors.get(source, available.get(source, {}))
            state = self.get("status:" + source, {"state": "not_started"})

            if source not in available:
                state = {**state, "state": "plugin_removed", "enabled": False}

            latest = self.recent(source=source, limit=1)["items"]
            items.append(
                {
                    "source": source,
                    "description": descriptor.get("description", "Archived source"),
                    "stored_records": self.get("count:" + source, 0),
                    "content_records": self.get(f"record-count:{source}:content", 0),
                    "metadata_records": self.get(f"record-count:{source}:metadata", 0),
                    "deleted_records": self.get(f"record-count:{source}:deleted", 0),
                    "latest_content_timestamp": latest[0]["timestamp"] if latest else None,
                    "timestamp_meaning": descriptor.get("timestamp_meaning", "source_timestamp"),
                    "recent_request": {
                        "tool": "get_recent_records",
                        "arguments": {"source": source, "limit": 20},
                    },
                    "collector": state,
                }
            )

        return {"items": items}

    def known_sources(self) -> set[str]:
        return (
            set(plugins())
            | set(self.source_descriptors)
            | {key.removeprefix("count:") for key, _ in self.scan("count:")}
        )

    def status(self):
        return {
            "sequence": self.sequence,
            "counts": {key.removeprefix("count:"): value for key, value in self.scan("count:")},
            "collectors": {
                key.removeprefix("status:"): value for key, value in self.scan("status:")
            },
            "pending_index": len(self.scan("pending:", limit=10001)),
            "diagnostics": self.get("daemon:diagnostics"),
            "timestamp": now(),
        }

    def close(self):
        with self.lock:
            self.search_index.close()
            self.db.flush()
            self.db.close()
