import asyncio
import fcntl
import hmac
import logging
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from starlette.middleware.trustedhost import TrustedHostMiddleware

from inboxd.api.presentation import RecordPresenter
from inboxd.domain.query import MatchMode, SearchOrder
from inboxd.domain.records import now
from inboxd.domain.sources import Source
from inboxd.ingestion.attachments import AttachmentWorker
from inboxd.ingestion.collector import run_blocking
from inboxd.runtime.config import Settings
from inboxd.sources.registry import plugins
from inboxd.storage.archive import Store
from inboxd.storage.slack import ConversationKind, SlackQueries

logger = logging.getLogger(__name__)


def token_for(path: Path) -> str:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    token_path = path / "api-token"

    try:
        descriptor = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return token_path.read_text().strip()

    token = secrets.token_urlsafe(32)

    with os.fdopen(descriptor, "w") as stream:
        stream.write(token)

    return token


def create_app(settings: Settings) -> FastAPI:
    token = token_for(settings.data_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        lock_file = (settings.data_dir / "daemon.lock").open("w")

        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock_file.close()
            raise RuntimeError("Another inboxd instance owns this data directory") from None

        available_plugins = plugins()
        store = Store(settings.data_dir)
        app.state.store = store
        store.set("daemon:started_at", now())
        store.set(
            "daemon:diagnostics",
            {
                "path": str(settings.data_dir / "logs/diagnostics.jsonl"),
                "format": "jsonl",
                "rotation": "daily_utc",
                "retention_days": settings.diagnostic_retention_days,
            },
        )
        logger.info(
            "Daemon started",
            extra={"event": "daemon_started", "details": {"sequence": store.sequence}},
        )
        collectors = []

        for source in available_plugins:
            previous = store.get("status:" + source, {})
            enabled = settings.source_enabled(source)
            state = {
                **previous,
                "enabled": enabled,
                "state": "plugin_unavailable"
                if available_plugins[source].get("error")
                else "starting"
                if enabled
                else "disabled",
                "updated_at": now(),
                "error": available_plugins[source].get("error"),
                "backfill_running": False,
            }

            if not enabled:
                state["disabled_at"] = now()

                if source == "slack":
                    state["socket_connected"] = False

            store.set("status:" + source, state)

        for source, plugin in available_plugins.items():
            if settings.source_enabled(source):
                try:
                    collectors.append(plugin["collector"](settings, store))
                except Exception as error:
                    logger.exception(
                        "Collector %s initialization failed: %s", source, type(error).__name__
                    )
                    store.set(
                        "status:" + source,
                        {
                            **store.get("status:" + source, {}),
                            "state": "initialization_failed",
                            "error": type(error).__name__,
                            "updated_at": now(),
                        },
                    )

        async def index_worker():
            while True:
                try:
                    count = await run_blocking(store.index_pending)
                    store.set(
                        "status:index", {"state": "ready", "last_success": now(), "batch": count}
                    )
                except Exception as error:
                    logger.exception("Search indexing failed")
                    store.set(
                        "status:index",
                        {"state": "retrying", "error": str(error), "updated_at": now()},
                    )
                    await asyncio.sleep(3)

                await asyncio.sleep(0.5)

        async def diagnostics_worker():
            while True:
                status = await run_blocking(store.status)
                logger.info(
                    "Archive health snapshot",
                    extra={
                        "event": "health_snapshot",
                        "details": {
                            "sequence": status["sequence"],
                            "counts": status["counts"],
                            "pending_index": status["pending_index"],
                            "collectors": {
                                name: {
                                    key: value
                                    for key, value in state.items()
                                    if key
                                    in {
                                        "state",
                                        "error",
                                        "last_success",
                                        "socket_connected",
                                        "completed_channels",
                                        "discovered_channels",
                                        "backfill_running",
                                        "backfill_complete",
                                        "backfilled",
                                        "mode",
                                        "feed_received",
                                        "feed_changed",
                                        "feed_pages",
                                        "metadata_backfill_complete",
                                        "metadata_backfilled",
                                        "metadata_changed",
                                        "activity_changed",
                                        "rate_limit_waits",
                                        "changed",
                                        "scanned",
                                        "pending_deletions",
                                    }
                                }
                                for name, state in status["collectors"].items()
                            },
                        },
                    },
                )
                await asyncio.sleep(60)

        tasks = [
            asyncio.create_task(collector.supervise(), name=collector.name)
            for collector in collectors
        ]
        tasks.extend(
            [
                asyncio.create_task(index_worker(), name="index"),
                asyncio.create_task(AttachmentWorker(settings, store).run(), name="attachments"),
                asyncio.create_task(diagnostics_worker(), name="diagnostics"),
            ]
        )

        try:
            yield
        finally:
            logger.info("Daemon shutdown started", extra={"event": "daemon_stopping"})

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.to_thread(store.close)
            lock_file.close()
            logger.info("Daemon shutdown completed", extra={"event": "daemon_stopped"})

    app = FastAPI(
        title="inboxd", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )
    app.add_middleware(
        TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "testserver"]
    )

    @app.middleware("http")
    async def local_auth(request: Request, call_next):
        from starlette.responses import JSONResponse

        supplied = request.headers.get("authorization", "").removeprefix("Bearer ")

        if not hmac.compare_digest(supplied, token):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)

        origin = request.headers.get("origin")

        if origin:
            return JSONResponse(
                {"detail": "Browser-origin requests are not accepted"}, status_code=403
            )

        started = time.perf_counter()
        request_id = uuid.uuid4().hex

        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "Local API request failed",
                extra={"event": "api_error", "details": {"request_id": request_id}},
            )
            raise

        response.headers["X-Request-ID"] = request_id
        route = request.scope.get("route")
        logger.info(
            "Local API request completed",
            extra={
                "event": "api_request",
                "details": {
                    "request_id": request_id,
                    "route": getattr(route, "path", "unmatched"),
                    "source": request.query_params.get("source"),
                    "status_code": response.status_code,
                    "response_bytes": int(response.headers.get("content-length", 0)),
                    "session_id": request.headers.get("x-inboxd-session", "")[:128] or None,
                    "has_query": bool(
                        request.query_params.get("q") or request.query_params.get("query")
                    ),
                    "order": request.query_params.get("order"),
                    "limit": request.query_params.get("limit"),
                    "conversation_scoped": bool(request.query_params.get("conversation_id")),
                    "time_filtered": bool(
                        request.query_params.get("before") or request.query_params.get("after")
                    ),
                    "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                },
            },
        )

        return response

    @app.get("/status")
    def status():
        return app.state.store.status()

    @app.get("/search")
    def search(
        q: str = Query("", max_length=500),
        source: Source | None = None,
        sender: str | None = None,
        conversation_id: str | None = None,
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0, le=1000),
        order: SearchOrder = "relevance",
        before: str | None = None,
        after: str | None = None,
        match: MatchMode = "all",
    ):
        try:
            result = app.state.store.search(
                q, source, sender, conversation_id, limit, offset, order, before, after, match
            )
            result = RecordPresenter(app.state.store).page(result, source, q)
            next_offset = (
                offset + limit
                if offset + limit < result["total"] and offset + limit <= 1000
                else None
            )
            result["next_request"] = (
                {
                    "tool": "search_messages",
                    "arguments": {
                        "query": q,
                        "source": source,
                        "sender": sender,
                        "conversation_id": conversation_id,
                        "limit": limit,
                        "offset": next_offset,
                        "order": order,
                        "before": before,
                        "after": after,
                        "match": match,
                    },
                }
                if next_offset is not None
                else None
            )
            result["has_more"] = offset + limit < result["total"]
            result["pagination_limit_reached"] = result["has_more"] and next_offset is None

            return result
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.get("/message")
    def message(
        id: str,
        offset: int = Query(0, ge=0),
        length: int = Query(12000, ge=1, le=20000),
        include_metadata: bool = False,
    ):
        value = app.state.store.get("message:" + id)

        if value is None:
            raise HTTPException(404, "Message not found")

        return RecordPresenter(app.state.store).detail(value, offset, length, include_metadata)

    @app.get("/conversation")
    def conversation(id: str, cursor: str | None = None, limit: int = Query(50, ge=1, le=100)):
        try:
            result = app.state.store.conversation(id, cursor, limit)
            result = RecordPresenter(app.state.store).page(result)
            result["has_more"] = result["next_cursor"] is not None
            result["next_request"] = (
                {
                    "tool": "get_conversation",
                    "arguments": {"id": id, "cursor": result["next_cursor"], "limit": limit},
                }
                if result["has_more"]
                else None
            )

            return result
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.get("/changes")
    def changes(
        cursor: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=100),
        source: Source | None = None,
    ):
        result = app.state.store.changes(cursor, limit, source)

        return RecordPresenter(app.state.store).changes(result)

    @app.get("/sources")
    def sources(source: Source | None = None):
        return app.state.store.sources(source)

    @app.get("/recent")
    def recent(
        source: Source | None = None,
        before: str | None = None,
        after: str | None = None,
        cursor: str | None = None,
        limit: int = Query(20, ge=1, le=100),
        conversation_id: str | None = None,
    ):
        try:
            result = app.state.store.recent(source, before, after, cursor, limit, conversation_id)
            result = RecordPresenter(app.state.store).page(result, source)
            result["has_more"] = result["next_cursor"] is not None
            result["next_request"] = (
                {
                    "tool": "get_recent_records",
                    "arguments": {
                        "source": source,
                        "before": before,
                        "after": after,
                        "cursor": result["next_cursor"],
                        "limit": limit,
                        "conversation_id": conversation_id,
                    },
                }
                if result["has_more"]
                else None
            )

            return result
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.get("/slack/conversations")
    def slack_conversations(
        query: str = "",
        kind: ConversationKind | None = None,
        user_id: str | None = None,
        account: str | None = None,
        include_archived: bool = False,
        cursor: str | None = None,
        limit: int = Query(20, ge=1, le=100),
    ):
        try:
            result = SlackQueries(app.state.store).conversations(
                query,
                kind,
                user_id,
                account,
                include_archived,
                cursor,
                limit,
            )
            result["has_more"] = result["next_cursor"] is not None
            result["next_request"] = (
                {
                    "tool": "list_slack_conversations",
                    "arguments": {
                        "query": query,
                        "kind": kind,
                        "user_id": user_id,
                        "account": account,
                        "include_archived": include_archived,
                        "cursor": result["next_cursor"],
                        "limit": limit,
                    },
                }
                if result["has_more"]
                else None
            )

            return result
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.get("/slack/thread")
    def slack_thread(
        conversation_id: str,
        thread_id: str,
        cursor: str | None = None,
        limit: int = Query(50, ge=1, le=100),
    ):
        try:
            result = SlackQueries(app.state.store).thread(conversation_id, thread_id, cursor, limit)
            result = RecordPresenter(app.state.store).page(result, "slack")
            result["has_more"] = result["next_cursor"] is not None
            result["next_request"] = (
                {
                    "tool": "get_slack_thread",
                    "arguments": {
                        "conversation_id": conversation_id,
                        "thread_id": thread_id,
                        "cursor": result["next_cursor"],
                        "limit": limit,
                    },
                }
                if result["has_more"]
                else None
            )

            return result
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.get("/attachments")
    def attachments(message_id: str):
        return {
            "items": [
                {
                    "id": key.removeprefix("attachment:"),
                    **{k: v for k, v in value.items() if k != "text"},
                }
                for key, value in app.state.store.scan("attachment:")
                if value["message_id"] == message_id
            ]
        }

    @app.get("/attachment-text")
    def attachment_text(
        id: str, offset: int = Query(0, ge=0), length: int = Query(10000, ge=1, le=20000)
    ):
        value = app.state.store.get("attachment:" + id)

        if value is None:
            raise HTTPException(404, "Attachment not found")

        text = value.get("text", "")

        return {
            "state": value["state"],
            "text": text[offset : offset + length],
            "text_length": len(text),
            "text_available": bool(text),
            "empty_reason": (
                "content_not_archived"
                if value["state"] != "ready"
                else "text_not_extracted"
                if not text
                else "offset_at_or_after_end"
                if offset >= len(text)
                else None
            ),
            "next_offset": offset + length if offset + length < len(text) else None,
            "next_request": {
                "tool": "get_attachment_text",
                "arguments": {"id": id, "offset": offset + length, "length": length},
            }
            if offset + length < len(text)
            else None,
            "extraction": value.get("extraction"),
        }

    return app
