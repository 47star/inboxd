from inboxd.sources.registry import plugins
from inboxd.storage.archive import Store


def query_context(store: Store, source: str | None, sources: set[str], empty: bool) -> dict:
    available = plugins()
    selected = [source] if source else sorted(sources or store.known_sources())
    pending = len(store.scan("pending:", limit=10001))
    statuses = []

    for name in selected:
        state = store.get("status:" + name, {})

        if name not in available:
            state = {**state, "state": "plugin_removed", "enabled": False}

        issues = []

        if state.get("state") == "disabled":
            issues.append("collection_disabled_cached_records_only")

        if state.get("backfill_running") or state.get("state") == "backfilling":
            issues.append("backfill_in_progress")

        if state.get("state") in {
            "retrying",
            "source_unavailable",
            "configuration_required",
            "partial",
            "stopped",
            "starting",
            "authentication_required",
            "plugin_unavailable",
            "initialization_failed",
            "plugin_removed",
        }:
            issues.append("collection_not_current")

        if not state.get("last_success"):
            issues.append("no_successful_sync_recorded")

        if name == "mail":
            issues.append("local_files_only_not_full_server_mailbox")

        if name == "redmine":
            issues.extend(["no_deletion_confirmation", "metadata_reconciled_in_batches"])

        if name == "slack" and state.get("unavailable_channels"):
            issues.append("some_conversations_unavailable")

        status = {
            "source": name,
            "enabled": state.get("enabled", True),
            "state": state.get("state", "not_started"),
            "last_success_at": state.get("last_success"),
            "last_live_event_at": state.get("last_event_at"),
            "issues": issues,
        }

        for key in ("cache_only", "next_run_at", "scopes", "window", "timezone", "coverage"):
            if key in state:
                status[key] = state[key]

        if any(key in state for key in ("next_run_at", "scopes", "window")):
            status.setdefault("cache_only", True)

        if name == "slack":
            status["unavailable_channels"] = state.get("unavailable_channels", [])

        if name in {"google_calendar", "google_tasks"}:
            status.update(
                cache_only=True,
                scopes=state.get("scopes", []),
                poll_seconds=state.get("poll_seconds"),
                coverage=state.get("coverage"),
            )

        statuses.append(status)

    return {
        "scope": "local_archive",
        "result_state": "no_local_matches" if empty else "matches",
        "sources": statuses,
        "pending_index": pending,
        "empty_result_meaning": (
            "No matching local records; check source coverage before concluding absence"
            if empty
            else None
        ),
    }
