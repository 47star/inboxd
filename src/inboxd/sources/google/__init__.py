from inboxd.sources.google.collector import GoogleCollector


def calendar_plugin():
    return {
        "name": "google_calendar",
        "collector": lambda settings, store: GoogleCollector(settings, store, "google_calendar"),
        "description": (
            "Google Calendar API events and recurring instances within the configured date window"
        ),
        "timestamp_meaning": "occurrence_start_including_future",
        "content_kinds": ("calendar",),
        "config_constraints": {
            "start_year": {"ge": 1900, "le": 2100},
            "future_years": {"ge": 1, "le": 50},
        },
        "config": {
            "enabled": False,
            "poll_seconds": 60,
            "reconcile_seconds": 300,
            "backfill_seconds": 1,
            "start_year": 1970,
            "future_years": 5,
        },
    }


def tasks_plugin():
    return {
        "name": "google_tasks",
        "collector": lambda settings, store: GoogleCollector(settings, store, "google_tasks"),
        "description": "Google Tasks API task lists including completed, hidden and assigned tasks",
        "timestamp_meaning": "task_updated_at_due_date_in_metadata",
        "content_kinds": ("task",),
        "config": {
            "enabled": False,
            "poll_seconds": 60,
            "reconcile_seconds": 21600,
            "backfill_seconds": 1,
        },
    }
