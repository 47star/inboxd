from inboxd.sources.redmine.collector import RedmineCollector


def plugin():
    return {
        "name": "redmine",
        "collector": RedmineCollector,
        "description": (
            "Redmine issues and metadata via initial backfill, Atom-triggered API and "
            "reconciliation"
        ),
        "timestamp_meaning": "latest_issue_or_journal_timestamp",
        "content_kinds": ("work_item",),
        "config_constraints": {"reconcile_batch_size": {"ge": 1, "le": 100}},
        "config": {
            "enabled": False,
            "url": "",
            "poll_seconds": 60,
            "reconcile_batch_size": 40,
        },
    }


__all__ = ["RedmineCollector"]
