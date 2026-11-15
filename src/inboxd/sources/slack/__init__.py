from inboxd.sources.slack.collector import SlackCollector


def plugin():
    return {
        "name": "slack",
        "collector": SlackCollector,
        "description": "Authorized Slack conversations, threads and source metadata",
        "timestamp_meaning": "message_sent_at",
        "content_kinds": ("message",),
        "config": {
            "enabled": True,
            "poll_seconds": 3,
            "reconcile_seconds": 3600,
            "full_reconcile_seconds": 604800,
            "event_poll_seconds": 0.2,
            "connection_check_seconds": 10,
        },
    }


__all__ = ["SlackCollector"]
