import json
import logging
import logging.handlers
import re
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

SECRET_PATTERN = re.compile(
    r"(?i)(?:xox[baprs]-[a-z0-9-]+|bearer\s+[a-z0-9._~+/=-]+|"
    r"(?:token|authorization|password|secret)[\"']?\s*[:=]\s*[\"']?[^\s,\"'}]+)"
)
PRIVATE_FIELDS = {"token", "authorization", "password", "secret", "text", "body", "raw", "query"}


def sanitized(value):
    if isinstance(value, dict):
        return {
            str(key): "[redacted]" if str(key).lower() in PRIVATE_FIELDS else sanitized(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [sanitized(item) for item in value]

    if isinstance(value, str):
        return SECRET_PATTERN.sub("[redacted]", value)

    return value


class JSONFormatter(logging.Formatter):
    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        value = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", "log"),
            "message": record.getMessage(),
            "run_id": self.run_id,
            "pid": record.process,
        }
        value.update(getattr(record, "details", {}))

        if record.exc_info:
            value["exception"] = self.formatException(record.exc_info)

        return json.dumps(sanitized(value), ensure_ascii=False, default=str)


def configure_logging(data_dir: Path, retention_days: int) -> str:
    directory = data_dir / "logs"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_id = uuid.uuid4().hex
    handler = logging.handlers.TimedRotatingFileHandler(
        directory / "diagnostics.jsonl",
        when="midnight",
        backupCount=retention_days,
        encoding="utf-8",
        utc=True,
    )
    handler.setFormatter(JSONFormatter(run_id))
    handler.setLevel(logging.INFO)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("slack_sdk").setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Diagnostics initialized",
        extra={
            "event": "diagnostics_started",
            "details": {
                "retention_days": retention_days,
                "rotation": "daily_utc",
                "monotonic_started": time.monotonic(),
            },
        },
    )

    return run_id
