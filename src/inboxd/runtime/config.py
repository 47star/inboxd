import json
import os
import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    data_dir: Path = Field(
        default_factory=lambda: Path.home() / "Library/Application Support/inboxd"
    )
    port: int = Field(default=48765, ge=1024, le=65535)
    poll_seconds: float = Field(default=3, ge=1)
    reconcile_seconds: float = Field(default=300, ge=10)
    sources: dict[str, dict] = Field(default_factory=dict)
    apple_helper: Path = Field(
        default_factory=lambda: (
            Path.home() / "Applications/inboxd-source.app/Contents/MacOS/inboxd-source"
        )
    )
    messages_path: Path = Field(default_factory=lambda: Path.home() / "Library/Messages/chat.db")
    mail_path: Path = Field(default_factory=lambda: Path.home() / "Library/Mail")
    secrets_file: Path = Field(default_factory=lambda: Path.home() / ".config/inboxd/secrets.json")
    attachment_max_bytes: int = 100 * 1024 * 1024
    diagnostic_retention_days: int = Field(default=30, ge=1, le=365)

    @model_validator(mode="after")
    def validate_sources(self):
        from inboxd.sources.registry import plugins

        available = plugins()

        for source, options in self.sources.items():
            if "enabled" in options and type(options["enabled"]) is not bool:
                raise ValueError(f"sources.{source}.enabled must be a boolean")

            if source not in available:
                if options.get("enabled") is False:
                    continue

                raise ValueError(f"Unknown source: {source}; disable it or install its plugin")

            plugin = available[source]

            if not plugin.get("error"):
                plugin["config_schema"].model_validate({**plugin["config"], **options})

        return self

    def source_options(self, source: str) -> dict:
        from inboxd.sources.registry import plugins

        defaults = plugins().get(source, {}).get("config", {})

        return {**defaults, **self.sources.get(source, {})}

    def source_value(self, source: str, name: str, default=None):
        options = self.source_options(source)

        return options.get(name, default)

    def source_enabled(self, source: str) -> bool:
        from inboxd.sources.registry import plugins

        plugin = plugins().get(source)

        if plugin is None or plugin.get("error"):
            return False

        return bool(self.source_options(source).get("enabled", False))

    def source_interval(self, source: str, operation: str = "poll") -> float:
        configured = self.source_value(source, f"{operation}_seconds")

        if configured is not None:
            return configured

        return self.reconcile_seconds if operation == "reconcile" else self.poll_seconds


def load_settings(path: str | None = None) -> Settings:
    target = Path(path).expanduser() if path else Path.home() / ".config/inboxd/config.toml"
    values = tomllib.loads(target.read_text()) if target.exists() else {}

    if os.environ.get("INBOXD_DATA_DIR"):
        values["data_dir"] = os.environ["INBOXD_DATA_DIR"]

    return Settings.model_validate(values)


def load_secrets(settings: Settings) -> dict[str, str]:
    values = json.loads(settings.secrets_file.read_text()) if settings.secrets_file.exists() else {}

    for name in [
        "slack_app_token",
        "slack_user_token",
        "redmine_api_key",
        "redmine_feed_key",
    ]:
        if os.environ.get(f"INBOXD_{name.upper()}"):
            values[name] = os.environ[f"INBOXD_{name.upper()}"]

    return values
