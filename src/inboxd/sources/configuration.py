from datetime import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, create_model, model_validator


class SourceOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    enabled: bool = False

    @model_validator(mode="after")
    def validate_options(self):
        for name, value in self.model_dump().items():
            if name.endswith("_seconds") and value <= 0:
                raise ValueError(f"{name} must be positive")

            if name in {"window_start", "window_end"}:
                parsed = time.fromisoformat(value)

                if parsed.tzinfo is not None:
                    raise ValueError(f"{name} must be a local time")

            if name == "timezone":
                try:
                    ZoneInfo(value)
                except (ZoneInfoNotFoundError, ValueError) as error:
                    raise ValueError("Unknown timezone") from error

        return self


def configuration_model(name: str, plugin: dict) -> type[BaseModel]:
    supplied = plugin.get("config_schema")

    if supplied is not None:
        if not isinstance(supplied, type) or not issubclass(supplied, SourceOptions):
            raise ValueError("config_schema must extend SourceOptions")

        return supplied

    fields = {}

    for key, value in plugin["config"].items():
        annotation = float if key.endswith("_seconds") else type(value)
        constraints = plugin.get("config_constraints", {}).get(key, {})
        fields[key] = (annotation, Field(default=value, **constraints))

    return create_model(name + "Options", __base__=SourceOptions, **fields)
