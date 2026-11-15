import logging
from functools import lru_cache
from importlib.metadata import entry_points

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def plugins() -> dict[str, dict]:
    result = {}

    for point in sorted(entry_points(group="inboxd.collectors"), key=lambda item: item.name):
        name = point.name

        if name in result:
            result[name] = unavailable(name, "Duplicate collector entry point")
            continue

        try:
            plugin = point.load()()

            if not isinstance(plugin, dict) or plugin.get("name") != name:
                raise ValueError("Plugin name must match its entry point")

            if not callable(plugin.get("collector")):
                raise TypeError("Plugin must provide a collector factory")

            if not isinstance(plugin.get("description"), str):
                raise TypeError("Plugin must provide a description")

            if not isinstance(plugin.get("config"), dict):
                raise TypeError("Plugin must provide configuration defaults")

            kinds = plugin.get("content_kinds", ())

            if not isinstance(kinds, (tuple, list)) or not all(isinstance(k, str) for k in kinds):
                raise ValueError("Plugin content kinds must be a sequence of strings")

            from inboxd.sources.configuration import configuration_model

            schema = configuration_model(name, plugin)
            schema.model_validate(plugin["config"])
            plugin = {**plugin, "config_schema": schema}
            result[name] = plugin
        except Exception as error:
            logger.exception("Collector plugin %s unavailable: %s", name, type(error).__name__)
            result[name] = unavailable(name, type(error).__name__)

    return result


def unavailable(name: str, error: str) -> dict:
    return {
        "name": name,
        "description": "Collector plugin unavailable",
        "config": {"enabled": False},
        "error": error,
    }


def content_kinds() -> set[str]:
    return {"message"}.union(*(plugin.get("content_kinds", ()) for plugin in plugins().values()))
