import asyncio
import json
import os
from pathlib import Path

import click
import uvicorn

from inboxd.api.http import create_app
from inboxd.api.mcp import main as run_mcp
from inboxd.api.mcp import request
from inboxd.runtime.config import load_settings
from inboxd.runtime.diagnostics import configure_logging
from inboxd.runtime.launchd import install_agent
from inboxd.runtime.monitor import monitor_events
from inboxd.runtime.native import build_helper
from inboxd.sources.google.auth import authorize


@click.group()
def main() -> None:
    os.umask(0o077)


@main.command()
def serve() -> None:
    settings = load_settings()
    configure_logging(settings.data_dir, settings.diagnostic_retention_days)
    application = create_app(settings)

    uvicorn.run(
        application,
        host="127.0.0.1",
        port=settings.port,
        access_log=False,
        log_config=None,
    )


@main.command()
def install() -> None:
    result = install_agent(load_settings())

    click.echo(json.dumps(result, ensure_ascii=False, indent=2))


@main.command()
def status() -> None:
    result = asyncio.run(request("/status"))

    click.echo(json.dumps(result, ensure_ascii=False, indent=2))


@main.command("google-auth")
@click.option("--client-file", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--no-browser", is_flag=True)
def google_auth(client_file: Path, no_browser: bool) -> None:
    result = authorize(load_settings().data_dir / "google", client_file, not no_browser)
    click.echo(json.dumps(result, ensure_ascii=False))


@main.command()
@click.option("--seconds", default=600, type=click.IntRange(min=1))
def monitor(seconds: int) -> None:
    asyncio.run(monitor_events(load_settings(), seconds))


@main.command(name="mcp")
def mcp_command() -> None:
    run_mcp()


@main.command(name="build-helper")
def build_helper_command() -> None:
    click.echo(build_helper())


if __name__ == "__main__":
    main()
