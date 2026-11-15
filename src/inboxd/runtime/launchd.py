import os
import plistlib
import subprocess
import sys
import time
from pathlib import Path

from inboxd.runtime.config import Settings

LABEL = "net.donghoon.inboxd"


def agent_definition(settings: Settings) -> dict:
    logs = settings.data_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True, mode=0o700)

    executable = str(Path(sys.executable).absolute())
    working_directory = str(Path(__file__).resolve().parents[3])

    return {
        "Label": LABEL,
        "ProgramArguments": [executable, "-m", "inboxd.cli", "serve"],
        "WorkingDirectory": working_directory,
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "ExitTimeOut": 120,
        "ProcessType": "Background",
        "StandardOutPath": str(logs / "stdout.log"),
        "StandardErrorPath": str(logs / "stderr.log"),
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
    }


def install_agent(settings: Settings) -> dict[str, str]:
    directory = Path.home() / "Library/LaunchAgents"
    directory.mkdir(exist_ok=True)

    target = directory / f"{LABEL}.plist"
    target.write_bytes(plistlib.dumps(agent_definition(settings)))

    domain = f"gui/{os.getuid()}"
    service = f"{domain}/{LABEL}"
    existing = subprocess.run(
        ["launchctl", "print", service],
        capture_output=True,
        check=False,
    )

    if existing.returncode == 0:
        subprocess.run(["launchctl", "bootout", service], check=True)

    deadline = time.monotonic() + 120

    while True:
        result = subprocess.run(
            ["launchctl", "bootstrap", domain, str(target)],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode == 0:
            break

        if result.returncode != 5 or time.monotonic() >= deadline:
            raise RuntimeError(f"LaunchAgent bootstrap failed: {result.stderr.strip()}")

        time.sleep(1)

    return {
        "launch_agent": str(target),
        "address": f"127.0.0.1:{settings.port}",
    }
