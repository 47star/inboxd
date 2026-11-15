import asyncio
import json
import time

import httpx

from inboxd.runtime.config import Settings


def report(value: dict) -> None:
    print(json.dumps(value, ensure_ascii=False), flush=True)


def event_summary(event: dict) -> dict:
    record = event["message"]

    return {
        "sequence": event["sequence"],
        "received_at": event["received_at"],
        "origin": event["origin"],
        "operation": event["operation"],
        "source": record["source"],
        "kind": record.get("kind", "message"),
        "id": record["id"],
        "timestamp": record["timestamp"],
    }


async def monitor_events(settings: Settings, seconds: int) -> None:
    token = (settings.data_dir / "api-token").read_text().strip()
    deadline = time.monotonic() + seconds

    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{settings.port}",
        headers={"Authorization": f"Bearer {token}"},
        trust_env=False,
    ) as client:
        response = await client.get("/status")
        response.raise_for_status()
        cursor = response.json()["sequence"]

        report({"monitor_started": True, "cursor": cursor})

        while time.monotonic() < deadline:
            try:
                response = await client.get("/changes", params={"cursor": cursor, "limit": 100})
                response.raise_for_status()
                data = response.json()

                for event in data["items"]:
                    report(event_summary(event))

                cursor = data["cursor"]

                if len(data["items"]) < 100:
                    await asyncio.sleep(1)
            except (httpx.HTTPError, OSError) as error:
                report({"connection_state": type(error).__name__})
                await asyncio.sleep(1)

        report({"monitor_finished": True, "cursor": cursor})
