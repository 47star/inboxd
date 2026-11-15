import json
import threading
import time
from pathlib import Path

import httpx

from inboxd.sources.google.auth import TOKEN_URL, GoogleAuthenticationRequired, save_json

TOKEN_LOCK = threading.Lock()
ROOTS = {
    "google_calendar": "https://www.googleapis.com/calendar/v3",
    "google_tasks": "https://tasks.googleapis.com/tasks/v1",
}


class GoogleUnavailable(Exception):
    def __init__(self, status: int, reason: str):
        self.status = status
        super().__init__(f"Google HTTP {status}: {reason}")


class GoogleClient:
    def __init__(self, directory: Path, transport=None):
        self.directory = directory
        self.http = httpx.Client(timeout=30, trust_env=False, transport=transport)

    def close(self):
        self.http.close()

    def credentials(self, force=False):
        with TOKEN_LOCK:
            path = self.directory / "token.json"

            if not path.exists():
                raise GoogleAuthenticationRequired("Run inboxd google-auth to connect Google")

            token = json.loads(path.read_text())

            if not force and token.get("expires_at", 0) > time.time() + 60:
                return token

            config = json.loads((self.directory / "client.json").read_text())

            try:
                response = self.http.post(
                    TOKEN_URL,
                    data={
                        **config,
                        "refresh_token": token["refresh_token"],
                        "grant_type": "refresh_token",
                    },
                )
            except httpx.HTTPError:
                raise GoogleUnavailable(0, "token refresh connection failed") from None

            if response.status_code in {400, 401}:
                raise GoogleAuthenticationRequired("Google authorization expired or was revoked")

            if response.status_code != 200:
                raise GoogleUnavailable(response.status_code, "token refresh failed")

            refreshed = response.json()
            token.update(refreshed, expires_at=time.time() + refreshed.get("expires_in", 3600))
            save_json(path, token)

            return token

    def get(self, source: str, path: str, params: dict):
        for attempt in range(2):
            credentials = self.credentials(force=attempt == 1)

            try:
                response = self.http.get(
                    ROOTS[source] + path,
                    params=params,
                    headers={"Authorization": "Bearer " + credentials["access_token"]},
                )
            except httpx.HTTPError:
                raise GoogleUnavailable(0, "API connection failed") from None

            if response.status_code == 401:
                continue

            if response.status_code != 200:
                try:
                    errors = response.json().get("error", {}).get("errors", [])
                    reason = (
                        errors[0].get("reason", "request failed") if errors else "request failed"
                    )
                except (ValueError, AttributeError, IndexError):
                    reason = "request failed"

                raise GoogleUnavailable(response.status_code, reason)

            return response.json()

        raise GoogleAuthenticationRequired("Google API rejected the refreshed credentials")
