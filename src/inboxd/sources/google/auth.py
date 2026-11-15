import base64
import hashlib
import json
import os
import secrets
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx

SCOPES = [
    "openid",
    "email",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/tasks.readonly",
]
TOKEN_URL = "https://oauth2.googleapis.com/token"


class GoogleAuthenticationRequired(Exception):
    pass


def save_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)

    with os.fdopen(descriptor, "w") as output:
        json.dump(value, output)
        output.flush()
        os.fsync(output.fileno())

    temporary.replace(path)


def authorize(directory: Path, client_file: Path, open_browser=True, timeout=600) -> dict:
    config = json.loads(client_file.read_text()).get("installed", {})

    if not config.get("client_id") or not config.get("client_secret"):
        raise ValueError("A Google Desktop app OAuth client JSON file is required")

    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    )
    result = {}

    class Callback(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlsplit(self.path)
            values = parse_qs(parsed.query)

            if parsed.path != "/callback" or values.get("state") != [state]:
                self.send_error(400)

                return

            result.update({key: value[0] for key, value in values.items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Google authorization received. You can close this tab.")

        def log_message(self, format, *args):
            return

    with HTTPServer(("127.0.0.1", 0), Callback) as server:
        server.timeout = 1
        redirect = f"http://127.0.0.1:{server.server_port}/callback"
        url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(
            {
                "client_id": config["client_id"],
                "redirect_uri": redirect,
                "response_type": "code",
                "scope": " ".join(SCOPES),
                "access_type": "offline",
                "prompt": "consent",
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
        print(url, flush=True)

        if open_browser:
            webbrowser.open(url)

        deadline = time.monotonic() + timeout

        while not result and time.monotonic() < deadline:
            server.handle_request()

    if not result.get("code"):
        raise GoogleAuthenticationRequired("Google consent was not completed")

    with httpx.Client(timeout=30, trust_env=False) as client:
        response = client.post(
            TOKEN_URL,
            data={
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
                "code": result["code"],
                "code_verifier": verifier,
                "redirect_uri": redirect,
                "grant_type": "authorization_code",
            },
        )

        if response.status_code != 200:
            raise GoogleAuthenticationRequired(f"Google token exchange HTTP {response.status_code}")

        token = response.json()
        granted = set(token.get("scope", "").split())

        if not set(SCOPES[2:]) <= granted or not token.get("refresh_token"):
            raise GoogleAuthenticationRequired(
                "Calendar and Tasks read access and offline access are required"
            )

        identity = client.get(
            "https://openidconnect.googleapis.com/v1/userinfo",
            headers={"Authorization": "Bearer " + token["access_token"]},
        )

        if identity.status_code != 200 or not identity.json().get("sub"):
            raise GoogleAuthenticationRequired("Google account identity could not be verified")

        token.update(
            account=identity.json()["sub"],
            email=identity.json().get("email", ""),
            expires_at=time.time() + token.get("expires_in", 3600),
        )
        save_json(
            directory / "client.json", {key: config[key] for key in ("client_id", "client_secret")}
        )
        save_json(directory / "token.json", token)

    return {"account": token["email"], "authorized": True}
