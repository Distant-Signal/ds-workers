#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI that reports each check on stdout
# ruff: noqa: D103, D401, S105, N818, FBT001, PLR2004, S310, S603, TRY003, EM101, EM102, E501, RUF001  # a throwaway test harness: it runs fixed local commands and fixed URLs, and its checks read as sentences
"""Prove the notifier's webhook presets against real ntfy and Gotify servers.

  uv run scripts/webhook-e2e.py --ntfy /path/to/ntfy --gotify /path/to/gotify-linux-amd64

Starts throwaway ntfy (with access control on: deny-all, one user, one token)
and Gotify servers from their release binaries on 127.0.0.1, creates a topic
grant, an app and a client, then runs the notifier's `#[ignore]`d
`the_presets_deliver_to_real_servers` test (cargo feature
`insecure-test-egress`, debug builds only), which sends through the real
presets. Then, independently of the Rust code, this script:

  - reads the messages back from each server and checks the title, body,
    priority, tags and click link that arrived, and that the sends with a
    wrong or missing token delivered nothing;
  - recomputes the generic payload's HMAC-SHA256 signature from the captured
    request (stdlib hmac) and checks a tampered body fails it.

The same test also shows the normal (guarded) sender refusing these servers
and every private, link-local, metadata and tailnet address. Everything is
torn down at the end. Stdlib only; exit status 0 only if every check passed.
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SITE_URL = "https://ds.example.org"
GENERIC_SECRET = "whsec_e2e_signing_secret"


class Failed(Exception):
    """A check did not hold."""


def check(ok: bool, what: str) -> None:
    print(f"  {'ok  ' if ok else 'FAIL'} {what}")
    if not ok:
        raise Failed(what)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, bytes]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url, data=data, method=method, headers=headers or {}
    )
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return int(response.status), bytes(response.read())
    except urllib.error.HTTPError as err:
        return err.code, err.read()


def wait_until_ready(url: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            status, _ = http("GET", url)
        except OSError:
            status = 0
        if status == 200:
            return
        time.sleep(0.3)
    raise Failed(f"{url} did not come up")


@contextmanager
def running(
    cmd: Sequence[str], env: dict[str, str], cwd: Path, log: Path
) -> Iterator[None]:
    with log.open("wb") as out:
        process = subprocess.Popen(
            cmd, env=env, cwd=cwd, stdout=out, stderr=subprocess.STDOUT
        )
        try:
            yield
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def start_ntfy(stack: ExitStack, binary: Path, work: Path) -> tuple[str, str]:
    """A topic URL and an access token for it, on an ntfy that denies by default."""
    port = free_port()
    config = work / "ntfy.yml"
    config.write_text(
        f"""base-url: "http://127.0.0.1:{port}"
listen-http: "127.0.0.1:{port}"
cache-file: "{work / "ntfy-cache.db"}"
auth-file: "{work / "ntfy-auth.db"}"
auth-default-access: "deny-all"
"""
    )
    env = {
        **os.environ,
        "NTFY_PASSWORD": "e2e-password",
        "NTFY_CONFIG_FILE": str(config),
    }
    ntfy = [str(binary)]
    # The auth database is created by the running server, so users and the
    # token are added once it is up.
    stack.enter_context(running([*ntfy, "serve"], env, work, work / "ntfy.log"))
    base = f"http://127.0.0.1:{port}"
    wait_until_ready(f"{base}/v1/health")
    subprocess.run(
        [*ntfy, "user", "add", "alice"], env=env, check=True, capture_output=True
    )
    subprocess.run(
        [*ntfy, "access", "alice", "trains", "rw"],
        env=env,
        check=True,
        capture_output=True,
    )
    out = subprocess.run(
        [*ntfy, "token", "add", "alice"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    found = re.search(r"tk_[a-z0-9]+", out)
    if not found:
        raise Failed("ntfy printed no token")
    return f"{base}/trains", found.group(0)


def start_gotify(stack: ExitStack, binary: Path, work: Path) -> tuple[str, str, str]:
    """The base URL, an app token (to send with) and a client token (to read back)."""
    port = free_port()
    data = work / "gotify"
    data.mkdir()
    env = {
        **os.environ,
        "GOTIFY_SERVER_LISTENADDR": "127.0.0.1",
        "GOTIFY_SERVER_PORT": str(port),
        "GOTIFY_DATABASE_DIALECT": "sqlite3",
        "GOTIFY_DATABASE_CONNECTION": str(data / "gotify.db"),
        "GOTIFY_UPLOADEDIMAGESDIR": str(data / "images"),
        "GOTIFY_PLUGINSDIR": str(data / "plugins"),
        "GOTIFY_DEFAULTUSER_NAME": "admin",
        "GOTIFY_DEFAULTUSER_PASS": "admin",
    }
    stack.enter_context(running([str(binary)], env, data, work / "gotify.log"))
    base = f"http://127.0.0.1:{port}"
    wait_until_ready(f"{base}/version")
    basic = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}
    _, app = http("POST", f"{base}/application", basic, {"name": "distant-signal"})
    _, client = http("POST", f"{base}/client", basic, {"name": "reader"})
    return base, str(json.loads(app)["token"]), str(json.loads(client)["token"])


def verify_ntfy(topic_url: str, token: str) -> None:
    print("ntfy: what arrived")
    status, body = http(
        "GET", f"{topic_url}/json?poll=1", {"Authorization": f"Bearer {token}"}
    )
    check(status == 200, "the topic can be read with the token")
    messages = [json.loads(line) for line in body.decode().splitlines() if line]
    messages = [m for m in messages if m.get("event") == "message"]
    check(
        len(messages) == 1,
        f"exactly one message arrived (the wrong-token and anonymous sends did not): {len(messages)}",
    )
    message = messages[0]
    check(
        message["title"] == "Delayed – 09:15 to Leeds",
        "the non-ASCII title survived the header encoding",
    )
    check(message["message"] == "About 20 minutes late.", "the body is the message")
    check(message["priority"] == 4, "an urgent notification is priority 4 (high)")
    check(message["tags"] == ["train"], "the tag is train")
    check(
        message["click"] == f"{SITE_URL}/track/42",
        "the click link is the absolute page",
    )
    anonymous, _ = http("GET", f"{topic_url}/json?poll=1")
    check(
        anonymous in (401, 403),
        f"the topic refuses a reader without the token ({anonymous})",
    )


def verify_gotify(base: str, client_token: str) -> None:
    print("Gotify: what arrived")
    status, body = http("GET", f"{base}/message", {"X-Gotify-Key": client_token})
    check(status == 200, "messages can be read with the client token")
    messages = json.loads(body)["messages"]
    check(
        len(messages) == 1,
        f"exactly one message arrived (the wrong-token send did not): {len(messages)}",
    )
    message = messages[0]
    check(message["title"] == "Leave by 08:40", "the title is the title")
    check(
        message["message"] == "Your train is 20 minutes late.",
        "the body is the message",
    )
    check(message["priority"] == 5, "a normal notification is priority 5")
    click = message["extras"]["client::notification"]["click"]["url"]
    check(click == f"{SITE_URL}/track/42", "the click extra is the absolute page")


def verify_generic(capture: Path) -> None:
    print("generic: the signature, recomputed independently")
    data = json.loads(capture.read_text())
    headers = {}
    for line in data["headers"][1:]:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    body: str = data["body"]
    timestamp = headers["x-ds-timestamp"]
    expected = (
        "v1="
        + hmac.new(
            GENERIC_SECRET.encode(), f"{timestamp}.{body}".encode(), hashlib.sha256
        ).hexdigest()
    )
    check(
        hmac.compare_digest(headers["x-ds-signature"], expected),
        "X-DS-Signature equals HMAC-SHA256(secret, timestamp.body)",
    )
    tampered = (
        "v1="
        + hmac.new(
            GENERIC_SECRET.encode(), f"{timestamp}.{body} ".encode(), hashlib.sha256
        ).hexdigest()
    )
    check(
        not hmac.compare_digest(headers["x-ds-signature"], tampered),
        "a changed body does not verify",
    )
    wrong_key = (
        "v1="
        + hmac.new(b"other", f"{timestamp}.{body}".encode(), hashlib.sha256).hexdigest()
    )
    check(
        not hmac.compare_digest(headers["x-ds-signature"], wrong_key),
        "another secret does not verify",
    )
    check(abs(int(timestamp) - time.time()) < 300, "the timestamp is current")
    check(headers["content-type"] == "application/json", "the body is JSON")
    payload = json.loads(body)
    check(
        sorted(payload)
        == [
            "attribution",
            "body",
            "delivery_id",
            "event",
            "occurred_at",
            "schema_version",
            "tag",
            "title",
            "urgent",
            "url",
        ],
        "the payload has exactly the documented fields",
    )
    check(
        payload["event"] == "train_update" and headers["x-ds-event"] == "train_update",
        "the event is named in body and header",
    )
    check(
        payload["attribution"]["url"] == f"{SITE_URL}/attribution",
        "the credits link is there",
    )
    check(
        "user" not in body and "journey" not in body,
        "no account or journey identifiers",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--ntfy", type=Path, required=True, help="the ntfy server binary"
    )
    parser.add_argument(
        "--gotify", type=Path, required=True, help="the Gotify server binary"
    )
    args = parser.parse_args(argv)
    work = Path(tempfile.mkdtemp(prefix="ds-webhook-e2e-"))
    try:
        with ExitStack() as stack:
            ntfy_url, ntfy_token = start_ntfy(stack, args.ntfy.resolve(), work)
            gotify_url, gotify_app, gotify_client = start_gotify(
                stack, args.gotify.resolve(), work
            )
            print(f"ntfy at {ntfy_url}, Gotify at {gotify_url}")
            env = {
                **os.environ,
                "E2E_NTFY_URL": ntfy_url,
                "E2E_NTFY_TOKEN": ntfy_token,
                "E2E_GOTIFY_URL": gotify_url,
                "E2E_GOTIFY_TOKEN": gotify_app,
                "E2E_CAPTURE_DIR": str(work),
                "E2E_SITE_URL": SITE_URL,
            }
            cargo = [
                "cargo",
                "test",
                "-p",
                "notifier",
                "--features",
                "insecure-test-egress",
            ]
            run = subprocess.run(
                [
                    *cargo,
                    "the_presets_deliver_to_real_servers",
                    "--",
                    "--ignored",
                    "--nocapture",
                ],
                env=env,
                cwd=ROOT,
                check=False,
            )
            check(
                run.returncode == 0,
                "the notifier's own sends and the guard's refusals passed",
            )
            verify_ntfy(ntfy_url, ntfy_token)
            verify_gotify(gotify_url, gotify_client)
            verify_generic(work / "generic.json")
    except Failed as err:
        print(f"FAILED: {err}")
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print("all checks passed; servers stopped and files removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
