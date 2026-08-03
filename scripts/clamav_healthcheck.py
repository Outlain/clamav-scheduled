#!/usr/bin/env python3

"""Check signature readiness, database age, and the active application mode."""

from __future__ import annotations

import argparse
import json
import os
import socket
import stat
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path


DEFAULT_DEFINITIONS_DIR = "/var/lib/clamav"
DEFAULT_SOCKET_PATH = "/tmp/clamav-runtime/clamd.sock"
DEFAULT_MAX_AGE_SECONDS = 172800


@dataclass(frozen=True)
class DefinitionStatus:
    main_path: Path
    daily_path: Path
    daily_age_seconds: int


def _database_candidate(directory: Path, stem: str) -> Path | None:
    candidates = [directory / f"{stem}.cld", directory / f"{stem}.cvd"]
    available: list[tuple[int, Path]] = []
    for candidate in candidates:
        try:
            candidate_stat = candidate.lstat()
            if stat.S_ISREG(candidate_stat.st_mode) and candidate_stat.st_size > 0 and os.access(candidate, os.R_OK):
                available.append((candidate_stat.st_mtime_ns, candidate))
        except OSError:
            continue
    if not available:
        return None
    return max(available, key=lambda item: item[0])[1]


def definition_status(directory: str, now: float | None = None) -> DefinitionStatus:
    definitions_dir = Path(directory)
    if not definitions_dir.is_dir():
        raise RuntimeError(f"Definitions directory does not exist: {definitions_dir}")
    if not os.access(definitions_dir, os.R_OK | os.X_OK):
        raise RuntimeError(f"Definitions directory is not readable: {definitions_dir}")

    main_path = _database_candidate(definitions_dir, "main")
    daily_path = _database_candidate(definitions_dir, "daily")
    if main_path is None or daily_path is None:
        raise RuntimeError(
            f"Definitions are incomplete in {definitions_dir}; readable main.cvd/main.cld and daily.cvd/daily.cld are required"
        )

    current_time = time.time() if now is None else now
    daily_age_seconds = max(0, int(current_time - daily_path.stat().st_mtime))
    return DefinitionStatus(main_path=main_path, daily_path=daily_path, daily_age_seconds=daily_age_seconds)


def validate_definition_age(status: DefinitionStatus, max_age_seconds: int) -> None:
    if max_age_seconds <= 0:
        raise RuntimeError("DEFINITIONS_MAX_AGE_SECONDS must be greater than zero")
    if status.daily_age_seconds > max_age_seconds:
        raise RuntimeError(
            f"Daily definitions are stale: age={status.daily_age_seconds}s maximum={max_age_seconds}s path={status.daily_path}"
        )


def clamd_command(socket_path: str, command: bytes, timeout_seconds: float = 5.0) -> str:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout_seconds)
        connection.connect(socket_path)
        connection.sendall(b"z" + command + b"\0")
        chunks: list[bytes] = []
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                raise RuntimeError("clamd closed the health-check connection without a reply")
            terminator = chunk.find(b"\0")
            if terminator >= 0:
                chunks.append(chunk[:terminator])
                return b"".join(chunks).decode("utf-8", "replace").strip()
            chunks.append(chunk)


def check_clamd(socket_path: str) -> str:
    ping_reply = clamd_command(socket_path, b"PING")
    if ping_reply != "PONG":
        raise RuntimeError(f"Unexpected clamd PING reply: {ping_reply!r}")
    version_reply = clamd_command(socket_path, b"VERSION")
    if not version_reply.startswith("ClamAV "):
        raise RuntimeError(f"Unexpected clamd VERSION reply: {version_reply!r}")
    return version_reply


def check_ui(port: int, timeout_seconds: float = 5.0) -> dict[str, object]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/healthz",
        headers={"User-Agent": "clamav-healthcheck"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                raise RuntimeError(f"UI health endpoint returned HTTP {response.status}")
            body = response.read(65537)
            if len(body) > 65536:
                raise RuntimeError("UI health endpoint returned an oversized response")
            try:
                payload = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("UI health endpoint returned invalid JSON") from exc
            if not isinstance(payload, dict) or payload.get("ok") is not True:
                raise RuntimeError("UI health endpoint did not report a ready application")
            return payload
    except urllib.error.HTTPError as exc:
        body = exc.read(65537)
        detail = ""
        if len(body) <= 65536:
            try:
                payload = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = None
            if isinstance(payload, dict):
                reason = payload.get("reason")
                phase = payload.get("phase")
                if isinstance(reason, str) and reason:
                    detail = reason
                elif isinstance(phase, str) and phase:
                    detail = f"phase={phase}"
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"UI health endpoint returned HTTP {exc.code}{suffix}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"UI health endpoint is unavailable: {exc.reason}") from exc


def parse_positive_env(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError:
        raise RuntimeError(f"{name} must be a positive integer") from None
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def wait_for_definitions(directory: str, max_age_seconds: int, timeout_seconds: int, stale_action: str) -> None:
    if stale_action not in {"warn", "fail"}:
        raise RuntimeError("DEFINITIONS_STALE_ACTION must be 'warn' or 'fail'")
    deadline = time.monotonic() + timeout_seconds
    last_error = "definitions are not ready"
    while True:
        try:
            status = definition_status(directory)
            try:
                validate_definition_age(status, max_age_seconds)
            except RuntimeError as exc:
                if stale_action == "warn":
                    print(f"[WARN] {exc}", file=sys.stderr, flush=True)
                    return
                raise
            print(
                f"Definitions ready: main={status.main_path.name} daily={status.daily_path.name} daily_age={status.daily_age_seconds}s",
                flush=True,
            )
            return
        except (OSError, RuntimeError) as exc:
            last_error = str(exc)

        if time.monotonic() >= deadline:
            raise RuntimeError(f"Definitions did not become ready within {timeout_seconds}s: {last_error}")
        time.sleep(min(2.0, max(0.1, deadline - time.monotonic())))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--wait", action="store_true", help="wait for definitions before clamd starts")
    action.add_argument("--reload", action="store_true", help="request an immediate clamd database reload")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    definitions_dir = os.environ.get("DEFINITIONS_DIR", DEFAULT_DEFINITIONS_DIR)
    runtime_dir = os.environ.get("RUNTIME_DIR", str(Path(DEFAULT_SOCKET_PATH).parent))
    socket_path = os.environ.get("CLAMD_SOCKET", os.path.join(runtime_dir, "clamd.sock"))

    try:
        if args.reload:
            reply = clamd_command(socket_path, b"RELOAD")
            if "RELOADING" not in reply.upper():
                raise RuntimeError(f"Unexpected clamd RELOAD reply: {reply!r}")
            print(reply)
            return 0

        max_age_seconds = parse_positive_env("DEFINITIONS_MAX_AGE_SECONDS", DEFAULT_MAX_AGE_SECONDS)
        if args.wait:
            timeout_seconds = parse_positive_env("DEFINITIONS_WAIT_TIMEOUT", 300)
            stale_action = os.environ.get("DEFINITIONS_STALE_ACTION", "warn").strip().lower()
            wait_for_definitions(definitions_dir, max_age_seconds, timeout_seconds, stale_action)
            return 0

        status = definition_status(definitions_dir)
        validate_definition_age(status, max_age_seconds)

        app_mode = os.environ.get("APP_MODE", "headless").strip().lower() or "headless"
        if app_mode == "ui":
            ui_port = parse_positive_env("UI_PORT", 8080)
            if ui_port > 65535:
                raise RuntimeError("UI_PORT must not exceed 65535")
            ui_status = check_ui(ui_port)
            if ui_status.get("configured") is True:
                version = check_clamd(socket_path)
                print(f"healthy: UI ready; {version}; daily definitions age={status.daily_age_seconds}s")
            else:
                print(f"healthy: UI ready for initial configuration; daily definitions age={status.daily_age_seconds}s")
        elif app_mode == "headless":
            version = check_clamd(socket_path)
            print(f"healthy: {version}; daily definitions age={status.daily_age_seconds}s")
        else:
            raise RuntimeError(f"Unsupported APP_MODE={app_mode!r}")
    except (OSError, RuntimeError) as exc:
        print(f"unhealthy: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
