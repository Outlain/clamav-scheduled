#!/usr/bin/env python3
"""Atomically write notifier events for clamav-scheduled."""

from __future__ import annotations

import argparse
import json
import os
import stat
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SERVICE = "clamav-scheduled"


def emit_event(
    event_dir: Path,
    event_type: str,
    severity: str,
    message: str,
    *,
    event_id: str | None = None,
    **fields: Any,
) -> Path:
    event_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
    identifier = event_id or str(uuid.uuid4())
    payload: dict[str, Any] = {
        "schema_version": 1,
        "event_id": identifier,
        "event_type": event_type,
        "service": SERVICE,
        "severity": severity,
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "message": message[:2000],
    }
    payload.update({key: value for key, value in fields.items() if value is not None})
    directory_info = event_dir.lstat()
    if not stat.S_ISDIR(directory_info.st_mode) or stat.S_ISLNK(directory_info.st_mode):
        raise RuntimeError(f"event path is not a real directory: {event_dir}")
    descriptor, name = tempfile.mkstemp(prefix=".event-", suffix=".tmp", dir=event_dir)
    temporary = Path(name)
    destination = event_dir / f"{identifier}.json"
    try:
        os.fchmod(descriptor, 0o640)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(event_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return destination
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-dir", default=os.environ.get("EVENT_DIR", "/events"))
    parser.add_argument("--event-type", required=True)
    parser.add_argument("--severity", choices=("info", "warning", "critical"), required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--source-path")
    parser.add_argument("--destination-path")
    parser.add_argument("--threat-name")
    parser.add_argument("--scan-type")
    parser.add_argument("--action-success", choices=("true", "false"))
    args = parser.parse_args()
    fields: dict[str, Any] = {
        "source_path": args.source_path,
        "destination_path": args.destination_path,
        "threat_name": args.threat_name,
        "scan_type": args.scan_type,
    }
    if args.action_success is not None:
        fields["action_success"] = args.action_success == "true"
    emit_event(
        Path(args.event_dir),
        args.event_type,
        args.severity,
        args.message,
        **fields,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
