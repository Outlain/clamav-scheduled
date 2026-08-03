#!/usr/bin/env python3
"""Atomically manage scan checkpoints with legacy-file migration support."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

FILENAME = "scan-checkpoints.json"


def _legacy_epoch(state_dir: Path, name: str) -> int:
    try:
        value = int((state_dir / f"last_{name}_scan_epoch").read_text(encoding="ascii").strip())
        return max(value, 0)
    except (FileNotFoundError, OSError, ValueError):
        return 0


def load_checkpoints(state_dir: Path) -> dict[str, int]:
    try:
        with (state_dir / FILENAME).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict) or value.get("version") != 1:
            raise ValueError("invalid checkpoint state")
        full = int(value.get("last_full_scan_epoch", 0))
        changed = int(value.get("last_changed_scan_epoch", 0))
        if full < 0 or changed < 0:
            raise ValueError("checkpoint epochs must not be negative")
        return {"last_full_scan_epoch": full, "last_changed_scan_epoch": changed}
    except FileNotFoundError:
        return {
            "last_full_scan_epoch": _legacy_epoch(state_dir, "full"),
            "last_changed_scan_epoch": _legacy_epoch(state_dir, "changed"),
        }


def update_checkpoints(state_dir: Path, full: int, changed: int) -> None:
    if full < 0 or changed < 0:
        raise ValueError("checkpoint epochs must not be negative")
    state_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
    destination = state_dir / FILENAME
    descriptor, name = tempfile.mkstemp(prefix=".scan-checkpoints.", dir=state_dir)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            descriptor = -1
            json.dump(
                {
                    "version": 1,
                    "last_full_scan_epoch": full,
                    "last_changed_scan_epoch": changed,
                },
                handle,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    read_parser = subparsers.add_parser("read")
    read_parser.add_argument("--state-dir", required=True)
    read_parser.add_argument("--field", choices=("full", "changed"), required=True)
    update_parser = subparsers.add_parser("update")
    update_parser.add_argument("--state-dir", required=True)
    update_parser.add_argument("--full", type=int, required=True)
    update_parser.add_argument("--changed", type=int, required=True)
    args = parser.parse_args()
    try:
        state_dir = Path(args.state_dir)
        if args.command == "read":
            state = load_checkpoints(state_dir)
            print(state[f"last_{args.field}_scan_epoch"])
        else:
            update_checkpoints(state_dir, args.full, args.changed)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"checkpoint operation failed: {exc}", file=os.sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
