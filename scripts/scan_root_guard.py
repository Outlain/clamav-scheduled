#!/usr/bin/env python3
"""Capture and verify scan-root mount identities around a scan."""

from __future__ import annotations

import argparse
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any


def identity(path: Path, *, require_directory: bool) -> dict[str, int]:
    info = path.lstat()
    if require_directory and not stat.S_ISDIR(info.st_mode):
        raise RuntimeError(f"scan root is not a directory: {path}")
    if stat.S_ISLNK(info.st_mode):
        raise RuntimeError(f"refusing a symlink mount guard path: {path}")
    return {"device": info.st_dev, "inode": info.st_ino, "type": stat.S_IFMT(info.st_mode)}


def marker_path_within(root: Path, marker: str) -> Path:
    relative = Path(marker)
    if relative.is_absolute() or relative == Path(".") or ".." in relative.parts:
        raise RuntimeError(f"scan-root marker must be a relative path inside its root: {marker}")
    root_resolved = root.resolve(strict=True)
    candidate = root / relative
    # Reject a final symlink before resolving intermediate components.
    identity(candidate, require_directory=False)
    try:
        candidate.resolve(strict=True).relative_to(root_resolved)
    except (OSError, RuntimeError, ValueError) as exc:
        raise RuntimeError(f"scan-root marker escapes its root: {candidate}") from exc
    return candidate


def capture(roots: list[Path], marker: str) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for root in roots:
        if not os.access(root, os.R_OK | os.X_OK):
            raise RuntimeError(f"scan root is not readable: {root}")
        record: dict[str, Any] = {"path": str(root), "identity": identity(root, require_directory=True)}
        if marker:
            marker_path = marker_path_within(root, marker)
            record["marker_path"] = str(marker_path)
            record["marker_identity"] = identity(marker_path, require_directory=False)
        records.append(record)
    return {"version": 1, "roots": records}


def write_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def verify(payload: dict[str, Any]) -> None:
    if payload.get("version") != 1 or not isinstance(payload.get("roots"), list):
        raise RuntimeError("invalid scan-root guard state")
    for record in payload["roots"]:
        root = Path(record["path"])
        if identity(root, require_directory=True) != record["identity"]:
            raise RuntimeError(f"scan root identity changed during scan: {root}")
        if not os.access(root, os.R_OK | os.X_OK):
            raise RuntimeError(f"scan root became unreadable during scan: {root}")
        marker_path = record.get("marker_path")
        if marker_path and identity(Path(marker_path), require_directory=False) != record.get("marker_identity"):
            raise RuntimeError(f"scan-root marker identity changed during scan: {marker_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture_parser = subparsers.add_parser("capture")
    capture_parser.add_argument("--roots", required=True)
    capture_parser.add_argument("--marker", default="")
    capture_parser.add_argument("--output", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--input", required=True)
    args = parser.parse_args()

    try:
        if args.command == "capture":
            roots = [Path(value) for value in args.roots.split(":") if value]
            if not roots:
                raise RuntimeError("at least one scan root is required")
            write_atomic(Path(args.output), capture(roots, args.marker))
        else:
            with Path(args.input).open("r", encoding="utf-8") as handle:
                verify(json.load(handle))
    except (OSError, RuntimeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"scan-root guard failed: {exc}", file=os.sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
