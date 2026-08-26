#!/usr/bin/env python3
"""Manage one durable, restart-safe scheduled-scan session.

The helper owns only the lifecycle of the active session directory.  The
scanner owns the contents of ``roots.json``, ``files.nul``, and the append-only
``completed.jsonl`` journal.  A session is resumable only after ``mark-ready``
atomically changes its state from BUILDING to READY.

CLI output is a sequence of POSIX-shell assignments.  Values are quoted with
``shlex.quote`` and all names use the ``PROGRESS_`` prefix.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import stat
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


SCHEMA_VERSION = 1
ACTIVE_DIRECTORY_NAME = "active"
SESSION_FILE_NAME = "session.json"
ROOTS_FILE_NAME = "roots.json"
FILES_FILE_NAME = "files.nul"
COMPLETED_FILE_NAME = "completed.jsonl"
KNOWN_FILE_NAMES = (
    SESSION_FILE_NAME,
    ROOTS_FILE_NAME,
    FILES_FILE_NAME,
    COMPLETED_FILE_NAME,
)
VALID_LABELS = frozenset({"FULL", "CHANGED"})
VALID_STATES = frozenset({"BUILDING", "READY"})
SCAN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
MAX_SESSION_JSON_BYTES = 64 * 1024
MAX_ROOTS_JSON_BYTES = 4 * 1024 * 1024
MAX_COMPLETION_RECORD_BYTES = 1024 * 1024


class ProgressStateError(RuntimeError):
    """The durable progress state is invalid or unsafe to use."""


@dataclass(frozen=True)
class SessionPaths:
    progress_root: Path
    session_dir: Path
    session_json: Path
    roots_json: Path
    files_nul: Path
    completed_jsonl: Path


@dataclass(frozen=True)
class PrepareResult:
    action: str
    session: dict[str, Any]
    paths: SessionPaths
    completed_records: int = 0


def _contains_control_characters(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _validate_text(name: str, value: str, *, maximum: int) -> str:
    if not value or len(value) > maximum or _contains_control_characters(value):
        raise ProgressStateError(
            f"{name} must contain 1..{maximum} characters and no control characters"
        )
    return value


def _validate_nonnegative_epoch(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProgressStateError(f"{name} must be a nonnegative integer")
    return value


def _paths(progress_root: Path) -> SessionPaths:
    active = progress_root / ACTIVE_DIRECTORY_NAME
    return SessionPaths(
        progress_root=progress_root,
        session_dir=active,
        session_json=active / SESSION_FILE_NAME,
        roots_json=active / ROOTS_FILE_NAME,
        files_nul=active / FILES_FILE_NAME,
        completed_jsonl=active / COMPLETED_FILE_NAME,
    )


def _lstat(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _require_directory(path: Path, description: str) -> os.stat_result:
    info = _lstat(path)
    if info is None:
        raise ProgressStateError(f"{description} does not exist: {path}")
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ProgressStateError(f"{description} must be a real directory, not a symlink: {path}")
    return info


def _require_regular_file(path: Path, description: str) -> os.stat_result:
    info = _lstat(path)
    if info is None:
        raise ProgressStateError(f"{description} is missing: {path}")
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ProgressStateError(f"{description} must be a regular file, not a symlink: {path}")
    if info.st_nlink != 1:
        raise ProgressStateError(f"{description} must not be hard-linked: {path}")
    return info


def _open_regular(path: Path, flags: int) -> int:
    descriptor = os.open(path, flags | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ProgressStateError(f"durable session file is not a private regular file: {path}")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_progress_root(progress_root: Path, *, create: bool, writable: bool) -> Path:
    raw = os.fspath(progress_root)
    if not os.path.isabs(raw):
        raise ProgressStateError(f"progress root must be absolute: {raw}")
    if _contains_control_characters(raw):
        raise ProgressStateError("progress root must not contain control characters")

    info = _lstat(progress_root)
    if info is None:
        if not create:
            return progress_root
        try:
            progress_root.mkdir(mode=0o750)
        except OSError as exc:
            raise ProgressStateError(f"could not create progress root {progress_root}: {exc}") from exc
        _fsync_directory(progress_root.parent)
    else:
        _require_directory(progress_root, "progress root")

    access_mode = os.R_OK | os.X_OK | (os.W_OK if writable else 0)
    if not os.access(progress_root, access_mode):
        requirement = "readable, writable, and searchable" if writable else "readable and searchable"
        raise ProgressStateError(f"progress root must be {requirement}: {progress_root}")

    if writable:
        try:
            descriptor, probe_name = tempfile.mkstemp(prefix=".write-probe-", dir=progress_root)
            os.close(descriptor)
            os.unlink(probe_name)
            _fsync_directory(progress_root)
        except OSError as exc:
            raise ProgressStateError(
                f"progress root failed its create/delete/fsync probe: {progress_root}: {exc}"
            ) from exc
    return progress_root


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(payload, handle, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _create_empty_file(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_json_file(path: Path, description: str, maximum_bytes: int) -> dict[str, Any]:
    info = _require_regular_file(path, description)
    if info.st_size > maximum_bytes:
        raise ProgressStateError(f"{description} is unexpectedly large: {path}")
    descriptor = _open_regular(path, os.O_RDONLY)
    try:
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            payload = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProgressStateError(f"{description} is invalid: {path}: {exc}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not isinstance(payload, dict):
        raise ProgressStateError(f"{description} must contain a JSON object: {path}")
    return payload


def _validate_session_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("version") != SCHEMA_VERSION:
        raise ProgressStateError("unsupported durable scan session version")
    if payload.get("state") not in VALID_STATES:
        raise ProgressStateError("durable scan session has an invalid state")
    scan_id = payload.get("scan_id")
    if not isinstance(scan_id, str) or not SCAN_ID_RE.fullmatch(scan_id):
        raise ProgressStateError("durable scan session has an invalid scan_id")
    if payload.get("label") not in VALID_LABELS:
        raise ProgressStateError("durable scan session has an invalid label")
    for name, maximum in (
        ("fingerprint", 512),
        ("request_identity", 4096),
        ("request_mode", 128),
    ):
        value = payload.get(name)
        if not isinstance(value, str):
            raise ProgressStateError(f"durable scan session has an invalid {name}")
        _validate_text(name, value, maximum=maximum)
    for name in (
        "cutoff_epoch",
        "reference_epoch",
        "created_epoch",
        "updated_epoch",
        "max_age_seconds",
        "resume_count",
    ):
        _validate_nonnegative_epoch(name, payload.get(name))
    if payload["updated_epoch"] < payload["created_epoch"]:
        raise ProgressStateError("durable scan session timestamps are inconsistent")
    return payload


def _validate_active_file_names(paths: SessionPaths) -> None:
    _require_directory(paths.session_dir, "active session directory")
    try:
        names = {entry.name for entry in os.scandir(paths.session_dir)}
    except OSError as exc:
        raise ProgressStateError(f"could not inspect active session directory: {exc}") from exc
    expected = set(KNOWN_FILE_NAMES)
    if names != expected:
        unknown = sorted(names - expected)
        missing = sorted(expected - names)
        raise ProgressStateError(
            f"active session layout is invalid (missing={missing}, unexpected={unknown})"
        )
    _require_regular_file(paths.session_json, "session metadata")
    _require_regular_file(paths.roots_json, "root guard")
    _require_regular_file(paths.files_nul, "NUL file manifest")
    _require_regular_file(paths.completed_jsonl, "completion journal")


def _validate_nul_manifest(path: Path) -> None:
    info = _require_regular_file(path, "NUL file manifest")
    if info.st_size == 0:
        return
    descriptor = _open_regular(path, os.O_RDONLY)
    try:
        os.lseek(descriptor, -1, os.SEEK_END)
        if os.read(descriptor, 1) != b"\0":
            raise ProgressStateError("NUL file manifest is missing its final terminator")
    finally:
        os.close(descriptor)


def _validate_roots_payload(payload: dict[str, Any]) -> None:
    if payload.get("version") != 1 or not isinstance(payload.get("roots"), list):
        raise ProgressStateError("root guard must be a version-1 object containing a roots list")


def _completion_lines(descriptor: int) -> Iterator[tuple[int | None, bytes]]:
    offset = 0
    pending = b""
    while True:
        chunk = os.read(descriptor, 64 * 1024)
        if not chunk:
            break
        pending += chunk
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            offset += len(line) + 1
            yield offset, line
        if len(pending) > MAX_COMPLETION_RECORD_BYTES:
            raise ProgressStateError("completion journal contains an oversized record")
    if pending:
        yield None, pending


def _validate_completion_journal(
    path: Path,
    scan_id: str,
    *,
    repair_truncated_tail: bool,
) -> int:
    _require_regular_file(path, "completion journal")
    flags = os.O_RDWR if repair_truncated_tail else os.O_RDONLY
    descriptor = _open_regular(path, flags)
    records = 0
    last_complete_offset = 0
    truncated_tail = False
    try:
        for next_offset, raw_line in _completion_lines(descriptor):
            if next_offset is None:
                truncated_tail = True
                break
            last_complete_offset = next_offset
            if not raw_line or len(raw_line) > MAX_COMPLETION_RECORD_BYTES:
                raise ProgressStateError("completion journal contains an invalid record length")
            try:
                payload = json.loads(raw_line)
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ProgressStateError(f"completion journal contains invalid JSON: {exc}") from exc
            if not isinstance(payload, dict):
                raise ProgressStateError("completion journal records must be JSON objects")
            record_scan_id = payload.get("scan_id")
            if record_scan_id != scan_id:
                raise ProgressStateError("completion journal contains a record for another scan")
            records += 1

        if truncated_tail:
            if not repair_truncated_tail:
                raise ProgressStateError("completion journal has a truncated final record")
            os.ftruncate(descriptor, last_complete_offset)
            os.fsync(descriptor)
            _fsync_directory(path.parent)
    finally:
        os.close(descriptor)
    return records


def _load_active(paths: SessionPaths, *, repair_journal: bool) -> tuple[dict[str, Any], int]:
    _validate_active_file_names(paths)
    session = _validate_session_payload(
        _read_json_file(paths.session_json, "session metadata", MAX_SESSION_JSON_BYTES)
    )
    roots_payload = _read_json_file(paths.roots_json, "root guard", MAX_ROOTS_JSON_BYTES)
    _validate_roots_payload(roots_payload)
    if session["state"] == "READY":
        _validate_nul_manifest(paths.files_nul)
    completed = _validate_completion_journal(
        paths.completed_jsonl,
        session["scan_id"],
        repair_truncated_tail=repair_journal,
    )
    return session, completed


def _remove_exact_session_directory(active: Path) -> None:
    info = _lstat(active)
    if info is None:
        return
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ProgressStateError(f"refusing to remove unsafe active session path: {active}")

    try:
        names = {entry.name for entry in os.scandir(active)}
    except OSError as exc:
        raise ProgressStateError(f"could not inspect active session directory: {exc}") from exc
    unexpected = names - set(KNOWN_FILE_NAMES)
    if unexpected:
        raise ProgressStateError(
            f"refusing to remove active session with unexpected entries: {sorted(unexpected)}"
        )

    for name in KNOWN_FILE_NAMES:
        path = active / name
        entry_info = _lstat(path)
        if entry_info is None:
            continue
        if stat.S_ISDIR(entry_info.st_mode):
            raise ProgressStateError(f"refusing to remove directory in place of session file: {path}")
        os.unlink(path)
    active.rmdir()


def _initialize_session(
    paths: SessionPaths,
    *,
    fingerprint: str,
    label: str,
    cutoff_epoch: int,
    reference_epoch: int,
    request_identity: str,
    request_mode: str,
    max_age_seconds: int,
    now_epoch: int,
) -> dict[str, Any]:
    scan_id = uuid.uuid4().hex
    payload: dict[str, Any] = {
        "version": SCHEMA_VERSION,
        "scan_id": scan_id,
        "state": "BUILDING",
        "fingerprint": fingerprint,
        "label": label,
        "cutoff_epoch": cutoff_epoch,
        "reference_epoch": reference_epoch,
        "request_identity": request_identity,
        "request_mode": request_mode,
        "max_age_seconds": max_age_seconds,
        "created_epoch": now_epoch,
        "updated_epoch": now_epoch,
        "resume_count": 0,
    }

    temporary = Path(tempfile.mkdtemp(prefix=".active-build-", dir=paths.progress_root))
    os.chmod(temporary, 0o700)
    published = False
    try:
        _atomic_write_json(temporary / SESSION_FILE_NAME, payload)
        _atomic_write_json(temporary / ROOTS_FILE_NAME, {"version": 1, "roots": []})
        _create_empty_file(temporary / FILES_FILE_NAME)
        _create_empty_file(temporary / COMPLETED_FILE_NAME)
        _fsync_directory(temporary)

        _remove_exact_session_directory(paths.session_dir)
        os.replace(temporary, paths.session_dir)
        published = True
        _fsync_directory(paths.progress_root)
    finally:
        if not published and _lstat(temporary) is not None:
            try:
                _remove_exact_session_directory(temporary)
            except (OSError, ProgressStateError):
                pass
    return payload


def _session_is_compatible(
    session: dict[str, Any],
    *,
    fingerprint: str,
    label: str,
    request_identity: str,
    request_mode: str,
    max_age_seconds: int,
    now_epoch: int,
) -> bool:
    if session["state"] != "READY":
        return False
    if (
        session["fingerprint"] != fingerprint
        or session["label"] != label
        or session["request_identity"] != request_identity
        or session["request_mode"] != request_mode
    ):
        return False
    return max_age_seconds == 0 or max(0, now_epoch - session["created_epoch"]) <= max_age_seconds


def prepare_session(
    progress_root: Path,
    *,
    fingerprint: str,
    label: str,
    cutoff_epoch: int,
    reference_epoch: int,
    request_identity: str,
    request_mode: str,
    max_age_seconds: int,
    now_epoch: int | None = None,
) -> PrepareResult:
    progress_root = _validate_progress_root(Path(progress_root), create=True, writable=True)
    fingerprint = _validate_text("fingerprint", fingerprint, maximum=512)
    request_identity = _validate_text("request identity", request_identity, maximum=4096)
    request_mode = _validate_text("request mode", request_mode, maximum=128)
    if label not in VALID_LABELS:
        raise ProgressStateError("label must be FULL or CHANGED")
    cutoff_epoch = _validate_nonnegative_epoch("cutoff epoch", cutoff_epoch)
    reference_epoch = _validate_nonnegative_epoch("reference epoch", reference_epoch)
    max_age_seconds = _validate_nonnegative_epoch("max age seconds", max_age_seconds)
    current_epoch = int(time.time()) if now_epoch is None else now_epoch
    current_epoch = _validate_nonnegative_epoch("current epoch", current_epoch)
    paths = _paths(progress_root)

    session: dict[str, Any] | None = None
    completed_records = 0
    if _lstat(paths.session_dir) is not None:
        try:
            session, completed_records = _load_active(paths, repair_journal=True)
        except ProgressStateError:
            session = None

    if session is not None and _session_is_compatible(
        session,
        fingerprint=fingerprint,
        label=label,
        request_identity=request_identity,
        request_mode=request_mode,
        max_age_seconds=max_age_seconds,
        now_epoch=current_epoch,
    ):
        session = dict(session)
        session["updated_epoch"] = max(current_epoch, session["created_epoch"])
        session["resume_count"] += 1
        _atomic_write_json(paths.session_json, session)
        return PrepareResult("RESUME", session, paths, completed_records)

    session = _initialize_session(
        paths,
        fingerprint=fingerprint,
        label=label,
        cutoff_epoch=cutoff_epoch,
        reference_epoch=reference_epoch,
        request_identity=request_identity,
        request_mode=request_mode,
        max_age_seconds=max_age_seconds,
        now_epoch=current_epoch,
    )
    return PrepareResult("NEW", session, paths, 0)


def status_session(progress_root: Path) -> PrepareResult | None:
    # The scheduler calls status before due-time arbitration. Repairing only an
    # unterminated final JSONL record here lets a power-loss-interrupted CHANGED
    # session retain priority over a newly due FULL scan. Complete malformed
    # records still fail validation and are never trusted.
    progress_root = _validate_progress_root(Path(progress_root), create=False, writable=True)
    paths = _paths(progress_root)
    if _lstat(progress_root) is None or _lstat(paths.session_dir) is None:
        return None
    session, completed_records = _load_active(paths, repair_journal=True)
    return PrepareResult("STATUS", session, paths, completed_records)


def mark_ready(progress_root: Path, scan_id: str, *, now_epoch: int | None = None) -> PrepareResult:
    progress_root = _validate_progress_root(Path(progress_root), create=False, writable=True)
    _validate_text("scan id", scan_id, maximum=64)
    paths = _paths(progress_root)
    session, completed_records = _load_active(paths, repair_journal=False)
    if session["scan_id"] != scan_id:
        raise ProgressStateError("active scan_id does not match the expected scan_id")
    if session["state"] != "BUILDING":
        raise ProgressStateError("only a BUILDING session can be marked READY")
    if completed_records:
        raise ProgressStateError("a BUILDING session must have an empty completion journal")

    roots_payload = _read_json_file(paths.roots_json, "root guard", MAX_ROOTS_JSON_BYTES)
    _validate_roots_payload(roots_payload)
    _validate_nul_manifest(paths.files_nul)
    for durable_file in (paths.roots_json, paths.files_nul, paths.completed_jsonl):
        descriptor = _open_regular(durable_file, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    current_epoch = int(time.time()) if now_epoch is None else now_epoch
    current_epoch = _validate_nonnegative_epoch("current epoch", current_epoch)
    session = dict(session)
    session["state"] = "READY"
    session["updated_epoch"] = max(current_epoch, session["created_epoch"])
    _atomic_write_json(paths.session_json, session)
    return PrepareResult("READY", session, paths, 0)


def clear_session(progress_root: Path, scan_id: str) -> None:
    progress_root = _validate_progress_root(Path(progress_root), create=False, writable=True)
    _validate_text("scan id", scan_id, maximum=64)
    paths = _paths(progress_root)
    _require_directory(paths.session_dir, "active session directory")
    session = _validate_session_payload(
        _read_json_file(paths.session_json, "session metadata", MAX_SESSION_JSON_BYTES)
    )
    if session["scan_id"] != scan_id:
        raise ProgressStateError("active scan_id does not match the expected scan_id")
    _remove_exact_session_directory(paths.session_dir)
    _fsync_directory(progress_root)


def _shell_fields(result: PrepareResult) -> dict[str, str | int]:
    session = result.session
    paths = result.paths
    return {
        "PROGRESS_ACTION": result.action,
        "PROGRESS_VERSION": session["version"],
        "PROGRESS_STATE": session["state"],
        "PROGRESS_SCAN_ID": session["scan_id"],
        "PROGRESS_LABEL": session["label"],
        "PROGRESS_FINGERPRINT": session["fingerprint"],
        "PROGRESS_CUTOFF_EPOCH": session["cutoff_epoch"],
        "PROGRESS_REFERENCE_EPOCH": session["reference_epoch"],
        "PROGRESS_REQUEST_IDENTITY": session["request_identity"],
        "PROGRESS_REQUEST_MODE": session["request_mode"],
        "PROGRESS_CREATED_EPOCH": session["created_epoch"],
        "PROGRESS_UPDATED_EPOCH": session["updated_epoch"],
        "PROGRESS_RESUME_COUNT": session["resume_count"],
        "PROGRESS_COMPLETED_RECORDS": result.completed_records,
        "PROGRESS_SESSION_DIR": os.fspath(paths.session_dir),
        "PROGRESS_SESSION_JSON": os.fspath(paths.session_json),
        "PROGRESS_ROOTS_JSON": os.fspath(paths.roots_json),
        "PROGRESS_FILES_NUL": os.fspath(paths.files_nul),
        "PROGRESS_COMPLETED_JSONL": os.fspath(paths.completed_jsonl),
    }


def _print_shell_fields(fields: dict[str, str | int]) -> None:
    for key, value in fields.items():
        print(f"{key}={shlex.quote(str(value))}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="create or resume an active session")
    prepare_parser.add_argument("--progress-root", required=True, type=Path)
    prepare_parser.add_argument("--fingerprint", required=True)
    prepare_parser.add_argument("--label", choices=sorted(VALID_LABELS), required=True)
    prepare_parser.add_argument("--cutoff-epoch", required=True, type=int)
    prepare_parser.add_argument("--reference-epoch", required=True, type=int)
    prepare_parser.add_argument("--request-identity", required=True)
    prepare_parser.add_argument("--request-mode", required=True)
    prepare_parser.add_argument("--max-age-seconds", required=True, type=int)

    status_parser = subparsers.add_parser("status", help="inspect the active session")
    status_parser.add_argument("--progress-root", required=True, type=Path)

    ready_parser = subparsers.add_parser("mark-ready", help="publish a completed manifest")
    ready_parser.add_argument("--progress-root", required=True, type=Path)
    ready_parser.add_argument("--scan-id", required=True)

    clear_parser = subparsers.add_parser("clear", help="remove the expected active session")
    clear_parser.add_argument("--progress-root", required=True, type=Path)
    clear_parser.add_argument("--scan-id", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "prepare":
            result = prepare_session(
                args.progress_root,
                fingerprint=args.fingerprint,
                label=args.label,
                cutoff_epoch=args.cutoff_epoch,
                reference_epoch=args.reference_epoch,
                request_identity=args.request_identity,
                request_mode=args.request_mode,
                max_age_seconds=args.max_age_seconds,
            )
            _print_shell_fields(_shell_fields(result))
        elif args.command == "status":
            result = status_session(args.progress_root)
            if result is None:
                _print_shell_fields({"PROGRESS_ACTION": "NONE", "PROGRESS_STATE": "NONE"})
            else:
                _print_shell_fields(_shell_fields(result))
        elif args.command == "mark-ready":
            result = mark_ready(args.progress_root, args.scan_id)
            _print_shell_fields(_shell_fields(result))
        else:
            clear_session(args.progress_root, args.scan_id)
            _print_shell_fields(
                {"PROGRESS_ACTION": "CLEARED", "PROGRESS_SCAN_ID": args.scan_id}
            )
    except (OSError, ProgressStateError, ValueError) as exc:
        print(f"scan progress operation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
