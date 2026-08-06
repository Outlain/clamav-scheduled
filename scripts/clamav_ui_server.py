#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from email.utils import formatdate
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo


DAY_LABELS = {
    1: "Mon",
    2: "Tue",
    3: "Wed",
    4: "Thu",
    5: "Fri",
    6: "Sat",
    7: "Sun",
}

DAY_NAME_TO_NUMBER = {
    "1": 1,
    "mon": 1,
    "monday": 1,
    "2": 2,
    "tue": 2,
    "tues": 2,
    "tuesday": 2,
    "3": 3,
    "wed": 3,
    "weds": 3,
    "wednesday": 3,
    "4": 4,
    "thu": 4,
    "thur": 4,
    "thurs": 4,
    "thursday": 4,
    "5": 5,
    "fri": 5,
    "friday": 5,
    "6": 6,
    "sat": 6,
    "saturday": 6,
    "7": 7,
    "sun": 7,
    "sunday": 7,
}

TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
MAX_JSON_BODY_BYTES = 64 * 1024
MAX_CLAMD_THREADS = 64
MAX_SCAN_WORKERS = 64
MAX_PROGRESS_STEPS = 10_000
MAX_PROGRESS_INTERVAL = 1_000_000
MAX_SCHEDULER_INTERVAL_SECONDS = 86_400
MAX_PATH_CHECK_TIMEOUT_SECONDS = 300
MAX_LOOKBACK_SECONDS = 366 * 24 * 60 * 60
MAX_HTTP_WORKERS = 128
MAX_HTTP_QUEUE = 256
MAX_HTTP_TIMEOUT_SECONDS = 120
MAX_PERSISTED_JSON_BYTES = 4 * 1024 * 1024
SCHEDULER_RESTART_MIN_DELAY_SECONDS = 2
SCHEDULER_RESTART_MAX_DELAY_SECONDS = 60
SCHEDULER_STABLE_RUNTIME_SECONDS = 60

SCAN_START_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Scanning (?P<total>\d+) files with persistent_session_workers=(?P<workers>\d+)$"
)
ENUMERATION_START_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Enumeration started for (?P<path>.+) "
    r"\(timeout=(?P<timeout>\d+)s\)\.$"
)
ENUMERATION_PROGRESS_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Enumeration progress: "
    r"visited_entries=(?P<visited>\d+) new_entries=(?P<new>\d+) "
    r"window_seconds=(?P<window>\d+) raw_list_bytes=(?P<list_bytes>\d+) "
    r"elapsed=(?P<elapsed>\d+)s latest_path=(?P<latest>.+)$"
)
ENUMERATION_COMPLETE_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Enumeration completed for (?P<path>.+): "
    r"eligible_files=(?P<files>\d+) elapsed=(?P<elapsed>\d+)s\.$"
)
FILE_LIST_COMPLETE_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] File-list build completed: "
    r"eligible_files=(?P<files>\d+) sources=(?P<sources>\d+)\.$"
)
INDEXING_START_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Indexing enumerated file list and capturing file identities before scanning\.$"
)
INDEXING_COMPLETE_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Indexing completed: files=(?P<files>\d+) "
    r"bytes=(?P<bytes>.+?) elapsed=(?P<elapsed>.+?)\.$"
)
NO_FILES_RE = re.compile(r"^\[(?P<label>FULL|CHANGED)\] No files found to scan\.$")
SCAN_HEARTBEAT_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Scan heartbeat: "
    r"processed=(?P<processed>\d+)/(?P<total>\d+) queued=(?P<queued>\d+) "
    r"active_workers=(?P<active_workers>\d+) clean=(?P<clean>\d+) infected=(?P<infected>\d+) "
    r"vanished=(?P<vanished>\d+) errors=(?P<errors>\d+) elapsed=(?P<elapsed>.+?)\.$"
)
PROGRESS_CONFIG_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Progress logging uses file-count checkpoints, not scan chunks: "
    r"mode=(?P<mode>\w+) progress_interval=(?P<interval>\d+) (?P<detail>.+)$"
)
PROGRESS_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Progress: (?P<percent>\d+)% "
    r"\((?P<processed>\d+)/(?P<total>\d+)\) "
    r"bytes=(?P<processed_bytes>.+?)/(?P<total_bytes>.+?) "
    r"clean=(?P<clean>\d+) infected=(?P<infected>\d+) vanished=(?P<vanished>\d+) errors=(?P<errors>\d+) "
    r"elapsed=(?P<elapsed>.+?) "
    r"avg_throughput=(?P<avg_throughput>.+?) "
    r"window_throughput=(?P<window_throughput>.+?) "
    r"avg_data_rate=(?P<avg_data_rate>.+?) "
    r"window_data_rate=(?P<window_data_rate>.+)$"
)
SUMMARY_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Summary: "
    r"scheduled_files=(?P<scheduled_files>\d+) indexed_files=(?P<indexed_files>\d+) processed_files=(?P<processed_files>\d+) "
    r"clean=(?P<clean>\d+) infected=(?P<infected>\d+) vanished=(?P<vanished>\d+) errors=(?P<errors>\d+) "
    r"quarantine_failures=(?P<quarantine_failures>\d+) bytes=(?P<bytes>.+?) "
    r"elapsed=(?P<elapsed>.+?) avg_throughput=(?P<avg_throughput>.+?) avg_data_rate=(?P<avg_data_rate>.+)$"
)
ROOT_SUMMARY_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Root summary (?P<root>.+?): "
    r"files=(?P<files>\d+) processed_files=(?P<processed_files>\d+) "
    r"bytes=(?P<bytes>.+?) processed_bytes=(?P<processed_bytes>.+?) "
    r"infected=(?P<infected>\d+) vanished=(?P<vanished>\d+) errors=(?P<errors>\d+)$"
)
CYCLE_START_RE = re.compile(r"^=== (?P<stamp>.+?) Scan cycle starting")
NO_SCANS_RE = re.compile(r"^=== (?P<stamp>.+?) No scans due\. Next wake at (?P<next_wake>.+?) ===$")
FILES_PER_SECOND_RE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)\s+files/s$")
DATA_RATE_RE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)\s+(?P<unit>B|KiB|MiB|GiB|TiB)/s$")
ELAPSED_PART_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>h|m|s)")


BOOTSTRAP_ENV_KEYS = {
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "PYTHONUNBUFFERED",
    "PYTHONDONTWRITEBYTECODE",
    "RUNTIME_DIR",
    "TMP_DIR",
    "DEFINITIONS_DIR",
    "DEFINITIONS_WAIT_TIMEOUT",
    "DEFINITIONS_MAX_AGE_SECONDS",
    "DEFINITIONS_STALE_ACTION",
    "CLAMD_MAX_QUEUE",
    "CLAMD_MAX_SCAN_SIZE",
    "CLAMD_MAX_FILE_SIZE",
    "CLAMD_MAX_RECURSION",
    "CLAMD_MAX_FILES",
    "CLAMD_MAX_SCAN_TIME",
    "CLAMD_READ_TIMEOUT",
    "CLAMD_COMMAND_READ_TIMEOUT",
    "CLAMD_SELF_CHECK",
    "CLAMD_START_TIMEOUT",
    "MAX_SCHEDULED_FILES",
    "SCANLOG_MAX_BYTES",
    "SCANLOG_ROTATIONS",
    "CLAMD_LOG_MAX_SIZE",
    "EVENT_DIR",
    "VANISHED_FILE_FAILURE_COUNT",
    "VANISHED_FILE_FAILURE_PERCENT",
    "VANISHED_FILE_FAILURE_MINIMUM",
}

DEFAULT_CONFIG: dict[str, Any] = {
    "version": 1,
    "tz": "UTC",
    "maxthreads": 13,
    "scan_paths": ["/downloads"],
    "exclude_paths": [],
    "full_scan_parallel_jobs": 8,
    "changed_scan_parallel_jobs": 8,
    "full_progress_steps": 100,
    "changed_progress_steps": 25,
    "full_chunk_size": 0,
    "changed_chunk_size": 0,
    "changed_scan_days": [1, 2, 3, 4, 5, 6, 7],
    "changed_scan_times": ["01:00", "13:00"],
    "full_scan_days": [7],
    "full_scan_times": ["03:30"],
    "scan_failure_retry_interval": 300,
    "force_full_poll_interval": 60,
    "path_check_timeout": 10,
    "path_enumeration_timeout": 1800,
    "path_unavailable_retry_interval": 300,
    "scan_path_marker": "",
    "quarantine_dir": "/quarantine",
    "scanlog": "/var/log/clamav/clamav_scheduled.log",
    "force_full_flag": "/state/force_full_scan.flag",
}

REPAIR_LIST_FIELDS = {
    "scan_paths": ":",
    "exclude_paths": ":",
    "changed_scan_days": ",",
    "changed_scan_times": ",",
    "full_scan_days": ",",
    "full_scan_times": ",",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def health_detail(value: Any) -> str:
    return str(value).replace("\r", " ").replace("\n", " ")[:512]


def config_repair_draft(value: Any) -> dict[str, Any]:
    draft = deepcopy(DEFAULT_CONFIG)
    if not isinstance(value, dict):
        return draft

    for field_name, default_value in DEFAULT_CONFIG.items():
        if field_name not in value:
            continue
        candidate = value[field_name]
        separator = REPAIR_LIST_FIELDS.get(field_name)
        if separator is not None:
            if isinstance(candidate, str):
                draft[field_name] = [part.strip() for part in candidate.split(separator) if part.strip()]
            elif isinstance(candidate, list):
                draft[field_name] = [
                    item for item in candidate if isinstance(item, (str, int, float)) and not isinstance(item, bool)
                ]
            continue
        if isinstance(default_value, int):
            if isinstance(candidate, (str, int, float)) and not isinstance(candidate, bool):
                draft[field_name] = candidate
            continue
        if isinstance(default_value, str) and isinstance(candidate, (str, int, float)):
            draft[field_name] = str(candidate)
    return draft


def read_json(path: Path, default: Any = None) -> Any:
    try:
        with path.open("rb") as handle:
            content = handle.read(MAX_PERSISTED_JSON_BYTES + 1)
    except FileNotFoundError:
        return default
    if len(content) > MAX_PERSISTED_JSON_BYTES:
        raise ValueError(f"Persisted JSON exceeds the {MAX_PERSISTED_JSON_BYTES}-byte limit: {path}")
    return json.loads(content.decode("utf-8"))


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_mode = 0o600
    try:
        existing_mode = path.stat().st_mode & 0o777
    except FileNotFoundError:
        pass

    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        os.fchmod(descriptor, existing_mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        try:
            directory_descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError:
            pass
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def write_json_atomic(path: Path, payload: Any) -> None:
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    write_text_atomic(path, content)


def probe_writable_directory(path: Path, field_name: str, *, create: bool) -> None:
    try:
        if create:
            path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise ValueError(f"{field_name} is not a directory: {path}")
        descriptor, probe_name = tempfile.mkstemp(prefix=".clamav-write-probe.", dir=path)
        os.close(descriptor)
        Path(probe_name).unlink()
    except ValueError:
        raise
    except OSError as exc:
        raise ValueError(f"{field_name} is not writable by the container user: {path} ({exc})") from exc


def validate_runtime_permissions(config: dict[str, Any], config_dir: Path, state_dir: Path) -> None:
    probe_writable_directory(config_dir, "CONFIG_DIR", create=True)
    probe_writable_directory(state_dir, "STATE_DIR", create=True)
    for scan_path in config["scan_paths"]:
        probe_writable_directory(
            Path(scan_path),
            "Scan root (write access is required to remove quarantined threats)",
            create=False,
        )
    probe_writable_directory(Path(config["quarantine_dir"]), "quarantine_dir", create=True)
    scanlog_path = Path(config["scanlog"])
    probe_writable_directory(scanlog_path.parent, "scanlog parent directory", create=True)
    try:
        descriptor = os.open(
            scanlog_path,
            os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.close(descriptor)
    except OSError as exc:
        raise ValueError(f"scanlog is not writable by the container user: {scanlog_path} ({exc})") from exc


def normalize_path_entry(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} entries must be strings.")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field_name} entries must not contain control characters.")
    entry = value.strip()
    if not entry:
        raise ValueError(f"{field_name} entries must not be empty.")
    if ":" in entry:
        raise ValueError(f"{field_name} entries must not contain ':' because ':' separates paths.")
    if not os.path.isabs(entry):
        raise ValueError(f"{field_name} entries must be absolute paths: {entry}")
    normalized = os.path.normpath(entry)
    return normalized


def normalize_path_list(value: Any, field_name: str, required: bool) -> list[str]:
    if value is None:
        entries: list[str] = []
    elif isinstance(value, str):
        if "\r" in value or "\n" in value:
            raise ValueError(f"{field_name} must not contain line breaks.")
        entries = value.split(":")
    elif isinstance(value, list):
        entries = value
    else:
        raise ValueError(f"{field_name} must be a list of absolute paths.")

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_entry in entries:
        if not isinstance(raw_entry, str):
            raise ValueError(f"{field_name} entries must be strings.")
        if not raw_entry.strip():
            continue
        normalized_entry = normalize_path_entry(raw_entry, field_name)
        if normalized_entry not in seen:
            seen.add(normalized_entry)
            normalized.append(normalized_entry)

    if required and not normalized:
        raise ValueError(f"{field_name} must include at least one absolute path.")

    return normalized


def canonicalize_path_entry(value: str, field_name: str, *, require_existing: bool) -> str:
    normalized = normalize_path_entry(value, field_name)
    try:
        return str(Path(normalized).resolve(strict=require_existing))
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        if require_existing:
            raise ValueError(
                f"{field_name} does not exist or cannot be resolved inside the container: {normalized}"
            ) from exc
        raise ValueError(f"{field_name} cannot be resolved inside the container: {normalized}") from exc


def normalize_scan_path_marker(value: Any) -> str:
    marker = normalize_optional_string(value)
    if not marker:
        return ""
    if marker in {".", ".."} or "/" in marker or "\\" in marker:
        raise ValueError("scan_path_marker must be a single file or directory name, not a path.")
    if any(ord(character) < 32 or ord(character) == 127 for character in marker):
        raise ValueError("scan_path_marker must not contain control characters.")
    return marker


def normalize_days(value: Any, field_name: str) -> list[int]:
    if value is None or value == "":
        raise ValueError(f"{field_name} must include at least one day.")

    if isinstance(value, str):
        tokens = [part.strip().lower() for part in value.split(",")]
    elif isinstance(value, list):
        tokens = [str(part).strip().lower() for part in value]
    else:
        raise ValueError(f"{field_name} must be a list of day values.")

    normalized: list[int] = []
    seen: set[int] = set()
    for token in tokens:
        if not token:
            continue
        if token == "*":
            return [1, 2, 3, 4, 5, 6, 7]
        if token not in DAY_NAME_TO_NUMBER:
            raise ValueError(f"Invalid day value in {field_name}: {token}")
        day_number = DAY_NAME_TO_NUMBER[token]
        if day_number not in seen:
            seen.add(day_number)
            normalized.append(day_number)

    if not normalized:
        raise ValueError(f"{field_name} must include at least one day.")

    return sorted(normalized)


def normalize_times(value: Any, field_name: str) -> list[str]:
    if value is None or value == "":
        raise ValueError(f"{field_name} must include at least one time.")

    if isinstance(value, str):
        tokens = [part.strip() for part in value.split(",")]
    elif isinstance(value, list):
        tokens = [str(part).strip() for part in value]
    else:
        raise ValueError(f"{field_name} must be a list of HH:MM values.")

    normalized: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if not token:
            continue
        if not TIME_RE.match(token):
            raise ValueError(f"Invalid time value in {field_name}: {token}")
        if token not in seen:
            seen.add(token)
            normalized.append(token)

    if not normalized:
        raise ValueError(f"{field_name} must include at least one time.")

    return sorted(normalized)


def normalize_optional_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_int(value: Any, field_name: str, minimum: int = 0, maximum: int | None = None) -> int:
    try:
        integer = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be an integer.") from None

    if integer < minimum:
        comparator = "greater than 0" if minimum == 1 else f"at least {minimum}"
        raise ValueError(f"{field_name} must be {comparator}.")
    if maximum is not None and integer > maximum:
        raise ValueError(f"{field_name} must be at most {maximum}.")
    return integer


def validate_and_normalize_config(
    payload: dict[str, Any], *, preserve_updated_at: bool = False
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Configuration must be a JSON object.")
    normalized = deepcopy(DEFAULT_CONFIG)
    normalized.update(payload)

    tz_value = normalize_optional_string(normalized.get("tz")) or DEFAULT_CONFIG["tz"]
    try:
        ZoneInfo(tz_value)
    except Exception as exc:  # pragma: no cover - depends on system tzdb
        raise ValueError(f"Invalid timezone: {tz_value}") from exc

    scan_paths = list(
        dict.fromkeys(
            canonicalize_path_entry(scan_path, "scan_paths", require_existing=True)
            for scan_path in normalize_path_list(normalized.get("scan_paths"), "scan_paths", required=True)
        )
    )
    for scan_path in scan_paths:
        if not os.path.isdir(scan_path):
            raise ValueError(f"Scan path is not a directory inside the container: {scan_path}")

    exclude_paths = list(
        dict.fromkeys(
            canonicalize_path_entry(path, "exclude_paths", require_existing=False)
            for path in normalize_path_list(normalized.get("exclude_paths"), "exclude_paths", required=False)
        )
    )

    quarantine_dir = canonicalize_path_entry(
        normalize_optional_string(normalized.get("quarantine_dir")) or str(DEFAULT_CONFIG["quarantine_dir"]),
        "quarantine_dir",
        require_existing=False,
    )
    scanlog = canonicalize_path_entry(
        normalize_optional_string(normalized.get("scanlog")) or str(DEFAULT_CONFIG["scanlog"]),
        "scanlog",
        require_existing=False,
    )
    force_full_flag = normalize_optional_string(normalized.get("force_full_flag"))
    if force_full_flag:
        force_full_flag = canonicalize_path_entry(force_full_flag, "force_full_flag", require_existing=False)

    if any(quarantine_dir == scan_path for scan_path in scan_paths):
        raise ValueError("quarantine_dir must not be the same directory as a scan root.")
    if any(path_within_scan_root(scan_path, quarantine_dir) for scan_path in scan_paths):
        raise ValueError("quarantine_dir must not contain a scan root.")
    if any(path_within_scan_root(scanlog, scan_path) for scan_path in scan_paths):
        raise ValueError("scanlog must be outside every scan root so logging cannot mutate a file being scanned.")

    updated_at = utc_now_iso()
    if preserve_updated_at:
        updated_at = normalize_optional_string(normalized.get("updated_at")) or updated_at

    result = {
        "version": 1,
        "tz": tz_value,
        "maxthreads": normalize_int(
            normalized.get("maxthreads"), "maxthreads", minimum=1, maximum=MAX_CLAMD_THREADS
        ),
        "scan_paths": scan_paths,
        "exclude_paths": exclude_paths,
        "full_scan_parallel_jobs": normalize_int(
            normalized.get("full_scan_parallel_jobs"),
            "full_scan_parallel_jobs",
            minimum=1,
            maximum=MAX_SCAN_WORKERS,
        ),
        "changed_scan_parallel_jobs": normalize_int(
            normalized.get("changed_scan_parallel_jobs"),
            "changed_scan_parallel_jobs",
            minimum=1,
            maximum=MAX_SCAN_WORKERS,
        ),
        "full_progress_steps": normalize_int(
            normalized.get("full_progress_steps"),
            "full_progress_steps",
            minimum=1,
            maximum=MAX_PROGRESS_STEPS,
        ),
        "changed_progress_steps": normalize_int(
            normalized.get("changed_progress_steps"),
            "changed_progress_steps",
            minimum=1,
            maximum=MAX_PROGRESS_STEPS,
        ),
        "full_chunk_size": normalize_int(
            normalized.get("full_chunk_size"),
            "full_chunk_size",
            minimum=0,
            maximum=MAX_PROGRESS_INTERVAL,
        ),
        "changed_chunk_size": normalize_int(
            normalized.get("changed_chunk_size"),
            "changed_chunk_size",
            minimum=0,
            maximum=MAX_PROGRESS_INTERVAL,
        ),
        "changed_scan_days": normalize_days(normalized.get("changed_scan_days"), "changed_scan_days"),
        "changed_scan_times": normalize_times(normalized.get("changed_scan_times"), "changed_scan_times"),
        "full_scan_days": normalize_days(normalized.get("full_scan_days"), "full_scan_days"),
        "full_scan_times": normalize_times(normalized.get("full_scan_times"), "full_scan_times"),
        "scan_failure_retry_interval": normalize_int(
            normalized.get("scan_failure_retry_interval"),
            "scan_failure_retry_interval",
            minimum=1,
            maximum=MAX_SCHEDULER_INTERVAL_SECONDS,
        ),
        "force_full_poll_interval": normalize_int(
            normalized.get("force_full_poll_interval"),
            "force_full_poll_interval",
            minimum=1,
            maximum=3600,
        ),
        "path_check_timeout": normalize_int(
            normalized.get("path_check_timeout"),
            "path_check_timeout",
            minimum=1,
            maximum=MAX_PATH_CHECK_TIMEOUT_SECONDS,
        ),
        "path_enumeration_timeout": normalize_int(
            normalized.get("path_enumeration_timeout"),
            "path_enumeration_timeout",
            minimum=1,
            maximum=MAX_SCHEDULER_INTERVAL_SECONDS,
        ),
        "path_unavailable_retry_interval": normalize_int(
            normalized.get("path_unavailable_retry_interval"),
            "path_unavailable_retry_interval",
            minimum=1,
            maximum=MAX_SCHEDULER_INTERVAL_SECONDS,
        ),
        "scan_path_marker": normalize_scan_path_marker(normalized.get("scan_path_marker")),
        "quarantine_dir": quarantine_dir,
        "scanlog": scanlog,
        "force_full_flag": force_full_flag,
        "updated_at": updated_at,
    }

    if result["full_scan_parallel_jobs"] > result["maxthreads"]:
        raise ValueError("full_scan_parallel_jobs must not exceed maxthreads.")
    if result["changed_scan_parallel_jobs"] > result["maxthreads"]:
        raise ValueError("changed_scan_parallel_jobs must not exceed maxthreads.")

    if "created_at" in normalized:
        result["created_at"] = normalize_optional_string(normalized.get("created_at")) or utc_now_iso()
    else:
        result["created_at"] = utc_now_iso()

    return result


def serialize_config_for_scheduler(config: dict[str, Any]) -> dict[str, str]:
    force_full_flag = config.get("force_full_flag") or str(DEFAULT_CONFIG["force_full_flag"])

    return {
        "TZ": config["tz"],
        "MAXTHREADS": str(config["maxthreads"]),
        "SCAN_PATHS": ":".join(config["scan_paths"]),
        "EXCLUDE_PATHS": ":".join(config["exclude_paths"]),
        "FULL_SCAN_PARALLEL_JOBS": str(config["full_scan_parallel_jobs"]),
        "CHANGED_SCAN_PARALLEL_JOBS": str(config["changed_scan_parallel_jobs"]),
        "FULL_PROGRESS_STEPS": str(config["full_progress_steps"]),
        "CHANGED_PROGRESS_STEPS": str(config["changed_progress_steps"]),
        "FULL_CHUNK_SIZE": str(config["full_chunk_size"]),
        "CHANGED_CHUNK_SIZE": str(config["changed_chunk_size"]),
        "CHANGED_SCAN_DAYS": ",".join(str(day) for day in config["changed_scan_days"]),
        "CHANGED_SCAN_TIMES": ",".join(config["changed_scan_times"]),
        "FULL_SCAN_DAYS": ",".join(str(day) for day in config["full_scan_days"]),
        "FULL_SCAN_TIMES": ",".join(config["full_scan_times"]),
        "SCAN_FAILURE_RETRY_INTERVAL": str(config["scan_failure_retry_interval"]),
        "FORCE_FULL_POLL_INTERVAL": str(config["force_full_poll_interval"]),
        "PATH_CHECK_TIMEOUT": str(config["path_check_timeout"]),
        "PATH_ENUMERATION_TIMEOUT": str(config["path_enumeration_timeout"]),
        "PATH_UNAVAILABLE_RETRY_INTERVAL": str(config["path_unavailable_retry_interval"]),
        "SCAN_PATH_MARKER": config["scan_path_marker"],
        "QUARANTINE_DIR": config["quarantine_dir"],
        "SCANLOG": config["scanlog"],
        "FORCE_FULL_FLAG": force_full_flag,
    }


def build_runtime_env(config: dict[str, Any], state_dir: Path) -> dict[str, str]:
    env: dict[str, str] = {key: value for key, value in os.environ.items() if key in BOOTSTRAP_ENV_KEYS}
    env.update(serialize_config_for_scheduler(config))
    env["STATE_DIR"] = str(state_dir)
    return env


def sanitize_line(line: str) -> str:
    return line.replace("\x01", "").strip()


def format_scan_label(label: str) -> str:
    return "Full Scan" if label == "FULL" else "Changed-Files Scan"


def parse_enumeration_path(value: str) -> str | None:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return value[:1024]
    if decoded is None:
        return None
    return str(decoded)[:1024]


HISTORY_DEDUPE_WINDOW_SECONDS = 7200
MIB_PER_UNIT = {
    "B": 1.0 / (1024.0 * 1024.0),
    "KiB": 1.0 / 1024.0,
    "MiB": 1.0,
    "GiB": 1024.0,
    "TiB": 1024.0 * 1024.0,
}


def parse_files_per_second(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    match = FILES_PER_SECOND_RE.match(value.strip())
    if not match:
        return None
    return float(match.group("value"))


def parse_data_rate_to_mib_per_second(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    match = DATA_RATE_RE.match(value.strip())
    if not match:
        return None
    return float(match.group("value")) * MIB_PER_UNIT[match.group("unit")]


def parse_elapsed_seconds(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    matches = list(ELAPSED_PART_RE.finditer(value.strip()))
    if not matches:
        return None

    total_seconds = 0.0
    for match in matches:
        amount = float(match.group("value"))
        unit = match.group("unit")
        if unit == "h":
            total_seconds += amount * 3600.0
        elif unit == "m":
            total_seconds += amount * 60.0
        else:
            total_seconds += amount
    return total_seconds


def history_entry_has_trace(entry: dict[str, Any]) -> bool:
    trace = entry.get("progress_trace")
    return isinstance(trace, list) and bool(trace)


def merge_history_entry(existing: dict[str, Any], incoming: dict[str, Any]) -> None:
    existing["cycle_started_at"] = choose_preferred_cycle_started_at(
        existing.get("cycle_started_at"),
        incoming.get("cycle_started_at"),
    )

    existing_roots = existing.setdefault("roots", [])
    for root_entry in incoming.get("roots", []):
        if root_entry not in existing_roots:
            existing_roots.append(deepcopy(root_entry))

    existing_trace = existing.get("progress_trace") or []
    incoming_trace = incoming.get("progress_trace") or []
    if len(incoming_trace) > len(existing_trace):
        existing["progress_trace"] = deepcopy(incoming_trace)


def build_progress_trace_point(progress_match: re.Match[str]) -> dict[str, Any]:
    return {
        "percent": int(progress_match.group("percent")),
        "processed_files": int(progress_match.group("processed")),
        "total_files": int(progress_match.group("total")),
        "elapsed_seconds": parse_elapsed_seconds(progress_match.group("elapsed")),
        "avg_throughput_files_per_sec": parse_files_per_second(progress_match.group("avg_throughput")),
        "window_throughput_files_per_sec": parse_files_per_second(progress_match.group("window_throughput")),
        "avg_data_rate_mib_per_sec": parse_data_rate_to_mib_per_second(progress_match.group("avg_data_rate")),
        "window_data_rate_mib_per_sec": parse_data_rate_to_mib_per_second(progress_match.group("window_data_rate")),
    }


def history_summary_identity(entry: dict[str, Any]) -> tuple[Any, ...]:
    return (
        entry.get("label"),
        entry.get("scheduled_files"),
        entry.get("indexed_files"),
        entry.get("processed_files"),
        entry.get("clean"),
        entry.get("infected"),
        entry.get("vanished"),
        entry.get("errors"),
        entry.get("quarantine_failures"),
        entry.get("bytes"),
        entry.get("elapsed"),
        entry.get("avg_throughput"),
        entry.get("avg_data_rate"),
    )


def parse_history_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in ("%a %b %d %H:%M:%S %Z %Y", "%a %b %d %H:%M:%S %Y"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def choose_preferred_cycle_started_at(existing_value: Any, incoming_value: Any) -> Any:
    existing_dt = parse_history_timestamp(existing_value)
    incoming_dt = parse_history_timestamp(incoming_value)

    if existing_dt and incoming_dt:
        return existing_value if existing_dt <= incoming_dt else incoming_value
    if incoming_dt and not existing_dt:
        return incoming_value
    return existing_value or incoming_value


def history_entries_match(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    if history_summary_identity(existing) != history_summary_identity(incoming):
        return False

    if history_entry_has_trace(existing) and history_entry_has_trace(incoming):
        return existing.get("cycle_started_at") == incoming.get("cycle_started_at")

    existing_dt = parse_history_timestamp(existing.get("cycle_started_at"))
    incoming_dt = parse_history_timestamp(incoming.get("cycle_started_at"))
    if existing_dt and incoming_dt:
        return abs((existing_dt - incoming_dt).total_seconds()) <= HISTORY_DEDUPE_WINDOW_SECONDS

    return existing.get("cycle_started_at") == incoming.get("cycle_started_at")


def history_entries_match_live(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    return (
        history_summary_identity(existing) == history_summary_identity(incoming)
        and existing.get("cycle_started_at") == incoming.get("cycle_started_at")
    )


def dedupe_history_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        cloned = deepcopy(entry)
        matched = False
        for existing in deduped:
            if not history_entries_match(existing, cloned):
                continue

            merge_history_entry(existing, cloned)
            matched = True
            break

        if not matched:
            deduped.append(cloned)

    return deduped


def recent_tail_lines(path: Path, max_lines: int = 200) -> list[str]:
    if not path.exists():
        return []
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        block_size = 65536
        chunks: list[bytes] = []
        bytes_collected = 0
        while size > 0 and bytes_collected < block_size * 4:
            step = min(block_size, size)
            size -= step
            handle.seek(size)
            chunk = handle.read(step)
            chunks.insert(0, chunk)
            bytes_collected += step
            if b"\n" in chunk and b"\n".join(chunks).count(b"\n") >= max_lines:
                break
    joined = b"".join(chunks).decode("utf-8", "replace")
    return joined.splitlines()[-max_lines:]


def path_within_scan_root(path: str, root: str) -> bool:
    canonical_path = os.path.realpath(path)
    canonical_root = os.path.realpath(root)
    try:
        return os.path.commonpath((canonical_path, canonical_root)) == canonical_root
    except ValueError:
        return False


def read_epoch_file(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except (OSError, ValueError):
        return 0


def read_scan_checkpoint(state_dir: Path, field: str) -> int:
    if field not in {"full", "changed"}:
        raise ValueError("checkpoint field must be full or changed")
    checkpoint_path = state_dir / "scan-checkpoints.json"
    if checkpoint_path.exists():
        value = read_json(checkpoint_path)
        if not isinstance(value, dict) or value.get("version") != 1:
            raise ValueError("Saved scan checkpoint state is invalid.")
        epoch = int(value.get(f"last_{field}_scan_epoch", 0))
        if epoch < 0:
            raise ValueError("Saved scan checkpoint must not be negative.")
        return epoch
    return read_epoch_file(state_dir / f"last_{field}_scan_epoch")


def write_key_value_file(path: Path, lines: list[str]) -> None:
    for line in lines:
        if "\n" in line or "\r" in line or "\0" in line:
            raise ValueError("Manual scan request values must not contain control line separators.")
        if "=" not in line:
            raise ValueError("Manual scan request entries must use KEY=VALUE format.")
    write_text_atomic(path, "\n".join(lines) + "\n")


def validate_manual_request_paths(
    config: dict[str, Any],
    raw_paths: Any,
    *,
    field_name: str,
    require_existing: bool,
) -> list[str]:
    paths = list(
        dict.fromkeys(
            canonicalize_path_entry(path, field_name, require_existing=require_existing)
            for path in normalize_path_list(raw_paths, field_name, required=False)
        )
    )
    if not paths:
        return []

    scan_roots = config["scan_paths"]
    for path in paths:
        if not any(path_within_scan_root(path, root) for root in scan_roots):
            raise ValueError(f"{field_name.replace('_', ' ').title()} is outside configured scan roots: {path}")

    return paths


class ServiceUnavailableError(RuntimeError):
    """A required runtime resource is temporarily unavailable."""


class SchedulerManager:
    def __init__(self, config_dir: Path, state_dir: Path) -> None:
        self.config_dir = config_dir
        self.state_dir = state_dir
        self.config_path = config_dir / "ui-config.json"
        self.history_path = config_dir / "ui-history.json"
        self.manual_full_request_path = state_dir / "manual_full_scan_request.env"
        self.manual_changed_request_path = state_dir / "manual_changed_scan_request.env"
        self.static_dir = Path("/usr/local/share/clamav-ui")
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._process: subprocess.Popen[bytes] | None = None
        self._process_exit_code: int | None = None
        self._process_started_monotonic = 0.0
        self._restart_failures = 0
        self._next_restart_monotonic = 0.0
        self._config_error = ""
        self._config: dict[str, Any] | None = None
        self._repair_config: dict[str, Any] | None = None
        history_warning = ""
        history_should_rewrite = False
        raw_history: list[Any] = []
        try:
            loaded_history = read_json(self.history_path, default=[])
            if loaded_history is None:
                loaded_history = []
            if not isinstance(loaded_history, list):
                raise ValueError("history root must be a JSON list")
            raw_history = loaded_history
            valid_history = [entry for entry in raw_history if isinstance(entry, dict)]
            if len(valid_history) != len(raw_history):
                history_warning = "Ignored malformed entries in the saved scan history."
            self._history = dedupe_history_entries(valid_history)[-100:]
            history_should_rewrite = not history_warning and self._history != raw_history
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
            self._history = []
            history_warning = f"Saved scan history could not be loaded and was left unchanged: {exc}"
            print(f"[ui] {history_warning}", file=sys.stderr, flush=True)
        self._recent_logs: deque[str] = deque(maxlen=250)
        self._phase = "unconfigured"
        self._next_wake = ""
        self._last_event = "Waiting for UI configuration."
        self._last_warning = history_warning
        self._current_scan: dict[str, Any] | None = None
        self._current_scan_trace: list[dict[str, Any]] = []
        self._last_summary: dict[str, Any] | None = self._history[-1] if self._history else None
        self._current_cycle_started_at = ""
        self._last_scan_kind = ""
        self._log_path: Path | None = None
        self._log_offset = 0
        self._log_inode: int | None = None

        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if history_should_rewrite:
            write_json_atomic(self.history_path, self._history)

        self._load_config_from_disk()
        self._replay_existing_log()
        if self._config is not None and not self._config_error:
            self._start_scheduler_locked(reset_backoff=True)
        if history_warning:
            self._last_warning = history_warning

        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        with self._lock:
            self._stop_scheduler_locked()
        self._monitor_thread.join(timeout=2)

    def get_bootstrap(self) -> dict[str, Any]:
        with self._lock:
            return {
                "configured": self._config is not None and not self._config_error,
                "config_error": self._config_error,
                "config": deepcopy(
                    self._config
                    if self._config is not None
                    else self._repair_config or DEFAULT_CONFIG
                ),
                "repair_mode": bool(self._config_error),
                "defaults": deepcopy(DEFAULT_CONFIG),
                "day_options": [{"value": day, "label": label} for day, label in DAY_LABELS.items()],
                "status": self._status_payload_locked(),
                "recent_logs": list(self._recent_logs),
                "history": self._history[-20:],
            }

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            payload = self._status_payload_locked()
            payload["recent_logs"] = list(self._recent_logs)
            payload["history"] = self._history[-20:]
            return payload

    def get_config(self) -> dict[str, Any]:
        with self._lock:
            if self._config is not None:
                return deepcopy(self._config)
            return deepcopy(self._repair_config or DEFAULT_CONFIG)

    def save_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = validate_and_normalize_config(payload)
        with self._lock:
            validate_runtime_permissions(normalized, self.config_dir, self.state_dir)
            write_json_atomic(self.config_path, normalized)
            self._config = normalized
            self._repair_config = None
            self._config_error = ""
            self._attach_log_file(Path(normalized["scanlog"]), replay=True)
            self._restart_scheduler_locked()
            return self._status_payload_locked()

    def force_full_scan(self) -> None:
        with self._lock:
            if self._config is None or self._config_error:
                raise ValueError("UI mode is not configured yet.")
            force_full_flag = serialize_config_for_scheduler(self._config)["FORCE_FULL_FLAG"]
            flag_path = Path(force_full_flag)
            try:
                flag_path.parent.mkdir(parents=True, exist_ok=True)
                flag_path.touch()
            except OSError as exc:
                raise ServiceUnavailableError(
                    f"STATE_DIR is not writable by the container user: {self.state_dir} ({exc})"
                ) from exc
            self._last_event = f"Force-full flag created at {force_full_flag}."

    def restart_scanner(self) -> dict[str, Any]:
        with self._lock:
            if self._config is None or self._config_error:
                raise ValueError("UI mode is not configured yet.")
            self._restart_scheduler_locked()
            return self._status_payload_locked()

    def queue_manual_full_scan(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._config is None or self._config_error:
                raise ValueError("UI mode is not configured yet.")

            target_paths = validate_manual_request_paths(
                self._config,
                payload.get("target_paths"),
                field_name="target_paths",
                require_existing=True,
            )
            ignore_paths = validate_manual_request_paths(
                self._config,
                payload.get("ignore_paths"),
                field_name="ignore_paths",
                require_existing=False,
            )

            request_lines = [
                f"REQUEST_TARGET_PATHS={':'.join(target_paths)}",
                f"REQUEST_IGNORE_PATHS={':'.join(ignore_paths)}",
                f"REQUEST_CREATED_AT={int(time.time())}",
            ]
            try:
                write_key_value_file(self.manual_full_request_path, request_lines)
            except OSError as exc:
                raise ServiceUnavailableError(
                    f"STATE_DIR is not writable by the container user: {self.state_dir} ({exc})"
                ) from exc

            target_label = ":".join(target_paths) if target_paths else "all configured scan paths"
            if ignore_paths:
                ignore_label = ":".join(ignore_paths)
                self._last_event = (
                    f"Queued on-demand full scan over {target_label} with extra ignore paths {ignore_label}."
                )
            else:
                self._last_event = f"Queued on-demand full scan over {target_label}."
            return self._status_payload_locked()

    def queue_manual_changed_scan(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._config is None or self._config_error:
                raise ValueError("UI mode is not configured yet.")

            mode = normalize_optional_string(payload.get("mode")).lower() or "since_last"
            if mode not in {"since_last", "relative"}:
                raise ValueError("mode must be 'since_last' or 'relative'.")

            target_paths = validate_manual_request_paths(
                self._config,
                payload.get("target_paths"),
                field_name="target_paths",
                require_existing=True,
            )
            ignore_paths = validate_manual_request_paths(
                self._config,
                payload.get("ignore_paths"),
                field_name="ignore_paths",
                require_existing=False,
            )

            lookback_seconds = 0
            if mode == "since_last":
                reference_epoch = read_scan_checkpoint(self.state_dir, "changed")
            else:
                lookback_seconds = normalize_int(
                    payload.get("lookback_seconds"),
                    "lookback_seconds",
                    minimum=1,
                    maximum=MAX_LOOKBACK_SECONDS,
                )
                reference_epoch = max(0, int(time.time()) - lookback_seconds)

            request_lines = [
                f"REQUEST_MODE={mode}",
                f"REQUEST_REFERENCE_EPOCH={reference_epoch}",
                f"REQUEST_LOOKBACK_SECONDS={lookback_seconds}",
                f"REQUEST_TARGET_PATHS={':'.join(target_paths)}",
                f"REQUEST_IGNORE_PATHS={':'.join(ignore_paths)}",
                f"REQUEST_CREATED_AT={int(time.time())}",
            ]
            try:
                write_key_value_file(self.manual_changed_request_path, request_lines)
            except OSError as exc:
                raise ServiceUnavailableError(
                    f"STATE_DIR is not writable by the container user: {self.state_dir} ({exc})"
                ) from exc

            target_label = ":".join(target_paths) if target_paths else "all configured scan paths"
            ignore_suffix = ""
            if ignore_paths:
                ignore_suffix = f" while ignoring {':'.join(ignore_paths)}"
            if mode == "since_last":
                self._last_event = (
                    f"Queued on-demand changed scan since the last successful checkpoint over {target_label}{ignore_suffix}."
                )
            else:
                self._last_event = (
                    f"Queued on-demand changed scan for the last {lookback_seconds} seconds over {target_label}{ignore_suffix}."
                )
            return self._status_payload_locked()

    def _load_config_from_disk(self) -> None:
        if not self.config_path.exists():
            self._config = None
            self._repair_config = None
            self._config_error = ""
            self._phase = "unconfigured"
            return

        raw_config: Any = None
        try:
            raw_config = read_json(self.config_path, default={}) or {}
            self._config = validate_and_normalize_config(raw_config, preserve_updated_at=True)
            self._repair_config = None
            if self._config != raw_config:
                self._config["updated_at"] = utc_now_iso()
                write_json_atomic(self.config_path, self._config)
            self._attach_log_file(Path(self._config["scanlog"]), replay=False)
            self._phase = "starting"
            self._last_event = "Loaded UI configuration from disk."
        except Exception as exc:
            self._config = None
            self._repair_config = config_repair_draft(raw_config)
            self._config_error = str(exc)
            self._phase = "config_error"
            self._last_event = "UI configuration could not be loaded."

    def _status_payload_locked(self) -> dict[str, Any]:
        process = self._process
        scheduler_running = process is not None and process.poll() is None
        phase = self._phase
        if self._config_error:
            phase = "config_error"
        elif self._config is None:
            phase = "unconfigured"
        elif not scheduler_running and phase not in {"unconfigured", "config_error", "restart_wait"}:
            phase = "stopped"

        active_scan_phases = {"enumerating", "indexing", "scanning"}
        current_scan = deepcopy(self._current_scan) if scheduler_running and phase in active_scan_phases else None

        payload = {
            "mode": "ui",
            "configured": self._config is not None and not self._config_error,
            "config_error": self._config_error,
            "scheduler_running": scheduler_running,
            "scheduler_pid": process.pid if scheduler_running and process is not None else None,
            "scheduler_exit_code": self._process_exit_code,
            "restart_in_seconds": (
                max(0, int(self._next_restart_monotonic - time.monotonic() + 0.999))
                if phase == "restart_wait"
                else None
            ),
            "phase": phase,
            "next_wake": self._next_wake,
            "last_event": self._last_event,
            "last_warning": self._last_warning,
            "current_scan": current_scan,
            "last_summary": deepcopy(self._last_summary),
            "scanlog": self._config["scanlog"] if self._config else DEFAULT_CONFIG["scanlog"],
            "pending_manual_full_request": self._read_manual_request_locked(self.manual_full_request_path),
            "pending_manual_changed_request": self._read_manual_request_locked(),
        }
        if self._config is not None:
            payload["effective_force_full_flag"] = serialize_config_for_scheduler(self._config)["FORCE_FULL_FLAG"]
        else:
            payload["effective_force_full_flag"] = DEFAULT_CONFIG["force_full_flag"]
        return payload

    def _read_manual_request_locked(self, request_path: Path | None = None) -> dict[str, Any] | None:
        request_path = request_path or self.manual_changed_request_path
        if not request_path.exists():
            return None

        request: dict[str, str] = {}
        try:
            for raw_line in request_path.read_text(encoding="utf-8").splitlines():
                if "=" not in raw_line:
                    continue
                key, value = raw_line.split("=", 1)
                request[key] = value
        except OSError:
            return None

        target_paths_value = request.get("REQUEST_TARGET_PATHS", "") or request.get("REQUEST_PATHS", "")
        target_paths = [part for part in target_paths_value.split(":") if part]
        ignore_paths_value = request.get("REQUEST_IGNORE_PATHS", "")
        ignore_paths = [part for part in ignore_paths_value.split(":") if part]

        invalid_numeric_fields = False

        def request_integer(key: str) -> int:
            nonlocal invalid_numeric_fields
            try:
                value = int(request.get(key, "0") or "0")
                if value < 0:
                    raise ValueError
                return value
            except ValueError:
                invalid_numeric_fields = True
                return 0

        payload = {
            "mode": request.get("REQUEST_MODE", ""),
            "reference_epoch": request_integer("REQUEST_REFERENCE_EPOCH"),
            "lookback_seconds": request_integer("REQUEST_LOOKBACK_SECONDS"),
            "target_paths": target_paths,
            "ignore_paths": ignore_paths,
            "created_at": request_integer("REQUEST_CREATED_AT"),
        }
        if invalid_numeric_fields:
            payload["invalid"] = True
        return payload

    def _start_scheduler_locked(self, *, reset_backoff: bool = False) -> None:
        if self._config is None:
            return
        if self._process is not None and self._process.poll() is None:
            return

        if reset_backoff:
            self._restart_failures = 0
            self._next_restart_monotonic = 0.0

        try:
            validate_runtime_permissions(self._config, self.config_dir, self.state_dir)
        except ValueError as exc:
            self._process = None
            self._process_exit_code = None
            self._schedule_restart_locked(f"Scanner preflight failed: {exc}")
            return

        env = build_runtime_env(self._config, self.state_dir)
        self._attach_log_file(Path(self._config["scanlog"]), replay=True)
        try:
            self._process = subprocess.Popen(
                ["/bin/sh", "/usr/local/bin/clamav_scheduled.sh"],
                env=env,
                start_new_session=True,
            )
        except OSError as exc:
            self._process = None
            self._process_exit_code = None
            self._schedule_restart_locked(f"Scanner process could not be started: {exc}")
            return
        self._process_exit_code = None
        self._process_started_monotonic = time.monotonic()
        self._next_restart_monotonic = 0.0
        self._phase = "starting"
        self._last_event = "Scanner process started from UI mode."

    def _stop_scheduler_locked(self) -> None:
        if self._process is None:
            return
        if self._process.poll() is not None:
            self._process_exit_code = self._process.returncode
            self._process = None
            self._current_scan = None
            self._current_scan_trace = []
            return

        had_active_scan = self._current_scan is not None
        process = self._process
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        self._process_exit_code = process.returncode
        self._process = None
        self._next_restart_monotonic = 0.0
        self._current_scan = None
        self._current_scan_trace = []
        self._phase = "stopped"
        if had_active_scan:
            self._last_warning = (
                "Scanner process stopped while a scan was in progress. The interrupted scan will be retried later because checkpoints were not advanced."
            )
            self._last_event = self._last_warning
        else:
            self._last_event = "Scanner process stopped."

    def _restart_scheduler_locked(self) -> None:
        self._stop_scheduler_locked()
        self._last_warning = ""
        self._start_scheduler_locked(reset_backoff=True)

    def _schedule_restart_locked(self, warning: str) -> None:
        self._restart_failures += 1
        delay = min(
            SCHEDULER_RESTART_MAX_DELAY_SECONDS,
            SCHEDULER_RESTART_MIN_DELAY_SECONDS * (2 ** min(self._restart_failures - 1, 10)),
        )
        self._next_restart_monotonic = time.monotonic() + delay
        self._phase = "restart_wait"
        self._last_warning = warning
        self._last_event = f"{warning} Restarting in {delay} seconds."

    def _maybe_restart_scheduler_locked(self) -> None:
        if self._stop_event.is_set() or self._config is None or self._config_error or self._process is not None:
            return
        if self._next_restart_monotonic <= 0 or time.monotonic() < self._next_restart_monotonic:
            return
        self._start_scheduler_locked()

    def _attach_log_file(self, log_path: Path, replay: bool) -> None:
        self._log_path = log_path
        self._log_offset = 0
        self._log_inode = None
        if replay:
            self._replay_existing_log()

    def _reset_runtime_state_from_replay_locked(self) -> None:
        self._next_wake = ""
        self._current_scan = None
        self._current_scan_trace = []
        self._current_cycle_started_at = ""
        self._last_scan_kind = ""
        self._last_event = "Waiting for scanner activity."
        self._last_warning = ""

    def _replay_existing_log(self) -> None:
        if self._log_path is None or not self._log_path.exists():
            return
        self._recent_logs.clear()
        self._reset_runtime_state_from_replay_locked()
        for line in recent_tail_lines(self._log_path, max_lines=200):
            self._handle_log_line(line, replay=True)

        if self._current_scan is not None:
            self._current_scan = None
            if self._phase in {"enumerating", "indexing", "scanning", "waiting_lock", "starting", "cycle_complete"}:
                self._phase = "idle" if self._next_wake else "stopped"

        try:
            stat_result = self._log_path.stat()
        except OSError:
            return
        self._log_offset = stat_result.st_size
        self._log_inode = stat_result.st_ino

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                self._poll_process_locked()
                self._poll_logs_locked()
                self._maybe_restart_scheduler_locked()
            self._stop_event.wait(1.0)

    def _poll_process_locked(self) -> None:
        if self._process is None:
            return
        return_code = self._process.poll()
        if return_code is None:
            return
        had_active_scan = self._current_scan is not None
        self._process_exit_code = return_code
        self._process = None
        self._current_scan = None
        self._current_scan_trace = []
        runtime = max(0.0, time.monotonic() - self._process_started_monotonic)
        if runtime >= SCHEDULER_STABLE_RUNTIME_SECONDS:
            self._restart_failures = 0
        if return_code == 0:
            if had_active_scan:
                self._last_warning = (
                    "Scanner process exited while a scan was in progress. The interrupted scan will be retried later because checkpoints were not advanced."
                )
                self._last_event = self._last_warning
            else:
                self._last_event = "Scanner process exited cleanly."
        else:
            self._last_warning = f"Scanner exited with code {return_code}."
            self._last_event = self._last_warning
        self._schedule_restart_locked(self._last_event)

    def _poll_logs_locked(self) -> None:
        if self._log_path is None:
            return
        if not self._log_path.exists():
            return
        try:
            stat_result = self._log_path.stat()
        except OSError:
            return

        if self._log_inode != stat_result.st_ino or stat_result.st_size < self._log_offset:
            self._log_offset = 0
            self._log_inode = stat_result.st_ino

        try:
            with self._log_path.open("r", encoding="utf-8", errors="replace") as handle:
                handle.seek(self._log_offset)
                for line in handle:
                    self._handle_log_line(line)
                self._log_offset = handle.tell()
        except OSError:
            return

    def _append_history_locked(self, entry: dict[str, Any]) -> dict[str, Any]:
        for existing in self._history:
            if history_entries_match_live(existing, entry):
                merge_history_entry(existing, entry)
                self._last_summary = existing
                write_json_atomic(self.history_path, self._history)
                return existing

        self._history.append(entry)
        if len(self._history) > 100:
            self._history = self._history[-100:]
        write_json_atomic(self.history_path, self._history)
        self._last_summary = entry
        return entry

    def _handle_log_line(self, raw_line: str, replay: bool = False) -> None:
        line = sanitize_line(raw_line)
        if not line:
            return

        self._recent_logs.append(line)

        if line == "clamd ready.":
            self._phase = "idle"
            self._last_event = line
            return

        cycle_match = CYCLE_START_RE.match(line)
        if cycle_match:
            self._current_cycle_started_at = cycle_match.group("stamp")
            self._last_event = line
            return

        no_scans_match = NO_SCANS_RE.match(line)
        if no_scans_match:
            self._phase = "idle"
            self._next_wake = no_scans_match.group("next_wake")
            self._last_event = f"No scans due. Next wake at {self._next_wake}."
            self._current_scan = None
            self._current_scan_trace = []
            self._last_scan_kind = ""
            return

        if line.startswith("=== FULL SCAN starting ==="):
            if replay:
                self._last_scan_kind = "FULL"
                self._last_event = "Historical full scan start detected in log replay."
                return
            self._phase = "enumerating"
            self._last_scan_kind = "FULL"
            self._current_scan_trace = []
            self._current_scan = {
                "label": "FULL",
                "display_label": format_scan_label("FULL"),
                "started_at": utc_now_iso(),
                "stage": "enumerating",
                "status_message": "Preparing scan roots and building a safe file list.",
                "enumerated_files": 0,
                "progress_trace": [],
            }
            self._last_event = "Full scan file discovery started."
            return

        if line.startswith("=== CHANGED-FILES scan starting ==="):
            if replay:
                self._last_scan_kind = "CHANGED"
                self._last_event = "Historical changed-files scan start detected in log replay."
                return
            self._phase = "enumerating"
            self._last_scan_kind = "CHANGED"
            self._current_scan_trace = []
            self._current_scan = {
                "label": "CHANGED",
                "display_label": format_scan_label("CHANGED"),
                "started_at": utc_now_iso(),
                "stage": "enumerating",
                "status_message": "Preparing scan roots and finding changed files.",
                "enumerated_files": 0,
                "progress_trace": [],
            }
            self._last_event = "Changed-files scan file discovery started."
            return

        enumeration_start_match = ENUMERATION_START_RE.match(line)
        if enumeration_start_match:
            if replay:
                self._last_event = line
                return
            label = enumeration_start_match.group("label")
            if self._current_scan is None or self._current_scan.get("label") != label:
                self._current_scan_trace = []
                self._current_scan = {
                    "label": label,
                    "display_label": format_scan_label(label),
                    "started_at": utc_now_iso(),
                    "enumerated_files": 0,
                    "progress_trace": [],
                }
            self._phase = "enumerating"
            self._current_scan.update(
                {
                    "stage": "enumerating",
                    "current_path": enumeration_start_match.group("path"),
                    "enumeration_timeout_seconds": int(enumeration_start_match.group("timeout")),
                    "enumeration_visited_entries": 0,
                    "enumeration_new_entries": 0,
                    "enumeration_window_seconds": 0,
                    "enumeration_raw_list_bytes": 0,
                    "latest_discovered_path": None,
                    "stage_started_at": utc_now_iso(),
                    "status_message": (
                        f"Walking {enumeration_start_match.group('path')} and building the NUL-safe file list. "
                        "The final total is not known until enumeration finishes."
                    ),
                }
            )
            self._last_event = line
            return

        enumeration_progress_match = ENUMERATION_PROGRESS_RE.match(line)
        if enumeration_progress_match:
            if replay:
                self._last_event = line
                return
            label = enumeration_progress_match.group("label")
            if self._current_scan is not None and self._current_scan.get("label") == label:
                visited = int(enumeration_progress_match.group("visited"))
                new_entries = int(enumeration_progress_match.group("new"))
                window_seconds = int(enumeration_progress_match.group("window"))
                latest_path = parse_enumeration_path(
                    enumeration_progress_match.group("latest")
                )
                if new_entries > 0:
                    status_message = (
                        f"Visited {visited} filesystem entries in the current path "
                        f"(+{new_entries} in the last {window_seconds}s). "
                        f"Latest successfully visited path: {latest_path or '<none>'}. "
                        "The final eligible-file total is not known yet."
                    )
                else:
                    status_message = (
                        f"Visited {visited} filesystem entries in the current path. "
                        f"No new entries were reported in the last {window_seconds}s; "
                        f"the traversal is still running. Latest successfully visited path: "
                        f"{latest_path or '<none>'}."
                    )
                self._current_scan.update(
                    {
                        "stage": "enumerating",
                        "enumeration_visited_entries": visited,
                        "enumeration_new_entries": new_entries,
                        "enumeration_window_seconds": window_seconds,
                        "enumeration_raw_list_bytes": int(
                            enumeration_progress_match.group("list_bytes")
                        ),
                        "enumeration_elapsed": f"{enumeration_progress_match.group('elapsed')}s",
                        "latest_discovered_path": latest_path,
                        "status_message": status_message,
                    }
                )
            self._phase = "enumerating"
            self._last_event = line
            return

        enumeration_complete_match = ENUMERATION_COMPLETE_RE.match(line)
        if enumeration_complete_match:
            if replay:
                self._last_event = line
                return
            label = enumeration_complete_match.group("label")
            if self._current_scan is not None and self._current_scan.get("label") == label:
                discovered = int(enumeration_complete_match.group("files"))
                self._current_scan["enumerated_files"] = int(
                    self._current_scan.get("enumerated_files", 0)
                ) + discovered
                self._current_scan["enumeration_elapsed"] = (
                    f"{enumeration_complete_match.group('elapsed')}s"
                )
                self._current_scan["status_message"] = (
                    f"Finished {enumeration_complete_match.group('path')}; "
                    f"{discovered} eligible files were added."
                )
            self._last_event = line
            return

        file_list_complete_match = FILE_LIST_COMPLETE_RE.match(line)
        if file_list_complete_match:
            if replay:
                self._last_event = line
                return
            label = file_list_complete_match.group("label")
            if self._current_scan is not None and self._current_scan.get("label") == label:
                self._current_scan.update(
                    {
                        "stage": "indexing",
                        "enumerated_files": int(file_list_complete_match.group("files")),
                        "enumeration_sources": int(file_list_complete_match.group("sources")),
                        "stage_started_at": utc_now_iso(),
                        "status_message": "File discovery completed. Preparing to capture file identities and sizes.",
                    }
                )
            self._phase = "indexing"
            self._last_event = line
            return

        indexing_start_match = INDEXING_START_RE.match(line)
        if indexing_start_match:
            if replay:
                self._last_event = line
                return
            label = indexing_start_match.group("label")
            if self._current_scan is not None and self._current_scan.get("label") == label:
                self._current_scan.update(
                    {
                        "stage": "indexing",
                        "stage_started_at": utc_now_iso(),
                        "status_message": "Capturing each file's identity and size before worker scanning begins.",
                    }
                )
            self._phase = "indexing"
            self._last_event = line
            return

        indexing_complete_match = INDEXING_COMPLETE_RE.match(line)
        if indexing_complete_match:
            if replay:
                self._last_event = line
                return
            label = indexing_complete_match.group("label")
            if self._current_scan is not None and self._current_scan.get("label") == label:
                self._current_scan.update(
                    {
                        "stage": "indexing",
                        "total_files": int(indexing_complete_match.group("files")),
                        "total_bytes": indexing_complete_match.group("bytes"),
                        "indexing_elapsed": indexing_complete_match.group("elapsed"),
                        "status_message": "File identities captured. Starting persistent ClamD workers.",
                    }
                )
            self._phase = "indexing"
            self._last_event = line
            return

        if line.startswith("=== Scan cycle paused due to unavailable scan path ==="):
            self._phase = "paused"
            self._current_scan = None
            self._current_scan_trace = []
            self._last_warning = line
            self._last_event = line
            return

        if line.startswith("[LOCKED]"):
            self._phase = "waiting_lock"
            self._current_scan = None
            self._current_scan_trace = []
            self._last_event = line
            return

        if line.startswith("[WARN]"):
            self._last_warning = line
            self._last_event = line
            if self._current_scan is not None:
                self._current_scan["status_message"] = line

        scan_start_match = SCAN_START_RE.match(line)
        if scan_start_match:
            if replay:
                self._last_scan_kind = scan_start_match.group("label")
                self._last_event = line
                return
            label = scan_start_match.group("label")
            if self._current_scan is None or self._current_scan.get("label") != label:
                self._current_scan_trace = []
                self._current_scan = {
                    "label": label,
                    "display_label": format_scan_label(label),
                    "started_at": self._current_cycle_started_at or utc_now_iso(),
                    "progress_trace": [],
                }
            self._phase = "scanning"
            self._current_scan["stage"] = "scanning"
            self._current_scan["status_message"] = "Persistent ClamD workers are actively scanning files."
            self._current_scan.setdefault("processed_files", 0)
            self._current_scan["total_files"] = int(scan_start_match.group("total"))
            self._current_scan["workers"] = int(scan_start_match.group("workers"))
            self._last_event = line
            return

        no_files_match = NO_FILES_RE.match(line)
        if no_files_match:
            if replay:
                self._last_event = line
                return
            if self._current_scan is not None and self._current_scan.get("label") == no_files_match.group("label"):
                self._current_scan.update(
                    {
                        "stage": "complete",
                        "total_files": 0,
                        "processed_files": 0,
                        "status_message": "No eligible files were found for this scan.",
                    }
                )
            self._phase = "cycle_complete"
            self._last_event = line
            return

        heartbeat_match = SCAN_HEARTBEAT_RE.match(line)
        if heartbeat_match:
            if replay:
                self._last_event = line
                return
            label = heartbeat_match.group("label")
            if self._current_scan is not None and self._current_scan.get("label") == label:
                self._current_scan.update(
                    {
                        "stage": "scanning",
                        "processed_files": int(heartbeat_match.group("processed")),
                        "total_files": int(heartbeat_match.group("total")),
                        "queued_files": int(heartbeat_match.group("queued")),
                        "active_workers": int(heartbeat_match.group("active_workers")),
                        "clean": int(heartbeat_match.group("clean")),
                        "infected": int(heartbeat_match.group("infected")),
                        "vanished": int(heartbeat_match.group("vanished")),
                        "errors": int(heartbeat_match.group("errors")),
                        "elapsed": heartbeat_match.group("elapsed"),
                        "status_message": (
                            f"Workers are active: {heartbeat_match.group('processed')} files completed, "
                            f"{heartbeat_match.group('queued')} still queued."
                        ),
                        "updated_at": utc_now_iso(),
                    }
                )
            self._phase = "scanning"
            self._last_event = line
            return

        progress_config_match = PROGRESS_CONFIG_RE.match(line)
        if progress_config_match:
            if replay:
                return
            if self._current_scan is not None:
                self._current_scan["progress_mode"] = progress_config_match.group("mode")
                self._current_scan["progress_interval"] = int(progress_config_match.group("interval"))
                self._current_scan["progress_detail"] = progress_config_match.group("detail")
            return

        progress_match = PROGRESS_RE.match(line)
        if progress_match:
            if replay:
                self._last_scan_kind = progress_match.group("label")
                self._last_event = line
                return
            label = progress_match.group("label")
            if self._current_scan is None or self._current_scan.get("label") != label:
                self._current_scan_trace = []
                self._current_scan = {
                    "label": label,
                    "display_label": format_scan_label(label),
                    "started_at": self._current_cycle_started_at or utc_now_iso(),
                    "progress_trace": [],
                }
            self._phase = "scanning"
            self._current_scan["stage"] = "scanning"
            self._current_scan_trace.append(build_progress_trace_point(progress_match))
            self._current_scan.update(
                {
                    "percent": int(progress_match.group("percent")),
                    "processed_files": int(progress_match.group("processed")),
                    "total_files": int(progress_match.group("total")),
                    "processed_bytes": progress_match.group("processed_bytes"),
                    "total_bytes": progress_match.group("total_bytes"),
                    "clean": int(progress_match.group("clean")),
                    "infected": int(progress_match.group("infected")),
                    "vanished": int(progress_match.group("vanished")),
                    "errors": int(progress_match.group("errors")),
                    "elapsed": progress_match.group("elapsed"),
                    "avg_throughput": progress_match.group("avg_throughput"),
                    "window_throughput": progress_match.group("window_throughput"),
                    "avg_data_rate": progress_match.group("avg_data_rate"),
                    "window_data_rate": progress_match.group("window_data_rate"),
                    "progress_trace": deepcopy(self._current_scan_trace[-120:]),
                    "updated_at": utc_now_iso(),
                }
            )
            self._last_event = line
            return

        summary_match = SUMMARY_RE.match(line)
        if summary_match:
            if replay:
                self._last_event = line
                return
            label = summary_match.group("label")
            entry = {
                "label": label,
                "display_label": format_scan_label(label),
                "cycle_started_at": self._current_cycle_started_at or utc_now_iso(),
                "scheduled_files": int(summary_match.group("scheduled_files")),
                "indexed_files": int(summary_match.group("indexed_files")),
                "processed_files": int(summary_match.group("processed_files")),
                "clean": int(summary_match.group("clean")),
                "infected": int(summary_match.group("infected")),
                "vanished": int(summary_match.group("vanished")),
                "errors": int(summary_match.group("errors")),
                "quarantine_failures": int(summary_match.group("quarantine_failures")),
                "bytes": summary_match.group("bytes"),
                "elapsed": summary_match.group("elapsed"),
                "avg_throughput": summary_match.group("avg_throughput"),
                "avg_data_rate": summary_match.group("avg_data_rate"),
                "progress_trace": deepcopy(self._current_scan_trace),
                "roots": [],
            }
            self._append_history_locked(entry)
            self._phase = "cycle_complete"
            self._last_event = line
            self._current_scan = None
            self._current_scan_trace = []
            self._last_scan_kind = label
            return

        root_match = ROOT_SUMMARY_RE.match(line)
        if root_match:
            if replay or self._last_summary is None:
                return
            latest = self._last_summary
            if latest.get("label") == root_match.group("label"):
                root_entry = {
                    "root": root_match.group("root"),
                    "files": int(root_match.group("files")),
                    "processed_files": int(root_match.group("processed_files")),
                    "bytes": root_match.group("bytes"),
                    "processed_bytes": root_match.group("processed_bytes"),
                    "infected": int(root_match.group("infected")),
                    "vanished": int(root_match.group("vanished")),
                    "errors": int(root_match.group("errors")),
                }
                roots = latest.setdefault("roots", [])
                if root_entry not in roots:
                    roots.append(root_entry)
                    write_json_atomic(self.history_path, self._history)
            return

        if line.startswith("=== Scan cycle finished ==="):
            if self._process is not None and self._process.poll() is None:
                self._phase = "idle"
            self._current_scan = None
            self._current_scan_trace = []
            self._last_event = line
            return

        if line.startswith("[ERROR]"):
            self._last_warning = line
            if self._current_scan is not None:
                self._current_scan["status_message"] = line

        if line.startswith("[FORCE]") or line.startswith("[MANUAL]") or line.startswith("[CHANGED]") or line.startswith("[ERROR]"):
            self._last_event = line


MANAGER: SchedulerManager | None = None


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    add_security_headers(handler)
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, status: int, body: bytes, content_type: str) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    add_security_headers(handler)
    handler.end_headers()
    handler.wfile.write(body)


def add_security_headers(handler: BaseHTTPRequestHandler) -> None:
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.send_header("X-Frame-Options", "DENY")
    handler.send_header("Referrer-Policy", "no-referrer")
    handler.send_header(
        "Content-Security-Policy",
        "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'; "
        "img-src 'self' data:; object-src 'none'; script-src 'self'; style-src 'self'",
    )


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        request_handler_class: type[BaseHTTPRequestHandler],
        *,
        max_workers: int,
        queue_size: int,
        request_timeout_seconds: int,
    ) -> None:
        self._worker_slots = threading.BoundedSemaphore(max_workers)
        self.request_queue_size = queue_size
        self.request_timeout_seconds = request_timeout_seconds
        super().__init__(server_address, request_handler_class)

    def process_request(self, request: Any, client_address: Any) -> None:
        self._worker_slots.acquire()
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._worker_slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._worker_slots.release()


class UIRequestHandler(BaseHTTPRequestHandler):
    server_version = "ClamAVScheduledUI/1.0"
    sys_version = ""

    def setup(self) -> None:
        super().setup()
        request_timeout = getattr(self.server, "request_timeout_seconds", 15)
        self.connection.settimeout(request_timeout)

    def log_message(self, fmt: str, *args: Any) -> None:
        if urlparse(self.path).path == "/healthz":
            return
        print(f"[ui] {self.address_string()} - {fmt % args}", flush=True)

    def do_GET(self) -> None:
        assert MANAGER is not None
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/healthz":
            status_payload = MANAGER.get_status()
            ready_phases = {"idle", "enumerating", "indexing", "scanning", "cycle_complete", "waiting_lock"}
            configured = bool(status_payload["configured"])
            scheduler_running = bool(status_payload["scheduler_running"])
            phase = str(status_payload["phase"])
            config_error = bool(status_payload["config_error"])
            healthy = not config_error and (
                not configured or (scheduler_running and phase in ready_phases)
            )
            if config_error:
                reason = f"saved UI configuration is invalid: {health_detail(status_payload['config_error'])}"
            elif not configured:
                reason = "initial UI configuration is required"
            elif not scheduler_running:
                reason = "scanner scheduler is not running"
                warning = health_detail(status_payload["last_warning"])
                if warning:
                    reason = f"{reason}: {warning}"
            elif phase not in ready_phases:
                reason = f"scanner scheduler phase is {phase}"
            else:
                reason = "ready"
            json_response(
                self,
                HTTPStatus.OK if healthy else HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "ok": healthy,
                    "reason": reason,
                    "configured": configured,
                    "scheduler_running": scheduler_running,
                    "scheduler_exit_code": status_payload["scheduler_exit_code"],
                    "restart_in_seconds": status_payload["restart_in_seconds"],
                    "phase": phase,
                },
            )
            return

        if path == "/api/bootstrap":
            json_response(self, HTTPStatus.OK, MANAGER.get_bootstrap())
            return

        if path == "/api/status":
            json_response(self, HTTPStatus.OK, MANAGER.get_status())
            return

        if path == "/api/config":
            json_response(self, HTTPStatus.OK, {"config": MANAGER.get_config()})
            return

        if path == "/" or path == "/index.html":
            self._serve_static("index.html", "text/html; charset=utf-8")
            return

        if path == "/app.js":
            self._serve_static("app.js", "application/javascript; charset=utf-8")
            return

        if path == "/styles.css":
            self._serve_static("styles.css", "text/css; charset=utf-8")
            return

        json_response(self, HTTPStatus.NOT_FOUND, {"error": "Not found"})

    def do_PUT(self) -> None:
        assert MANAGER is not None
        parsed = urlparse(self.path)
        if parsed.path != "/api/config":
            json_response(self, HTTPStatus.NOT_FOUND, {"error": "Not found"})
            return

        try:
            payload = self._read_json_body()
            status = MANAGER.save_config(payload)
        except ValueError as exc:
            json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception as exc:  # pragma: no cover - defensive HTTP path
            self._handle_internal_error(exc)
            return

        json_response(self, HTTPStatus.OK, {"ok": True, "status": status, "config": MANAGER.get_config()})

    def do_POST(self) -> None:
        assert MANAGER is not None
        parsed = urlparse(self.path)
        if parsed.path == "/api/actions/force-full":
            try:
                MANAGER.force_full_scan()
            except ServiceUnavailableError as exc:
                self._handle_service_unavailable(exc)
                return
            except ValueError as exc:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except Exception as exc:  # pragma: no cover - defensive HTTP path
                self._handle_internal_error(exc)
                return
            json_response(self, HTTPStatus.OK, {"ok": True, "status": MANAGER.get_status()})
            return

        if parsed.path == "/api/actions/manual-full":
            try:
                payload = self._read_json_body()
                status = MANAGER.queue_manual_full_scan(payload)
            except ServiceUnavailableError as exc:
                self._handle_service_unavailable(exc)
                return
            except ValueError as exc:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except Exception as exc:  # pragma: no cover - defensive HTTP path
                self._handle_internal_error(exc)
                return
            json_response(self, HTTPStatus.OK, {"ok": True, "status": status})
            return

        if parsed.path == "/api/actions/manual-changed":
            try:
                payload = self._read_json_body()
                status = MANAGER.queue_manual_changed_scan(payload)
            except ServiceUnavailableError as exc:
                self._handle_service_unavailable(exc)
                return
            except ValueError as exc:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except Exception as exc:  # pragma: no cover - defensive HTTP path
                self._handle_internal_error(exc)
                return
            json_response(self, HTTPStatus.OK, {"ok": True, "status": status})
            return

        if parsed.path == "/api/actions/restart-scanner":
            try:
                status = MANAGER.restart_scanner()
            except ValueError as exc:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except Exception as exc:  # pragma: no cover - defensive HTTP path
                self._handle_internal_error(exc)
                return
            json_response(self, HTTPStatus.OK, {"ok": True, "status": status})
            return

        json_response(self, HTTPStatus.NOT_FOUND, {"error": "Not found"})

    def _read_json_body(self) -> dict[str, Any]:
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise ValueError("Content-Length must be an integer.") from None
        if content_length < 0:
            raise ValueError("Content-Length must not be negative.")
        if content_length > MAX_JSON_BODY_BYTES:
            raise ValueError(f"JSON request body exceeds the {MAX_JSON_BODY_BYTES}-byte limit.")
        raw = self.rfile.read(content_length)
        if len(raw) != content_length:
            raise ValueError("JSON request body ended before Content-Length bytes were received.")
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Request body must be valid UTF-8 JSON.") from exc
        if not isinstance(payload, dict):
            raise ValueError("Request body must be a JSON object.")
        return payload

    def _handle_internal_error(self, exc: Exception) -> None:
        print(f"[ui] request handling failed: {exc!r}", file=sys.stderr, flush=True)
        json_response(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "Internal server error."})

    def _handle_service_unavailable(self, exc: ServiceUnavailableError) -> None:
        detail = health_detail(exc)
        print(f"[ui] request unavailable: {detail}", file=sys.stderr, flush=True)
        json_response(self, HTTPStatus.SERVICE_UNAVAILABLE, {"error": detail})

    def _serve_static(self, filename: str, content_type: str) -> None:
        assert MANAGER is not None
        file_path = MANAGER.static_dir / filename
        if not file_path.exists():
            json_response(self, HTTPStatus.NOT_FOUND, {"error": "Static file not found"})
            return
        body = file_path.read_bytes()
        text_response(self, HTTPStatus.OK, body, content_type)


def run_server() -> int:
    global MANAGER
    os.umask(0o077)

    config_dir = Path(os.environ.get("CONFIG_DIR", "/config"))
    state_dir = Path(os.environ.get("STATE_DIR", "/state"))
    ui_bind = os.environ.get("UI_BIND", "0.0.0.0")
    ui_port = normalize_int(os.environ.get("UI_PORT", "8080"), "UI_PORT", minimum=1, maximum=65535)
    ui_max_workers = normalize_int(
        os.environ.get("UI_MAX_REQUEST_THREADS", "32"),
        "UI_MAX_REQUEST_THREADS",
        minimum=1,
        maximum=MAX_HTTP_WORKERS,
    )
    ui_queue_size = normalize_int(
        os.environ.get("UI_REQUEST_QUEUE_SIZE", "64"),
        "UI_REQUEST_QUEUE_SIZE",
        minimum=1,
        maximum=MAX_HTTP_QUEUE,
    )
    ui_request_timeout = normalize_int(
        os.environ.get("UI_REQUEST_TIMEOUT_SECONDS", "15"),
        "UI_REQUEST_TIMEOUT_SECONDS",
        minimum=1,
        maximum=MAX_HTTP_TIMEOUT_SECONDS,
    )

    MANAGER = SchedulerManager(config_dir=config_dir, state_dir=state_dir)
    server = BoundedThreadingHTTPServer(
        (ui_bind, ui_port),
        UIRequestHandler,
        max_workers=ui_max_workers,
        queue_size=ui_queue_size,
        request_timeout_seconds=ui_request_timeout,
    )
    shutdown_started = threading.Event()

    def shutdown_handler(_signum: int, _frame: Any) -> None:
        if shutdown_started.is_set():
            return
        shutdown_started.set()
        threading.Thread(target=server.shutdown, name="ui-http-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    print(f"[ui] ClamAV UI available at http://{ui_bind}:{ui_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        if MANAGER is not None:
            MANAGER.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(run_server())
