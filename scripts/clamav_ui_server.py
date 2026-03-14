#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from copy import deepcopy
from datetime import datetime
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

SCAN_START_RE = re.compile(
    r"^\[(?P<label>FULL|CHANGED)\] Scanning (?P<total>\d+) files with persistent_session_workers=(?P<workers>\d+)$"
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


BOOTSTRAP_ENV_KEYS = {
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "PYTHONUNBUFFERED",
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
    "path_enumeration_timeout": 300,
    "path_unavailable_retry_interval": 300,
    "scan_path_marker": "",
    "quarantine_dir": "/downloads/quarantine",
    "scanlog": "/var/log/clamav/clamav_scheduled.log",
    "force_full_flag": "",
}


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temp_path, path)


def normalize_path_entry(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} entries must be strings.")
    entry = value.strip()
    if not entry:
        raise ValueError(f"{field_name} entries must not be empty.")
    if not entry.startswith("/"):
        raise ValueError(f"{field_name} entries must be absolute paths: {entry}")
    normalized = entry.rstrip("/") or "/"
    return normalized


def normalize_path_list(value: Any, field_name: str, required: bool) -> list[str]:
    if value is None:
        entries: list[str] = []
    elif isinstance(value, str):
        entries = [part.strip() for part in value.replace("\r", "").replace(":", "\n").split("\n")]
    elif isinstance(value, list):
        entries = [str(part).strip() for part in value]
    else:
        raise ValueError(f"{field_name} must be a list of absolute paths.")

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_entry in entries:
        if not raw_entry:
            continue
        normalized_entry = normalize_path_entry(raw_entry, field_name)
        if normalized_entry not in seen:
            seen.add(normalized_entry)
            normalized.append(normalized_entry)

    if required and not normalized:
        raise ValueError(f"{field_name} must include at least one absolute path.")

    return normalized


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


def normalize_int(value: Any, field_name: str, minimum: int = 0) -> int:
    try:
        integer = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be an integer.") from None

    if integer < minimum:
        comparator = "greater than 0" if minimum == 1 else f"at least {minimum}"
        raise ValueError(f"{field_name} must be {comparator}.")
    return integer


def validate_and_normalize_config(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(DEFAULT_CONFIG)
    normalized.update(payload)

    tz_value = normalize_optional_string(normalized.get("tz")) or DEFAULT_CONFIG["tz"]
    try:
        ZoneInfo(tz_value)
    except Exception as exc:  # pragma: no cover - depends on system tzdb
        raise ValueError(f"Invalid timezone: {tz_value}") from exc

    scan_paths = normalize_path_list(normalized.get("scan_paths"), "scan_paths", required=True)
    for scan_path in scan_paths:
        if not os.path.isdir(scan_path):
            raise ValueError(f"Scan path does not exist or is not a directory inside the container: {scan_path}")

    exclude_paths = normalize_path_list(normalized.get("exclude_paths"), "exclude_paths", required=False)

    quarantine_dir = normalize_path_entry(
        normalize_optional_string(normalized.get("quarantine_dir")) or f"{scan_paths[0]}/quarantine",
        "quarantine_dir",
    )
    scanlog = normalize_path_entry(
        normalize_optional_string(normalized.get("scanlog")) or str(DEFAULT_CONFIG["scanlog"]),
        "scanlog",
    )
    force_full_flag = normalize_optional_string(normalized.get("force_full_flag"))
    if force_full_flag:
        force_full_flag = normalize_path_entry(force_full_flag, "force_full_flag")

    result = {
        "version": 1,
        "tz": tz_value,
        "maxthreads": normalize_int(normalized.get("maxthreads"), "maxthreads", minimum=1),
        "scan_paths": scan_paths,
        "exclude_paths": exclude_paths,
        "full_scan_parallel_jobs": normalize_int(normalized.get("full_scan_parallel_jobs"), "full_scan_parallel_jobs", minimum=1),
        "changed_scan_parallel_jobs": normalize_int(normalized.get("changed_scan_parallel_jobs"), "changed_scan_parallel_jobs", minimum=1),
        "full_progress_steps": normalize_int(normalized.get("full_progress_steps"), "full_progress_steps", minimum=1),
        "changed_progress_steps": normalize_int(normalized.get("changed_progress_steps"), "changed_progress_steps", minimum=1),
        "full_chunk_size": normalize_int(normalized.get("full_chunk_size"), "full_chunk_size", minimum=0),
        "changed_chunk_size": normalize_int(normalized.get("changed_chunk_size"), "changed_chunk_size", minimum=0),
        "changed_scan_days": normalize_days(normalized.get("changed_scan_days"), "changed_scan_days"),
        "changed_scan_times": normalize_times(normalized.get("changed_scan_times"), "changed_scan_times"),
        "full_scan_days": normalize_days(normalized.get("full_scan_days"), "full_scan_days"),
        "full_scan_times": normalize_times(normalized.get("full_scan_times"), "full_scan_times"),
        "scan_failure_retry_interval": normalize_int(normalized.get("scan_failure_retry_interval"), "scan_failure_retry_interval", minimum=1),
        "force_full_poll_interval": normalize_int(normalized.get("force_full_poll_interval"), "force_full_poll_interval", minimum=1),
        "path_check_timeout": normalize_int(normalized.get("path_check_timeout"), "path_check_timeout", minimum=1),
        "path_enumeration_timeout": normalize_int(normalized.get("path_enumeration_timeout"), "path_enumeration_timeout", minimum=1),
        "path_unavailable_retry_interval": normalize_int(normalized.get("path_unavailable_retry_interval"), "path_unavailable_retry_interval", minimum=1),
        "scan_path_marker": normalize_optional_string(normalized.get("scan_path_marker")),
        "quarantine_dir": quarantine_dir,
        "scanlog": scanlog,
        "force_full_flag": force_full_flag,
        "updated_at": utc_now_iso(),
    }

    if "created_at" in normalized:
        result["created_at"] = normalize_optional_string(normalized.get("created_at")) or utc_now_iso()
    else:
        result["created_at"] = utc_now_iso()

    return result


def serialize_config_for_scheduler(config: dict[str, Any]) -> dict[str, str]:
    first_scan_path = config["scan_paths"][0]
    force_full_flag = config.get("force_full_flag") or f"{first_scan_path}/.clamav_force_full_scan.flag"

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


class SchedulerManager:
    def __init__(self, config_dir: Path, state_dir: Path) -> None:
        self.config_dir = config_dir
        self.state_dir = state_dir
        self.config_path = config_dir / "ui-config.json"
        self.history_path = config_dir / "ui-history.json"
        self.static_dir = Path("/usr/local/share/clamav-ui")
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._process: subprocess.Popen[bytes] | None = None
        self._process_exit_code: int | None = None
        self._config_error = ""
        self._config: dict[str, Any] | None = None
        self._history: list[dict[str, Any]] = read_json(self.history_path, default=[]) or []
        self._recent_logs: deque[str] = deque(maxlen=250)
        self._phase = "unconfigured"
        self._next_wake = ""
        self._last_event = "Waiting for UI configuration."
        self._last_warning = ""
        self._current_scan: dict[str, Any] | None = None
        self._last_summary: dict[str, Any] | None = self._history[-1] if self._history else None
        self._current_cycle_started_at = ""
        self._last_scan_kind = ""
        self._log_path: Path | None = None
        self._log_offset = 0
        self._log_inode: int | None = None

        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self._load_config_from_disk()
        self._replay_existing_log()
        if self._config is not None and not self._config_error:
            self._start_scheduler_locked()

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
                "config": deepcopy(self._config) if self._config is not None else deepcopy(DEFAULT_CONFIG),
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
            return deepcopy(DEFAULT_CONFIG)

    def save_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = validate_and_normalize_config(payload)
        write_json_atomic(self.config_path, normalized)
        with self._lock:
            self._config = normalized
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
            flag_path.parent.mkdir(parents=True, exist_ok=True)
            flag_path.touch()
            self._last_event = f"Force-full flag created at {force_full_flag}."

    def restart_scheduler(self) -> dict[str, Any]:
        with self._lock:
            if self._config is None or self._config_error:
                raise ValueError("UI mode is not configured yet.")
            self._restart_scheduler_locked()
            return self._status_payload_locked()

    def _load_config_from_disk(self) -> None:
        if not self.config_path.exists():
            self._config = None
            self._config_error = ""
            self._phase = "unconfigured"
            return

        try:
            raw_config = read_json(self.config_path, default={}) or {}
            self._config = validate_and_normalize_config(raw_config)
            if self._config.get("updated_at") != raw_config.get("updated_at"):
                write_json_atomic(self.config_path, self._config)
            self._attach_log_file(Path(self._config["scanlog"]), replay=False)
            self._phase = "starting"
            self._last_event = "Loaded UI configuration from disk."
        except Exception as exc:
            self._config = None
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
        elif not scheduler_running and phase not in {"unconfigured", "config_error"}:
            phase = "stopped"

        payload = {
            "mode": "ui",
            "configured": self._config is not None and not self._config_error,
            "config_error": self._config_error,
            "scheduler_running": scheduler_running,
            "scheduler_pid": process.pid if scheduler_running and process is not None else None,
            "scheduler_exit_code": self._process_exit_code,
            "phase": phase,
            "next_wake": self._next_wake,
            "last_event": self._last_event,
            "last_warning": self._last_warning,
            "current_scan": deepcopy(self._current_scan),
            "last_summary": deepcopy(self._last_summary),
            "scanlog": self._config["scanlog"] if self._config else DEFAULT_CONFIG["scanlog"],
        }
        if self._config is not None:
            payload["effective_force_full_flag"] = serialize_config_for_scheduler(self._config)["FORCE_FULL_FLAG"]
        else:
            payload["effective_force_full_flag"] = f"{DEFAULT_CONFIG['scan_paths'][0]}/.clamav_force_full_scan.flag"
        return payload

    def _start_scheduler_locked(self) -> None:
        if self._config is None:
            return
        if self._process is not None and self._process.poll() is None:
            return

        env = build_runtime_env(self._config, self.state_dir)
        self._attach_log_file(Path(self._config["scanlog"]), replay=True)
        self._process = subprocess.Popen(
            ["/bin/sh", "/usr/local/bin/clamav_scheduled.sh"],
            env=env,
        )
        self._process_exit_code = None
        self._phase = "starting"
        self._last_event = "Scheduler process started from UI mode."

    def _stop_scheduler_locked(self) -> None:
        if self._process is None:
            return
        if self._process.poll() is not None:
            self._process_exit_code = self._process.returncode
            self._process = None
            return

        self._process.terminate()
        try:
            self._process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)
        self._process_exit_code = self._process.returncode
        self._process = None
        self._phase = "stopped"
        self._last_event = "Scheduler process stopped."

    def _restart_scheduler_locked(self) -> None:
        self._stop_scheduler_locked()
        self._start_scheduler_locked()

    def _attach_log_file(self, log_path: Path, replay: bool) -> None:
        self._log_path = log_path
        self._log_offset = 0
        self._log_inode = None
        if replay:
            self._replay_existing_log()

    def _replay_existing_log(self) -> None:
        if self._log_path is None or not self._log_path.exists():
            return
        self._recent_logs.clear()
        for line in recent_tail_lines(self._log_path, max_lines=200):
            self._handle_log_line(line, replay=True)
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
            self._stop_event.wait(1.0)

    def _poll_process_locked(self) -> None:
        if self._process is None:
            return
        return_code = self._process.poll()
        if return_code is None:
            return
        self._process_exit_code = return_code
        self._process = None
        self._phase = "stopped"
        if return_code == 0:
            self._last_event = "Scheduler process exited cleanly."
        else:
            self._last_warning = f"Scheduler exited with code {return_code}."
            self._last_event = self._last_warning

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

    def _append_history_locked(self, entry: dict[str, Any]) -> None:
        if self._history and self._history[-1] == entry:
            return
        self._history.append(entry)
        if len(self._history) > 100:
            self._history = self._history[-100:]
        write_json_atomic(self.history_path, self._history)
        self._last_summary = entry

    def _handle_log_line(self, raw_line: str, replay: bool = False) -> None:
        line = sanitize_line(raw_line)
        if not line:
            return

        self._recent_logs.append(line)

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
            if self._current_scan is None:
                self._last_scan_kind = ""
            return

        if line.startswith("=== FULL SCAN starting ==="):
            self._phase = "scanning"
            self._last_scan_kind = "FULL"
            self._current_scan = {
                "label": "FULL",
                "display_label": format_scan_label("FULL"),
                "started_at": self._current_cycle_started_at or utc_now_iso(),
            }
            self._last_event = "Full scan started."
            return

        if line.startswith("=== CHANGED-FILES scan starting ==="):
            self._phase = "scanning"
            self._last_scan_kind = "CHANGED"
            self._current_scan = {
                "label": "CHANGED",
                "display_label": format_scan_label("CHANGED"),
                "started_at": self._current_cycle_started_at or utc_now_iso(),
            }
            self._last_event = "Changed-files scan started."
            return

        if line.startswith("=== Scan cycle paused due to unavailable scan path ==="):
            self._phase = "paused"
            self._last_warning = line
            self._last_event = line
            return

        if line.startswith("[LOCKED]"):
            self._phase = "waiting_lock"
            self._last_event = line
            return

        if line.startswith("[WARN]"):
            self._last_warning = line
            self._last_event = line

        scan_start_match = SCAN_START_RE.match(line)
        if scan_start_match:
            label = scan_start_match.group("label")
            if self._current_scan is None or self._current_scan.get("label") != label:
                self._current_scan = {
                    "label": label,
                    "display_label": format_scan_label(label),
                    "started_at": self._current_cycle_started_at or utc_now_iso(),
                }
            self._current_scan["total_files"] = int(scan_start_match.group("total"))
            self._current_scan["workers"] = int(scan_start_match.group("workers"))
            self._last_event = line
            return

        progress_config_match = PROGRESS_CONFIG_RE.match(line)
        if progress_config_match:
            if self._current_scan is not None:
                self._current_scan["progress_mode"] = progress_config_match.group("mode")
                self._current_scan["progress_interval"] = int(progress_config_match.group("interval"))
                self._current_scan["progress_detail"] = progress_config_match.group("detail")
            return

        progress_match = PROGRESS_RE.match(line)
        if progress_match:
            label = progress_match.group("label")
            if self._current_scan is None or self._current_scan.get("label") != label:
                self._current_scan = {
                    "label": label,
                    "display_label": format_scan_label(label),
                    "started_at": self._current_cycle_started_at or utc_now_iso(),
                }
            self._phase = "scanning"
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
                    "updated_at": utc_now_iso(),
                }
            )
            self._last_event = line
            return

        summary_match = SUMMARY_RE.match(line)
        if summary_match:
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
                "roots": [],
            }
            self._append_history_locked(entry)
            self._phase = "cycle_complete"
            self._last_event = line
            self._current_scan = None
            self._last_scan_kind = label
            return

        root_match = ROOT_SUMMARY_RE.match(line)
        if root_match and self._history:
            latest = self._history[-1]
            if latest.get("label") == root_match.group("label"):
                latest.setdefault("roots", []).append(
                    {
                        "root": root_match.group("root"),
                        "files": int(root_match.group("files")),
                        "processed_files": int(root_match.group("processed_files")),
                        "bytes": root_match.group("bytes"),
                        "processed_bytes": root_match.group("processed_bytes"),
                        "infected": int(root_match.group("infected")),
                        "vanished": int(root_match.group("vanished")),
                        "errors": int(root_match.group("errors")),
                    }
                )
                write_json_atomic(self.history_path, self._history)
            return

        if line.startswith("=== Scan cycle finished ==="):
            if self._process is not None and self._process.poll() is None:
                self._phase = "idle"
            self._last_event = line
            return

        if line.startswith("[FORCE]") or line.startswith("[CHANGED]") or line.startswith("[ERROR]"):
            self._last_event = line


MANAGER: SchedulerManager | None = None


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, status: int, body: bytes, content_type: str) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


class UIRequestHandler(BaseHTTPRequestHandler):
    server_version = "ClamAVScheduledUI/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[ui] {self.address_string()} - {fmt % args}", flush=True)

    def do_GET(self) -> None:
        assert MANAGER is not None
        parsed = urlparse(self.path)
        path = parsed.path

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
            json_response(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return

        json_response(self, HTTPStatus.OK, {"ok": True, "status": status, "config": MANAGER.get_config()})

    def do_POST(self) -> None:
        assert MANAGER is not None
        parsed = urlparse(self.path)
        if parsed.path == "/api/actions/force-full":
            try:
                MANAGER.force_full_scan()
            except ValueError as exc:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            json_response(self, HTTPStatus.OK, {"ok": True, "status": MANAGER.get_status()})
            return

        if parsed.path == "/api/actions/restart":
            try:
                status = MANAGER.restart_scheduler()
            except ValueError as exc:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            json_response(self, HTTPStatus.OK, {"ok": True, "status": status})
            return

        json_response(self, HTTPStatus.NOT_FOUND, {"error": "Not found"})

    def _read_json_body(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

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

    config_dir = Path(os.environ.get("CONFIG_DIR", "/config"))
    state_dir = Path(os.environ.get("STATE_DIR", "/state"))
    ui_bind = os.environ.get("UI_BIND", "0.0.0.0")
    ui_port = int(os.environ.get("UI_PORT", "8080"))

    MANAGER = SchedulerManager(config_dir=config_dir, state_dir=state_dir)
    server = ThreadingHTTPServer((ui_bind, ui_port), UIRequestHandler)

    def shutdown_handler(_signum: int, _frame: Any) -> None:
        server.shutdown()

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
