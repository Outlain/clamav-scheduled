#!/usr/bin/env python3

from __future__ import annotations

import argparse
import array
import errno
import json
import os
import queue
import re
import shutil
import socket
import stat
import sys
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


SESSION_PREFIX_RE = re.compile(r"^\d+:\s+")
QUARANTINE_MOVE_LOCK = threading.Lock()
MAX_SCAN_WORKERS = 64
MAX_PROGRESS_STEPS = 10_000
MAX_PROGRESS_INTERVAL = 1_000_000
MAX_SCHEDULED_FILE_CAP = 5_000_000


def is_missing_path_error(detail: str, path: str) -> bool:
    normalized_detail = detail.lower()

    if "no such file or directory" not in normalized_detail and "can't open file or directory" not in normalized_detail:
        return False

    parent_dir = os.path.dirname(path) or "."
    return not os.path.exists(path) and os.path.isdir(parent_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persistent clamd session scanner")
    parser.add_argument("--socket", required=True, dest="socket_path")
    parser.add_argument("--list-file", required=True)
    parser.add_argument("--results-file", required=True)
    parser.add_argument("--quarantine-dir", required=True)
    parser.add_argument("--configured-workers", required=True, type=int)
    parser.add_argument("--requested-progress-interval", required=True, type=int)
    parser.add_argument("--progress-steps", required=True, type=int)
    parser.add_argument("--max-files", required=True, type=int)
    parser.add_argument("--scanlog-max-bytes", required=True, type=int)
    parser.add_argument("--scanlog-rotations", required=True, type=int)
    parser.add_argument("--label", required=True)
    parser.add_argument("--scanlog", required=True)
    parser.add_argument("--scan-paths", required=True)
    return parser.parse_args()


def calculate_scan_runtime(
    total_files: int,
    configured_workers: int,
    requested_progress_interval: int,
    progress_steps: int,
) -> tuple[int, int, str, str]:
    if not 1 <= configured_workers <= MAX_SCAN_WORKERS:
        raise ValueError(f"configured workers must be between 1 and {MAX_SCAN_WORKERS}")
    if not 0 <= requested_progress_interval <= MAX_PROGRESS_INTERVAL:
        raise ValueError(f"requested progress interval must be between 0 and {MAX_PROGRESS_INTERVAL}")
    if not 1 <= progress_steps <= MAX_PROGRESS_STEPS:
        raise ValueError(f"progress steps must be between 1 and {MAX_PROGRESS_STEPS}")

    effective_workers = min(max(1, total_files), configured_workers)
    if requested_progress_interval > 0:
        return (
            effective_workers,
            requested_progress_interval,
            "fixed",
            f"configured_interval={requested_progress_interval}",
        )

    progress_interval = max(1, (total_files + progress_steps - 1) // progress_steps)
    progress_interval = max(progress_interval, effective_workers)
    return (
        effective_workers,
        progress_interval,
        "auto",
        f"derived_interval=ceil({total_files}/{progress_steps}) capped_by_worker_floor={effective_workers}",
    )


def format_log_value(value: object) -> str:
    return json.dumps(str(value), ensure_ascii=True)


def format_bytes(byte_count: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    value = float(byte_count)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.2f} {units[unit_index]}"


def format_duration_ms(duration_ms: int) -> str:
    if duration_ms < 1000:
        return f"{duration_ms}ms"

    total_seconds = duration_ms / 1000
    if total_seconds < 60:
        return f"{total_seconds:.2f}s"

    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = total_seconds - (hours * 3600) - (minutes * 60)

    if hours > 0:
        return f"{hours}h {minutes}m {seconds:.0f}s"
    return f"{minutes}m {seconds:.0f}s"


def format_files_per_second(file_count: int, elapsed_ms: int) -> str:
    rate = (file_count * 1000 / elapsed_ms) if elapsed_ms > 0 else 0.0
    if rate >= 10:
        return f"{rate:.1f} files/s"
    return f"{rate:.2f} files/s"


def format_bytes_per_second(byte_count: int, elapsed_ms: int) -> str:
    rate = (byte_count * 1000 / elapsed_ms) if elapsed_ms > 0 else 0.0
    return format_bytes(int(rate)).replace(" B", " B/s").replace(" KiB", " KiB/s").replace(" MiB", " MiB/s").replace(" GiB", " GiB/s").replace(" TiB", " TiB/s").replace(" PiB", " PiB/s")


class Logger:
    def __init__(self, scanlog_path: str, max_bytes: int, rotations: int) -> None:
        self._lock = threading.Lock()
        self._path = scanlog_path
        self._max_bytes = max_bytes
        self._rotations = rotations
        self._handle = open(scanlog_path, "a", encoding="utf-8", errors="backslashreplace", buffering=1)

    def _rotate_if_needed(self, incoming_bytes: int) -> None:
        try:
            current_size = os.fstat(self._handle.fileno()).st_size
        except OSError:
            return
        if current_size + incoming_bytes <= self._max_bytes:
            return

        self._handle.close()
        oldest = f"{self._path}.{self._rotations}"
        try:
            os.unlink(oldest)
        except FileNotFoundError:
            pass
        for index in range(self._rotations - 1, 0, -1):
            source = f"{self._path}.{index}"
            destination = f"{self._path}.{index + 1}"
            try:
                os.replace(source, destination)
            except FileNotFoundError:
                continue
        try:
            os.replace(self._path, f"{self._path}.1")
        except FileNotFoundError:
            pass
        self._handle = open(
            self._path,
            "a",
            encoding="utf-8",
            errors="backslashreplace",
            buffering=1,
        )

    def log(self, message: str) -> None:
        with self._lock:
            print(message, flush=True)
            incoming_bytes = len((message + "\n").encode("utf-8", "backslashreplace"))
            self._rotate_if_needed(incoming_bytes)
            self._handle.write(message + "\n")

    def close(self) -> None:
        with self._lock:
            self._handle.close()


class ResultsWriter:
    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._handle = open(path, "w", encoding="utf-8", errors="backslashreplace", buffering=1)

    def write(
        self,
        status: str,
        size_bytes: int,
        duration_ms: int,
        path: str,
        *,
        scan_label: str = "",
        threat_name: str = "",
        quarantine_path: str | None = None,
        quarantine_success: bool | None = None,
    ) -> None:
        payload = {
            "scan": scan_label or None,
            "status": status,
            "size_bytes": size_bytes,
            "duration_ms": duration_ms,
            "source": path,
            "threat": threat_name or None,
            "quarantine": quarantine_path,
            "quarantine_success": quarantine_success,
        }
        with self._lock:
            self._handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")

    def close(self) -> None:
        with self._lock:
            self._handle.close()


@dataclass(frozen=True)
class FileEntry:
    path: str
    size_bytes: int
    root: str
    device: int | None = None
    inode: int | None = None
    modified_ns: int | None = None
    changed_ns: int | None = None


class FileChangedError(RuntimeError):
    pass


class FileVanishedError(FileChangedError):
    pass


class QuarantineError(RuntimeError):
    def __init__(self, message: str, destination: str | None = None) -> None:
        super().__init__(message)
        self.destination = destination


def stat_matches_entry(stat_result: os.stat_result, entry: FileEntry) -> bool:
    if (
        entry.device is None
        or entry.inode is None
        or entry.modified_ns is None
        or entry.changed_ns is None
    ):
        return False
    return (
        stat_result.st_dev == entry.device
        and stat_result.st_ino == entry.inode
        and stat_result.st_size == entry.size_bytes
        and stat_result.st_mtime_ns == entry.modified_ns
        and stat_result.st_ctime_ns == entry.changed_ns
    )


def ensure_file_unchanged(entry: FileEntry) -> os.stat_result:
    try:
        stat_result = os.stat(entry.path, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise FileVanishedError(f"File vanished after enumeration: {entry.path}") from exc
    if not stat.S_ISREG(stat_result.st_mode):
        raise FileChangedError(f"Refusing to scan or quarantine a non-regular file: {entry.path}")
    if not stat_matches_entry(stat_result, entry):
        raise FileChangedError(f"File changed after enumeration; refusing unsafe quarantine move: {entry.path}")
    return stat_result


class Metrics:
    def __init__(self, total_files: int, total_bytes: int, root_stats: dict[str, dict[str, int]], progress_interval: int) -> None:
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.root_stats = root_stats
        self.progress_interval = max(1, progress_interval)
        self.processed_files = 0
        self.processed_bytes = 0
        self.infected_files = 0
        self.vanished_files = 0
        self.error_files = 0
        self.quarantine_failures = 0
        self.slowest_files: list[tuple[int, str, str, int]] = []
        self.last_log_processed_files = 0
        self.last_log_processed_bytes = 0
        self.last_log_elapsed_ms = 0
        self._lock = threading.Lock()

    def record(self, entry: FileEntry, status: str, duration_ms: int, quarantine_failed: bool) -> tuple[int, bool]:
        with self._lock:
            self.processed_files += 1
            self.processed_bytes += entry.size_bytes
            self.root_stats[entry.root]["processed_files"] += 1
            self.root_stats[entry.root]["processed_bytes"] += entry.size_bytes

            if status == "INFECTED":
                self.infected_files += 1
                self.root_stats[entry.root]["infected"] += 1
            elif status == "VANISHED":
                self.vanished_files += 1
                self.root_stats[entry.root]["vanished"] += 1
            elif status == "ERROR":
                self.error_files += 1
                self.root_stats[entry.root]["errors"] += 1

            if quarantine_failed:
                self.quarantine_failures += 1

            self.slowest_files.append((duration_ms, status, entry.path, entry.size_bytes))
            self.slowest_files.sort(key=lambda item: item[0], reverse=True)
            if len(self.slowest_files) > 3:
                self.slowest_files = self.slowest_files[:3]

            should_log = self.processed_files % self.progress_interval == 0 or self.processed_files == self.total_files
            return self.processed_files, should_log

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "processed_files": self.processed_files,
                "processed_bytes": self.processed_bytes,
                "infected_files": self.infected_files,
                "vanished_files": self.vanished_files,
                "error_files": self.error_files,
                "quarantine_failures": self.quarantine_failures,
            }

    def progress_snapshot(self, elapsed_ms: int) -> dict[str, int]:
        with self._lock:
            window_elapsed_ms = elapsed_ms - self.last_log_elapsed_ms
            if window_elapsed_ms < 1:
                window_elapsed_ms = 1

            snapshot = {
                "processed_files": self.processed_files,
                "processed_bytes": self.processed_bytes,
                "infected_files": self.infected_files,
                "vanished_files": self.vanished_files,
                "error_files": self.error_files,
                "quarantine_failures": self.quarantine_failures,
                "window_files": self.processed_files - self.last_log_processed_files,
                "window_bytes": self.processed_bytes - self.last_log_processed_bytes,
                "window_elapsed_ms": window_elapsed_ms,
            }

            self.last_log_processed_files = self.processed_files
            self.last_log_processed_bytes = self.processed_bytes
            self.last_log_elapsed_ms = elapsed_ms
            return snapshot


def parse_clamd_scan_reply(reply: bytes, requested_path: str) -> tuple[str, str, str]:
    decoded = reply.decode("utf-8", "replace").rstrip("\n")
    decoded = SESSION_PREFIX_RE.sub("", decoded, count=1)
    if ": " not in decoded:
        raise RuntimeError(f"Unexpected clamd reply: {decoded}")

    _response_identifier, detail = decoded.split(": ", 1)
    if detail == "OK":
        return "CLEAN", requested_path, ""
    if detail.endswith(" FOUND"):
        threat_name = detail.removesuffix(" FOUND").strip()
        if not threat_name:
            raise RuntimeError(f"Unexpected clamd infection reply without a signature: {decoded}")
        return "INFECTED", requested_path, threat_name
    if detail.endswith("ERROR"):
        if is_missing_path_error(detail, requested_path):
            return "VANISHED", requested_path, ""
        return "ERROR", requested_path, ""
    raise RuntimeError(f"Unexpected clamd reply detail: {decoded}")


def open_scannable_descriptor(entry: FileEntry) -> int:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(entry.path, flags)
    except FileNotFoundError as exc:
        raise FileVanishedError("File vanished after enumeration") from exc

    try:
        stat_result = os.fstat(descriptor)
        if not stat.S_ISREG(stat_result.st_mode):
            raise FileChangedError("Refusing to scan a non-regular file")
        if not stat_matches_entry(stat_result, entry):
            raise FileChangedError("File identity changed after enumeration")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


class SessionScanner:
    def __init__(self, socket_path: str, timeout_seconds: float = 930.0) -> None:
        self.socket_path = socket_path
        self.timeout_seconds = timeout_seconds
        self.sock: socket.socket | None = None

    def connect(self) -> None:
        self.close()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout_seconds)
        sock.connect(self.socket_path)
        sock.sendall(b"zIDSESSION\0")
        self.sock = sock

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.sendall(b"zEND\0")
            except OSError:
                pass
            self.sock.close()
        self.sock = None

    def read_reply(self) -> bytes:
        if self.sock is None:
            raise ConnectionError("clamd session is not connected")

        chunks: list[bytes] = []
        while True:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("clamd session closed unexpectedly")

            terminator_index = chunk.find(b"\0")
            if terminator_index >= 0:
                chunks.append(chunk[:terminator_index])
                return b"".join(chunks)

            chunks.append(chunk)

    def scan_entry(self, entry: FileEntry) -> tuple[str, str, str]:
        if self.sock is None:
            self.connect()

        assert self.sock is not None
        descriptor = open_scannable_descriptor(entry)
        try:
            descriptors = array.array("i", [descriptor])
            self.sock.sendall(b"zFILDES\0")
            sent = self.sock.sendmsg(
                [b"x"],
                [(socket.SOL_SOCKET, socket.SCM_RIGHTS, descriptors)],
            )
            if sent != 1:
                raise ConnectionError("clamd descriptor transfer was incomplete")
            return parse_clamd_scan_reply(self.read_reply(), entry.path)
        finally:
            os.close(descriptor)

    def scan_path(self, path: str) -> tuple[str, str, str]:
        try:
            stat_result = os.stat(path, follow_symlinks=False)
        except FileNotFoundError as exc:
            raise FileVanishedError("File vanished before scan") from exc
        entry = FileEntry(
            path=path,
            size_bytes=stat_result.st_size,
            root=os.path.dirname(path) or "/",
            device=stat_result.st_dev,
            inode=stat_result.st_ino,
            modified_ns=stat_result.st_mtime_ns,
            changed_ns=stat_result.st_ctime_ns,
        )
        return self.scan_entry(entry)


def match_root(path: str, roots: list[str]) -> str:
    best_root = roots[0]
    best_length = -1
    for root in roots:
        if path == root or path.startswith(root + os.sep):
            if len(root) > best_length:
                best_root = root
                best_length = len(root)
    return best_root


def unique_quarantine_path(path: str, quarantine_dir: str, roots: list[str]) -> str:
    root = match_root(path, roots)
    multiple_roots = len(roots) > 1
    try:
        relative_path = os.path.relpath(path, root)
    except ValueError:
        relative_path = os.path.basename(path)

    if relative_path.startswith(".."):
        relative_path = os.path.basename(path)

    if multiple_roots:
        root_prefix = os.path.basename(root.rstrip(os.sep)) or "root"
        candidate = Path(quarantine_dir) / root_prefix / relative_path
    else:
        candidate = Path(quarantine_dir) / relative_path

    candidate.parent.mkdir(parents=True, exist_ok=True)
    if not os.path.lexists(candidate):
        return str(candidate)

    stem = candidate.stem
    suffix = candidate.suffix
    for index in range(1, 10000):
        alternate = candidate.with_name(f"{stem}.{index}{suffix}")
        if not os.path.lexists(alternate):
            return str(alternate)
    raise RuntimeError(f"Unable to allocate quarantine path for {path}")


def copy_to_quarantine_no_replace(
    entry: FileEntry,
    quarantine_dir: str,
    roots: list[str],
) -> str:
    source_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    source_descriptor = os.open(entry.path, source_flags)
    destination_descriptor = -1
    destination = ""
    try:
        source_stat = os.fstat(source_descriptor)
        if not stat.S_ISREG(source_stat.st_mode) or not stat_matches_entry(source_stat, entry):
            raise FileChangedError(f"File changed before cross-filesystem quarantine copy: {entry.path}")

        for _attempt in range(10000):
            destination = unique_quarantine_path(entry.path, quarantine_dir, roots)
            try:
                destination_descriptor = os.open(
                    destination,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                break
            except FileExistsError:
                continue
        else:
            raise RuntimeError(f"Unable to reserve quarantine path for {entry.path}")

        copy_complete = False
        try:
            with os.fdopen(source_descriptor, "rb") as source_handle, os.fdopen(
                destination_descriptor, "wb"
            ) as destination_handle:
                source_descriptor = -1
                destination_descriptor = -1
                shutil.copyfileobj(source_handle, destination_handle)
                destination_handle.flush()
                os.fsync(destination_handle.fileno())
            copy_complete = True
        finally:
            if not copy_complete and destination:
                try:
                    os.unlink(destination)
                except FileNotFoundError:
                    pass

        try:
            ensure_file_unchanged(entry)
        except FileChangedError as exc:
            raise QuarantineError(
                f"The scanned file changed during quarantine copy and was not removed: {entry.path}",
                destination,
            ) from exc
        try:
            os.unlink(entry.path)
        except OSError as exc:
            raise QuarantineError(
                f"A quarantine copy was created but the source could not be removed: {entry.path}",
                destination,
            ) from exc
        return destination
    finally:
        if source_descriptor >= 0:
            os.close(source_descriptor)
        if destination_descriptor >= 0:
            os.close(destination_descriptor)


def move_to_quarantine(
    path: str,
    quarantine_dir: str,
    roots: list[str],
    expected_entry: FileEntry | None = None,
) -> str:
    if expected_entry is None:
        stat_result = os.stat(path, follow_symlinks=False)
        entry = FileEntry(
            path=path,
            size_bytes=stat_result.st_size,
            root=match_root(path, roots),
            device=stat_result.st_dev,
            inode=stat_result.st_ino,
            modified_ns=stat_result.st_mtime_ns,
            changed_ns=stat_result.st_ctime_ns,
        )
    else:
        entry = expected_entry
    with QUARANTINE_MOVE_LOCK:
        ensure_file_unchanged(entry)
        fallback_errnos = {errno.EXDEV, errno.EPERM, errno.EOPNOTSUPP}
        if hasattr(errno, "ENOTSUP"):
            fallback_errnos.add(errno.ENOTSUP)

        for _attempt in range(10000):
            destination = unique_quarantine_path(path, quarantine_dir, roots)
            try:
                os.link(path, destination, follow_symlinks=False)
                break
            except FileExistsError:
                continue
            except OSError as exc:
                if exc.errno in fallback_errnos:
                    return copy_to_quarantine_no_replace(entry, quarantine_dir, roots)
                raise
        else:
            raise RuntimeError(f"Unable to reserve quarantine path for {path}")

        destination_stat = os.stat(destination, follow_symlinks=False)
        if (
            not stat.S_ISREG(destination_stat.st_mode)
            or destination_stat.st_dev != entry.device
            or destination_stat.st_ino != entry.inode
        ):
            raise QuarantineError(
                f"Source identity changed while creating its quarantine link: {path}",
                destination,
            )
        os.chmod(destination, 0o600)
        try:
            source_stat = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise QuarantineError(
                f"Source changed after its quarantine link was created and was not removed: {path}",
                destination,
            ) from exc
        if (
            not stat.S_ISREG(source_stat.st_mode)
            or source_stat.st_dev != destination_stat.st_dev
            or source_stat.st_ino != destination_stat.st_ino
        ):
            raise QuarantineError(
                f"Source changed after its quarantine link was created and was not removed: {path}",
                destination,
            )

        try:
            os.unlink(path)
        except OSError as exc:
            raise QuarantineError(
                f"A quarantine link was created but the source could not be removed: {path}",
                destination,
            ) from exc
        return destination


def worker_loop(
    work_queue: "queue.Queue[FileEntry]",
    logger: Logger,
    results: ResultsWriter,
    metrics: Metrics,
    socket_path: str,
    quarantine_dir: str,
    roots: list[str],
    label: str,
    start_ns: int,
    worker_errors: "queue.SimpleQueue[BaseException] | None" = None,
) -> None:
    scanner = SessionScanner(socket_path)
    try:
        while True:
            try:
                entry = work_queue.get_nowait()
            except queue.Empty:
                return

            status = "ERROR"
            scanned_path = entry.path
            threat_name = ""
            quarantine_destination: str | None = None
            quarantine_success: bool | None = None
            quarantine_failed = False
            scan_start_ns = time.monotonic_ns()

            try:
                ensure_file_unchanged(entry)
                for attempt in range(2):
                    try:
                        status, scanned_path, threat_name = scanner.scan_entry(entry)
                        break
                    except (ConnectionError, BrokenPipeError, OSError):
                        scanner.close()
                        if attempt == 1:
                            raise
                else:
                    raise RuntimeError("scanner retry loop exhausted")

                if status == "INFECTED":
                    try:
                        ensure_file_unchanged(entry)
                        quarantine_destination = move_to_quarantine(
                            entry.path,
                            quarantine_dir,
                            roots,
                            expected_entry=entry,
                        )
                        quarantine_success = True
                    except QuarantineError as exc:  # pragma: no cover - operational path
                        quarantine_destination = exc.destination
                        quarantine_success = False
                        quarantine_failed = True
                        logger.log(
                            f"[ERROR] [{label}] Failed to quarantine infected file "
                            f"path={format_log_value(entry.path)} error={format_log_value(exc)}"
                        )
                    except Exception as exc:  # pragma: no cover - operational path
                        quarantine_success = False
                        quarantine_failed = True
                        logger.log(
                            f"[ERROR] [{label}] Failed to quarantine infected file "
                            f"path={format_log_value(entry.path)} error={format_log_value(exc)}"
                        )

                    threat_event = {
                        "event": "threat_detected",
                        "scan": label,
                        "threat": threat_name,
                        "source": entry.path,
                        "quarantine": quarantine_destination,
                        "quarantine_success": quarantine_success,
                    }
                    logger.log(json.dumps(threat_event, ensure_ascii=True, sort_keys=True))
                elif status == "VANISHED":
                    logger.log(
                        f"[{label}] File vanished before scan completed: path={format_log_value(entry.path)}"
                    )
            except FileVanishedError:
                logger.log(f"[{label}] File vanished before scan started: path={format_log_value(entry.path)}")
                status = "VANISHED"
            except Exception as exc:
                logger.log(
                    f"[ERROR] [{label}] Scan failed: path={format_log_value(entry.path)} "
                    f"error={format_log_value(exc)}"
                )
                status = "ERROR"

            duration_ms = max(0, (time.monotonic_ns() - scan_start_ns) // 1_000_000)
            results.write(
                status,
                entry.size_bytes,
                duration_ms,
                entry.path,
                scan_label=label,
                threat_name=threat_name,
                quarantine_path=quarantine_destination,
                quarantine_success=quarantine_success,
            )

            processed_files, should_log = metrics.record(entry, status, duration_ms, quarantine_failed)

            if should_log:
                elapsed_ms = max(1, (time.monotonic_ns() - start_ns) // 1_000_000)
                snapshot = metrics.progress_snapshot(elapsed_ms)
                clean_files = max(
                    0,
                    snapshot["processed_files"]
                    - snapshot["infected_files"]
                    - snapshot["vanished_files"]
                    - snapshot["error_files"],
                )
                logger.log(
                    f"[{label}] Progress: {processed_files * 100 // metrics.total_files}% "
                    f"({processed_files}/{metrics.total_files}) "
                    f"bytes={format_bytes(snapshot['processed_bytes'])}/{format_bytes(metrics.total_bytes)} "
                    f"clean={clean_files} infected={snapshot['infected_files']} vanished={snapshot['vanished_files']} errors={snapshot['error_files']} "
                    f"elapsed={format_duration_ms(elapsed_ms)} "
                    f"avg_throughput={format_files_per_second(processed_files, elapsed_ms)} "
                    f"window_throughput={format_files_per_second(snapshot['window_files'], snapshot['window_elapsed_ms'])} "
                    f"avg_data_rate={format_bytes_per_second(snapshot['processed_bytes'], elapsed_ms)} "
                    f"window_data_rate={format_bytes_per_second(snapshot['window_bytes'], snapshot['window_elapsed_ms'])}"
                )

            work_queue.task_done()
    except BaseException as exc:
        if worker_errors is None:
            raise
        worker_errors.put(exc)
    finally:
        scanner.close()


def iter_nul_paths(list_file: str) -> Iterator[str]:
    pending = b""
    with open(list_file, "rb") as handle:
        while True:
            chunk = handle.read(64 * 1024)
            if not chunk:
                break
            fields = (pending + chunk).split(b"\0")
            pending = fields.pop()
            for field in fields:
                if field:
                    yield os.fsdecode(field)

    if pending:
        raise ValueError(f"NUL-delimited scan list is missing its final terminator: {list_file}")


def build_entries(
    list_file: str,
    roots: list[str],
    max_files: int | None = None,
) -> tuple[list[FileEntry], dict[str, dict[str, int]], int]:
    entries: list[FileEntry] = []
    root_stats = {
        root: {
            "files": 0,
            "bytes": 0,
            "processed_files": 0,
            "processed_bytes": 0,
            "infected": 0,
            "vanished": 0,
            "errors": 0,
        }
        for root in roots
    }
    total_bytes = 0
    seen_paths: set[bytes] = set()

    for path in iter_nul_paths(list_file):
        encoded_path = os.fsencode(path)
        if encoded_path in seen_paths:
            continue
        seen_paths.add(encoded_path)
        if max_files is not None and len(entries) >= max_files:
            raise ValueError(f"scan list exceeds the configured maximum of {max_files} unique files")

        try:
            stat_result = os.stat(path, follow_symlinks=False)
            size_bytes = stat_result.st_size
            device = stat_result.st_dev
            inode = stat_result.st_ino
            modified_ns = stat_result.st_mtime_ns
            changed_ns = stat_result.st_ctime_ns
        except OSError:
            size_bytes = 0
            device = None
            inode = None
            modified_ns = None
            changed_ns = None

        root = match_root(path, roots)
        root_stats[root]["files"] += 1
        root_stats[root]["bytes"] += size_bytes
        total_bytes += size_bytes
        entries.append(
            FileEntry(
                path=path,
                size_bytes=size_bytes,
                root=root,
                device=device,
                inode=inode,
                modified_ns=modified_ns,
                changed_ns=changed_ns,
            )
        )

    return entries, root_stats, total_bytes


def scan_completed_successfully(metrics: Metrics) -> bool:
    return (
        metrics.processed_files == metrics.total_files
        and metrics.error_files == 0
        and metrics.quarantine_failures == 0
    )


def main() -> int:
    args = parse_args()
    roots = [root for root in args.scan_paths.split(":") if root]
    if not roots:
        print("[ERROR] --scan-paths must contain at least one path", file=sys.stderr)
        return 2

    if not 1_048_576 <= args.scanlog_max_bytes <= 1_073_741_824:
        print("[ERROR] --scanlog-max-bytes must be between 1048576 and 1073741824", file=sys.stderr)
        return 2
    if not 1 <= args.scanlog_rotations <= 20:
        print("[ERROR] --scanlog-rotations must be between 1 and 20", file=sys.stderr)
        return 2

    logger = Logger(args.scanlog, args.scanlog_max_bytes, args.scanlog_rotations)
    results = ResultsWriter(args.results_file)

    try:
        if not 1 <= args.max_files <= MAX_SCHEDULED_FILE_CAP:
            logger.log(
                f"[ERROR] [{args.label}] max files must be between 1 and {MAX_SCHEDULED_FILE_CAP}"
            )
            return 2
        try:
            entries, root_stats, total_bytes = build_entries(args.list_file, roots, args.max_files)
        except (OSError, ValueError) as exc:
            logger.log(f"[ERROR] [{args.label}] Could not index scan list: {format_log_value(exc)}")
            return 2
        total_files = len(entries)
        try:
            effective_workers, progress_interval, progress_mode, progress_detail = calculate_scan_runtime(
                max(1, total_files),
                args.configured_workers,
                args.requested_progress_interval,
                args.progress_steps,
            )
        except ValueError as exc:
            logger.log(f"[ERROR] [{args.label}] Invalid scan runtime settings: {format_log_value(exc)}")
            return 2

        if total_files == 0:
            logger.log(f"[{args.label}] No files found to scan.")
            return 0

        logger.log(
            f"[{args.label}] Scanning {total_files} files with persistent_session_workers={effective_workers}"
        )
        logger.log(
            f"[{args.label}] Progress logging uses file-count checkpoints, not scan chunks: "
            f"mode={progress_mode} progress_interval={progress_interval} {progress_detail}"
        )

        work_queue: "queue.Queue[FileEntry]" = queue.Queue()
        for entry in entries:
            work_queue.put(entry)

        metrics = Metrics(total_files, total_bytes, root_stats, progress_interval)
        start_ns = time.monotonic_ns()
        worker_errors: "queue.SimpleQueue[BaseException]" = queue.SimpleQueue()

        threads = [
            threading.Thread(
                target=worker_loop,
                args=(work_queue, logger, results, metrics, args.socket_path, args.quarantine_dir, roots, args.label, start_ns, worker_errors),
                daemon=True,
            )
            for _ in range(effective_workers)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        try:
            worker_error = worker_errors.get_nowait()
        except queue.Empty:
            worker_error = None
        if worker_error is not None:
            logger.log(
                f"[ERROR] [{args.label}] Scanner worker aborted before all files were processed: "
                f"{format_log_value(worker_error)}"
            )
            return 1

        elapsed_ms = max(1, (time.monotonic_ns() - start_ns) // 1_000_000)
        clean_files = max(0, metrics.processed_files - metrics.infected_files - metrics.vanished_files - metrics.error_files)

        logger.log(
            f"[{args.label}] Summary: scheduled_files={metrics.total_files} indexed_files={metrics.total_files} "
            f"processed_files={metrics.processed_files} clean={clean_files} infected={metrics.infected_files} vanished={metrics.vanished_files} "
            f"errors={metrics.error_files} quarantine_failures={metrics.quarantine_failures} "
            f"bytes={format_bytes(metrics.total_bytes)} "
            f"elapsed={format_duration_ms(elapsed_ms)} "
            f"avg_throughput={format_files_per_second(metrics.processed_files, elapsed_ms)} "
            f"avg_data_rate={format_bytes_per_second(metrics.processed_bytes, elapsed_ms)}"
        )

        for root in roots:
            stats = metrics.root_stats[root]
            if stats["files"] <= 0:
                continue
            logger.log(
                f"[{args.label}] Root summary {root}: files={stats['files']} processed_files={stats['processed_files']} "
                f"bytes={format_bytes(stats['bytes'])} processed_bytes={format_bytes(stats['processed_bytes'])} "
                f"infected={stats['infected']} vanished={stats['vanished']} errors={stats['errors']}"
            )

        for duration_ms, status, path, size_bytes in metrics.slowest_files:
            logger.log(
                f"[{args.label}] Slow file: duration={format_duration_ms(duration_ms)} "
                f"status={status} size={format_bytes(size_bytes)} path={format_log_value(path)}"
            )

        return 0 if scan_completed_successfully(metrics) else 1
    finally:
        results.close()
        logger.close()


if __name__ == "__main__":
    sys.exit(main())
