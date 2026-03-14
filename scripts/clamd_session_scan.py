#!/usr/bin/env python3

from __future__ import annotations

import argparse
import errno
import os
import queue
import re
import shutil
import socket
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path


SESSION_PREFIX_RE = re.compile(r"^\d+:\s+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persistent clamd session scanner")
    parser.add_argument("--socket", required=True, dest="socket_path")
    parser.add_argument("--list-file", required=True)
    parser.add_argument("--results-file", required=True)
    parser.add_argument("--quarantine-dir", required=True)
    parser.add_argument("--workers", required=True, type=int)
    parser.add_argument("--progress-interval", required=True, type=int)
    parser.add_argument("--label", required=True)
    parser.add_argument("--scanlog", required=True)
    parser.add_argument("--scan-paths", required=True)
    return parser.parse_args()


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
    def __init__(self, scanlog_path: str) -> None:
        self._lock = threading.Lock()
        self._handle = open(scanlog_path, "a", encoding="utf-8", errors="backslashreplace", buffering=1)

    def log(self, message: str) -> None:
        with self._lock:
            print(message, flush=True)
            self._handle.write(message + "\n")

    def close(self) -> None:
        with self._lock:
            self._handle.close()


class ResultsWriter:
    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._handle = open(path, "w", encoding="utf-8", errors="backslashreplace", buffering=1)

    def write(self, status: str, size_bytes: int, duration_ms: int, path: str) -> None:
        with self._lock:
            self._handle.write(f"{status}\t{size_bytes}\t{duration_ms}\t{path}\n")

    def close(self) -> None:
        with self._lock:
            self._handle.close()


@dataclass(frozen=True)
class FileEntry:
    path: str
    size_bytes: int
    root: str


class Metrics:
    def __init__(self, total_files: int, total_bytes: int, root_stats: dict[str, dict[str, int]], progress_interval: int) -> None:
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.root_stats = root_stats
        self.progress_interval = max(1, progress_interval)
        self.processed_files = 0
        self.infected_files = 0
        self.error_files = 0
        self.quarantine_failures = 0
        self.slowest_files: list[tuple[int, str, str, int]] = []
        self._lock = threading.Lock()

    def record(self, entry: FileEntry, status: str, duration_ms: int, quarantine_failed: bool) -> tuple[int, bool]:
        with self._lock:
            self.processed_files += 1

            if status == "INFECTED":
                self.infected_files += 1
                self.root_stats[entry.root]["infected"] += 1
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


class SessionScanner:
    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self.sock: socket.socket | None = None

    def connect(self) -> None:
        self.close()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
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

    def scan_path(self, path: str) -> tuple[str, str]:
        if self.sock is None:
            self.connect()

        assert self.sock is not None

        payload = b"zSCAN " + os.fsencode(path) + b"\0"
        self.sock.sendall(payload)
        reply = self.read_reply()

        decoded = reply.decode("utf-8", "replace").rstrip("\n")
        decoded = SESSION_PREFIX_RE.sub("", decoded, count=1)

        if ": " not in decoded:
            raise RuntimeError(f"Unexpected clamd reply: {decoded}")

        scanned_path, detail = decoded.rsplit(": ", 1)
        if detail == "OK":
            return "CLEAN", scanned_path
        if detail.endswith("FOUND"):
            return "INFECTED", scanned_path
        if detail.endswith("ERROR"):
            return "ERROR", scanned_path
        raise RuntimeError(f"Unexpected clamd reply detail: {decoded}")


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
    if not candidate.exists():
        return str(candidate)

    stem = candidate.stem
    suffix = candidate.suffix
    for index in range(1, 10000):
        alternate = candidate.with_name(f"{stem}.{index}{suffix}")
        if not alternate.exists():
            return str(alternate)
    raise RuntimeError(f"Unable to allocate quarantine path for {path}")


def move_to_quarantine(path: str, quarantine_dir: str, roots: list[str]) -> str:
    destination = unique_quarantine_path(path, quarantine_dir, roots)
    try:
        os.rename(path, destination)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        shutil.move(path, destination)
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
            quarantine_failed = False
            scan_start_ns = time.monotonic_ns()

            try:
                for attempt in range(2):
                    try:
                        status, scanned_path = scanner.scan_path(entry.path)
                        break
                    except (ConnectionError, BrokenPipeError, OSError):
                        scanner.close()
                        if attempt == 1:
                            raise
                else:
                    raise RuntimeError("scanner retry loop exhausted")

                if status == "INFECTED":
                    try:
                        destination = move_to_quarantine(entry.path, quarantine_dir, roots)
                        logger.log(f"[{label}] Infected file moved to quarantine: {entry.path} -> {destination}")
                    except Exception as exc:  # pragma: no cover - operational path
                        quarantine_failed = True
                        status = "ERROR"
                        logger.log(f"[ERROR] [{label}] Failed to quarantine infected file {entry.path}: {exc}")
            except Exception as exc:
                logger.log(f"[ERROR] [{label}] Scan failed for {entry.path}: {exc}")
                status = "ERROR"

            duration_ms = max(0, (time.monotonic_ns() - scan_start_ns) // 1_000_000)
            results.write(status, entry.size_bytes, duration_ms, entry.path)

            processed_files, should_log = metrics.record(entry, status, duration_ms, quarantine_failed)

            if should_log:
                elapsed_ms = max(1, (time.monotonic_ns() - start_ns) // 1_000_000)
                logger.log(
                    f"[{label}] Progress: {processed_files * 100 // metrics.total_files}% "
                    f"({processed_files}/{metrics.total_files}) ~{format_files_per_second(processed_files, elapsed_ms)}"
                )

            work_queue.task_done()
    finally:
        scanner.close()


def build_entries(list_file: str, roots: list[str]) -> tuple[list[FileEntry], dict[str, dict[str, int]], int]:
    entries: list[FileEntry] = []
    root_stats = {root: {"files": 0, "bytes": 0, "infected": 0, "errors": 0} for root in roots}
    total_bytes = 0

    with open(list_file, "r", encoding="utf-8", errors="surrogateescape") as handle:
        for raw_line in handle:
            path = raw_line.rstrip("\n")
            if not path:
                continue

            try:
                size_bytes = os.stat(path).st_size
            except OSError:
                size_bytes = 0

            root = match_root(path, roots)
            root_stats[root]["files"] += 1
            root_stats[root]["bytes"] += size_bytes
            total_bytes += size_bytes
            entries.append(FileEntry(path=path, size_bytes=size_bytes, root=root))

    return entries, root_stats, total_bytes


def main() -> int:
    args = parse_args()
    roots = [root for root in args.scan_paths.split(":") if root]

    logger = Logger(args.scanlog)
    results = ResultsWriter(args.results_file)

    try:
        entries, root_stats, total_bytes = build_entries(args.list_file, roots)
        total_files = len(entries)

        if total_files == 0:
            logger.log(f"[{args.label}] No files found to scan.")
            return 0

        logger.log(
            f"[{args.label}] Scanning {total_files} files with persistent_session_workers={args.workers} "
            f"progress_interval={args.progress_interval}"
        )

        work_queue: "queue.Queue[FileEntry]" = queue.Queue()
        for entry in entries:
            work_queue.put(entry)

        metrics = Metrics(total_files, total_bytes, root_stats, args.progress_interval)
        start_ns = time.monotonic_ns()

        threads = [
            threading.Thread(
                target=worker_loop,
                args=(work_queue, logger, results, metrics, args.socket_path, args.quarantine_dir, roots, args.label, start_ns),
                daemon=True,
            )
            for _ in range(max(1, args.workers))
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        elapsed_ms = max(1, (time.monotonic_ns() - start_ns) // 1_000_000)
        clean_files = max(0, metrics.processed_files - metrics.infected_files - metrics.error_files)

        logger.log(
            f"[{args.label}] Summary: scheduled_files={metrics.total_files} indexed_files={metrics.total_files} "
            f"processed_files={metrics.processed_files} clean={clean_files} infected={metrics.infected_files} "
            f"errors={metrics.error_files} bytes={format_bytes(metrics.total_bytes)} "
            f"elapsed={format_duration_ms(elapsed_ms)} "
            f"throughput={format_files_per_second(metrics.processed_files, elapsed_ms)} "
            f"data_rate={format_bytes_per_second(metrics.total_bytes, elapsed_ms)}"
        )

        for root in roots:
            stats = metrics.root_stats[root]
            if stats["files"] <= 0:
                continue
            logger.log(
                f"[{args.label}] Root summary {root}: files={stats['files']} "
                f"bytes={format_bytes(stats['bytes'])} infected={stats['infected']} errors={stats['errors']}"
            )

        for duration_ms, status, path, size_bytes in metrics.slowest_files:
            logger.log(
                f"[{args.label}] Slow file: duration={format_duration_ms(duration_ms)} "
                f"status={status} size={format_bytes(size_bytes)} path={path}"
            )

        return 0 if metrics.error_files == 0 else 1
    finally:
        results.close()
        logger.close()


if __name__ == "__main__":
    sys.exit(main())
