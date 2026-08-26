#!/usr/bin/env python3
"""Run a bounded GNU find enumeration with low-overhead progress heartbeats."""

from __future__ import annotations

import argparse
import json
import os
import selectors
import signal
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


READ_SIZE = 64 * 1024
POLL_SECONDS = 0.25
TERMINATION_GRACE_SECONDS = 10.0
DEFAULT_HEARTBEAT_SECONDS = 30
MAX_DIAGNOSTIC_LINES = 100
MAX_DIAGNOSTIC_BUFFER_BYTES = 64 * 1024
MAX_DISPLAY_CHARACTERS = 1024


def display_text(value: bytes | str | None) -> str:
    if value is None:
        return "null"
    decoded = os.fsdecode(value) if isinstance(value, bytes) else value
    if len(decoded) > MAX_DISPLAY_CHARACTERS:
        prefix_length = MAX_DISPLAY_CHARACTERS // 4
        suffix_length = MAX_DISPLAY_CHARACTERS - prefix_length - 3
        decoded = f"{decoded[:prefix_length]}...{decoded[-suffix_length:]}"
    return json.dumps(decoded, ensure_ascii=True)


class DualLogger:
    def __init__(self, scanlog: Path) -> None:
        self.scanlog = scanlog

    def emit(self, line: str) -> None:
        print(line, flush=True)
        encoded = f"{line}\n".encode("utf-8", "backslashreplace")
        descriptor = os.open(self.scanlog, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, encoded)
        finally:
            os.close(descriptor)


@dataclass
class TraversalProgress:
    visited_entries: int = 0
    observed_bytes: int = 0
    latest_path: bytes | None = None
    pending: bytes = b""

    def feed(self, chunk: bytes) -> None:
        self.observed_bytes += len(chunk)
        fields = (self.pending + chunk).split(b"\0")
        self.pending = fields.pop()
        for field in fields:
            if not field:
                continue
            self.visited_entries += 1
            self.latest_path = field


@dataclass
class DiagnosticLines:
    pending: bytes = b""

    def feed(self, chunk: bytes, *, final: bool = False) -> list[bytes]:
        fields = (self.pending + chunk).split(b"\n")
        self.pending = fields.pop()
        lines = [field.rstrip(b"\r") for field in fields if field]
        if len(self.pending) > MAX_DIAGNOSTIC_BUFFER_BYTES:
            lines.append(self.pending[:MAX_DIAGNOSTIC_BUFFER_BYTES] + b" ...[truncated]")
            self.pending = b""
        if final and self.pending:
            lines.append(self.pending.rstrip(b"\r"))
            self.pending = b""
        return lines


def build_find_command(
    find_binary: str,
    source_path: str,
    output_path: str,
    reference_file: str | None,
) -> list[str]:
    # Print every visited entry to stdout for progress accounting, but write
    # only regular eligible files to the NUL-delimited scan list.
    command = [find_binary, "--", source_path, "-print0", "-type", "f"]
    if reference_file:
        command.extend(["(", "-newer", reference_file, "-o", "-cnewer", reference_file, ")"])
    command.extend(["-fprint0", output_path])
    return command


def terminate_process_group(process: subprocess.Popen[bytes], sig: signal.Signals) -> None:
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        return


def terminate_process_group_safely(
    process: subprocess.Popen[bytes],
    sig: signal.Signals,
    *,
    stopped: bool,
) -> None:
    if stopped:
        terminate_process_group(process, signal.SIGCONT)
    terminate_process_group(process, sig)


def pause_file_exists(pause_file: Path) -> bool:
    try:
        info = pause_file.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise RuntimeError(
            f"pause file must be a regular file and not a symlink: {pause_file}"
        )
    return True


def wait_for_initial_resume(
    *,
    label: str,
    pause_file: Path,
    heartbeat_seconds: int,
    logger: DualLogger,
) -> None:
    if not pause_file_exists(pause_file):
        return

    logger.emit(f"[{label}] Enumeration paused by operator.")
    paused_started = time.monotonic()
    next_heartbeat = paused_started + heartbeat_seconds
    while pause_file_exists(pause_file):
        now = time.monotonic()
        if now >= next_heartbeat:
            logger.emit(
                f"[{label}] Enumeration pause heartbeat: process_started=false "
                f"paused_seconds={int(now - paused_started)}."
            )
            while next_heartbeat <= now:
                next_heartbeat += heartbeat_seconds
        time.sleep(min(POLL_SECONDS, max(0.0, next_heartbeat - now)))
    logger.emit(f"[{label}] Enumeration resumed by operator.")


def normalized_return_code(return_code: int) -> int:
    if return_code >= 0:
        return min(return_code, 255)
    return min(128 + abs(return_code), 255)


def run_enumeration(
    *,
    label: str,
    source_path: str,
    output_path: Path,
    reference_file: str | None,
    timeout_seconds: int,
    heartbeat_seconds: int,
    find_binary: str,
    pause_file: Path,
    logger: DualLogger,
) -> int:
    if not pause_file.is_absolute():
        raise ValueError("pause-file must be absolute")
    wait_for_initial_resume(
        label=label,
        pause_file=pause_file,
        heartbeat_seconds=heartbeat_seconds,
        logger=logger,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb"):
        pass

    process = subprocess.Popen(
        build_find_command(find_binary, source_path, str(output_path), reference_file),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None
    assert process.stderr is not None

    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")

    traversal = TraversalProgress()
    diagnostics = DiagnosticLines()
    diagnostic_lines = 0
    suppressed_diagnostics = 0
    started = time.monotonic()
    deadline = started + timeout_seconds
    next_heartbeat = started + heartbeat_seconds
    previous_heartbeat = started
    previous_entries = 0
    timed_out = False
    termination_deadline: float | None = None
    kill_sent = False
    process_stopped = False
    pause_started: float | None = None
    paused_total = 0.0
    next_pause_heartbeat = 0.0

    def emit_diagnostic(line: bytes) -> None:
        nonlocal diagnostic_lines, suppressed_diagnostics
        diagnostic_lines += 1
        if diagnostic_lines <= MAX_DIAGNOSTIC_LINES:
            logger.emit(
                f"[WARN] [{label}] Enumeration diagnostic from find: detail={display_text(line)}"
            )
        else:
            suppressed_diagnostics += 1

    try:
        while selector.get_map() or process.poll() is None:
            now = time.monotonic()
            pause_requested = pause_file_exists(pause_file)
            if (
                pause_requested
                and not timed_out
                and not process_stopped
                and process.poll() is None
            ):
                terminate_process_group(process, signal.SIGSTOP)
                process_stopped = True
                pause_started = now
                next_pause_heartbeat = now + heartbeat_seconds
                logger.emit(f"[{label}] Enumeration paused by operator.")
            elif not pause_requested and process_stopped:
                terminate_process_group(process, signal.SIGCONT)
                process_stopped = False
                assert pause_started is not None
                pause_duration = max(0.0, now - pause_started)
                paused_total += pause_duration
                deadline += pause_duration
                next_heartbeat += pause_duration
                previous_heartbeat += pause_duration
                pause_started = None
                logger.emit(f"[{label}] Enumeration resumed by operator.")

            if not process_stopped and not timed_out and now >= deadline:
                timed_out = True
                termination_deadline = now + TERMINATION_GRACE_SECONDS
                terminate_process_group_safely(
                    process,
                    signal.SIGTERM,
                    stopped=process_stopped,
                )
            elif (
                timed_out
                and process.poll() is None
                and termination_deadline is not None
                and now >= termination_deadline
                and not kill_sent
            ):
                kill_sent = True
                terminate_process_group_safely(
                    process,
                    signal.SIGKILL,
                    stopped=process_stopped,
                )

            if process_stopped:
                wait_for = min(POLL_SECONDS, max(0.0, next_pause_heartbeat - now))
            else:
                wait_for = min(POLL_SECONDS, max(0.0, next_heartbeat - now))
            if not timed_out and not process_stopped:
                wait_for = min(wait_for, max(0.0, deadline - now))
            elif termination_deadline is not None and not kill_sent:
                wait_for = min(wait_for, max(0.0, termination_deadline - now))

            for key, _mask in selector.select(wait_for):
                try:
                    chunk = os.read(key.fileobj.fileno(), READ_SIZE)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if key.data == "stdout":
                    traversal.feed(chunk)
                else:
                    for diagnostic in diagnostics.feed(chunk):
                        emit_diagnostic(diagnostic)

            now = time.monotonic()
            if process_stopped and now >= next_pause_heartbeat and process.poll() is None:
                assert pause_started is not None
                logger.emit(
                    f"[{label}] Enumeration pause heartbeat: process_started=true "
                    f"visited_entries={traversal.visited_entries} "
                    f"paused_seconds={int(now - pause_started)} "
                    f"active_elapsed={int(pause_started - started - paused_total)}s "
                    f"latest_path={display_text(traversal.latest_path)}"
                )
                while next_pause_heartbeat <= now:
                    next_pause_heartbeat += heartbeat_seconds
            elif not process_stopped and now >= next_heartbeat and process.poll() is None:
                window_seconds = max(1, int(now - previous_heartbeat))
                new_entries = traversal.visited_entries - previous_entries
                try:
                    raw_list_bytes = output_path.stat().st_size
                except OSError:
                    raw_list_bytes = 0
                logger.emit(
                    f"[{label}] Enumeration progress: "
                    f"visited_entries={traversal.visited_entries} "
                    f"new_entries={new_entries} "
                    f"window_seconds={window_seconds} "
                    f"raw_list_bytes={raw_list_bytes} "
                    f"elapsed={int(now - started - paused_total)}s "
                    f"latest_path={display_text(traversal.latest_path)}"
                )
                previous_entries = traversal.visited_entries
                previous_heartbeat = now
                while next_heartbeat <= now:
                    next_heartbeat += heartbeat_seconds

        for diagnostic in diagnostics.feed(b"", final=True):
            emit_diagnostic(diagnostic)
        if suppressed_diagnostics:
            logger.emit(
                f"[WARN] [{label}] Enumeration diagnostics suppressed: "
                f"additional_lines={suppressed_diagnostics}."
            )

        return_code = process.wait()
        if timed_out:
            return 124
        if traversal.pending:
            logger.emit(
                f"[ERROR] [{label}] Enumeration progress stream ended without a NUL terminator."
            )
            return 1
        return normalized_return_code(return_code)
    finally:
        selector.close()
        if process.poll() is None:
            terminate_process_group_safely(
                process,
                signal.SIGKILL,
                stopped=process_stopped,
            )
            process.wait()
        process.stdout.close()
        process.stderr.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", choices=("FULL", "CHANGED"), required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--reference-file")
    parser.add_argument("--timeout-seconds", required=True, type=int)
    parser.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=DEFAULT_HEARTBEAT_SECONDS,
    )
    parser.add_argument("--find-binary", default="find")
    parser.add_argument("--pause-file", required=True, type=Path)
    parser.add_argument("--scanlog", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = DualLogger(args.scanlog)
    try:
        if args.timeout_seconds < 1:
            raise ValueError("timeout-seconds must be at least 1")
        if not 1 <= args.heartbeat_seconds <= 3600:
            raise ValueError("heartbeat-seconds must be between 1 and 3600")
        if not os.path.isabs(args.source_path):
            raise ValueError("source-path must be absolute")
        if not args.output.is_absolute():
            raise ValueError("output must be absolute")
        if args.reference_file and not os.path.isabs(args.reference_file):
            raise ValueError("reference-file must be absolute")
        if not args.pause_file.is_absolute():
            raise ValueError("pause-file must be absolute")
        return run_enumeration(
            label=args.label,
            source_path=args.source_path,
            output_path=args.output,
            reference_file=args.reference_file,
            timeout_seconds=args.timeout_seconds,
            heartbeat_seconds=args.heartbeat_seconds,
            find_binary=args.find_binary,
            pause_file=args.pause_file,
            logger=logger,
        )
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        detail = display_text(str(exc))
        try:
            logger.emit(f"[ERROR] [{args.label}] Enumeration helper failed: detail={detail}")
        except OSError as log_exc:
            print(
                f"Enumeration helper failed and could not write its scan log: {exc}; {log_exc}",
                file=sys.stderr,
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
