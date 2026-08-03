#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
import socket
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def load_scanner():
    candidates = [
        Path("/usr/local/bin/clamd_session_scan.py"),
        Path(__file__).resolve().parents[1] / "scripts" / "clamd_session_scan.py",
    ]
    module_path = next(path for path in candidates if path.exists())
    spec = importlib.util.spec_from_file_location("integration_clamd_session_scan", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


scanner_module = load_scanner()


def clamd_command(socket_path: str, command: bytes) -> str:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect(socket_path)
        connection.sendall(b"z" + command + b"\0")
        chunks: list[bytes] = []
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                raise RuntimeError("clamd closed the integration-test connection")
            terminator = chunk.find(b"\0")
            if terminator >= 0:
                chunks.append(chunk[:terminator])
                return b"".join(chunks).decode("utf-8", "replace").strip()
            chunks.append(chunk)


def file_entry(path: str, root: str):
    stat_result = os.stat(path, follow_symlinks=False)
    return scanner_module.FileEntry(
        path=path,
        size_bytes=stat_result.st_size,
        root=root,
        device=stat_result.st_dev,
        inode=stat_result.st_ino,
        modified_ns=stat_result.st_mtime_ns,
        changed_ns=stat_result.st_ctime_ns,
    )


def write_signature(path: Path, name: str, marker: bytes) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(f"{name}:0:*:{marker.hex().upper()}\n", encoding="ascii")
    os.replace(temporary, path)


def wait_for_socket(socket_path: str, process: subprocess.Popen[bytes], timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = "socket has not appeared"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"clamd exited during startup with code {process.returncode}")
        try:
            if clamd_command(socket_path, b"PING") == "PONG":
                return
        except (OSError, RuntimeError) as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"clamd did not become ready: {last_error}")


def main() -> int:
    marker = b"CodexClamAVIntegrationThreat"
    threat_name = "Codex.Integration.Threat"
    with tempfile.TemporaryDirectory(prefix="clamav-integration-") as temp_dir:
        base = Path(temp_dir)
        definitions = base / "defs"
        scan_root = base / "downloads"
        quarantine = base / "quarantine"
        clamd_temp = base / "tmp"
        definitions.mkdir()
        scan_root.mkdir()
        quarantine.mkdir()
        clamd_temp.mkdir()
        socket_path = str(base / "clamd.sock")
        config_path = base / "clamd.conf"
        output_path = base / "clamd-output.log"

        write_signature(definitions / "initial.ndb", "Codex.Integration.Initial", b"NotTheThreatMarker")
        config_path.write_text(
            "\n".join(
                [
                    f"DatabaseDirectory {definitions}",
                    f"LocalSocket {socket_path}",
                    "LocalSocketMode 600",
                    f"PidFile {base / 'clamd.pid'}",
                    f"LogFile {base / 'clamd.log'}",
                    "LogTime yes",
                    "LogFileMaxSize 10M",
                    "LogRotate yes",
                    "Foreground yes",
                    "MaxThreads 2",
                    "MaxQueue 4",
                    "ReadTimeout 60",
                    "CommandReadTimeout 30",
                    "SelfCheck 30",
                    "MaxScanTime 60000",
                    "MaxScanSize 64M",
                    "MaxFileSize 32M",
                    "MaxRecursion 16",
                    "MaxFiles 1000",
                    "AlertExceedsMax yes",
                    "ConcurrentDatabaseReload no",
                    f"TemporaryDirectory {clamd_temp}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        with output_path.open("wb") as output_handle:
            process = subprocess.Popen(
                ["clamd", "-c", str(config_path)],
                stdout=output_handle,
                stderr=subprocess.STDOUT,
            )
            try:
                wait_for_socket(socket_path, process)
                version = clamd_command(socket_path, b"VERSION")
                if version != "ClamAV 1.4.5" and not version.startswith("ClamAV 1.4.5/"):
                    raise AssertionError(f"unexpected ClamAV version: {version}")

                clean_path = scan_root / "before-reload.bin"
                clean_path.write_bytes(marker)
                clean_scanner = scanner_module.SessionScanner(socket_path, timeout_seconds=30)
                try:
                    clean_result = clean_scanner.scan_entry(file_entry(str(clean_path), str(scan_root)))
                finally:
                    clean_scanner.close()
                if clean_result[0] != "CLEAN":
                    raise AssertionError(f"custom threat was detected before reload: {clean_result}")

                limit_path = scan_root / "policy-limit.bin"
                with limit_path.open("wb") as limit_handle:
                    limit_handle.truncate(33 * 1024 * 1024)
                limit_scanner = scanner_module.SessionScanner(socket_path, timeout_seconds=30)
                try:
                    limit_result = limit_scanner.scan_entry(
                        file_entry(str(limit_path), str(scan_root))
                    )
                finally:
                    limit_scanner.close()
                if limit_result[0] != "POLICY_LIMIT":
                    raise AssertionError(
                        f"MaxFileSize alert was not classified as a policy limit: {limit_result}"
                    )

                write_signature(definitions / "daily.ndb", threat_name, marker)
                reload_reply = clamd_command(socket_path, b"RELOAD")
                if "RELOADING" not in reload_reply.upper():
                    raise AssertionError(f"unexpected RELOAD reply: {reload_reply}")

                unusual_name = os.fsencode(scan_root) + b"/threat-\n-\xff.bin"
                descriptor = os.open(unusual_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.write(descriptor, b"prefix-" + marker + b"-suffix")
                os.close(descriptor)
                unusual_path = os.fsdecode(unusual_name)

                infected_result = None
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    entry = file_entry(unusual_path, str(scan_root))
                    infected_scanner = scanner_module.SessionScanner(socket_path, timeout_seconds=30)
                    try:
                        infected_result = infected_scanner.scan_entry(entry)
                    finally:
                        infected_scanner.close()
                    if infected_result[0] == "INFECTED":
                        break
                    time.sleep(0.25)

                reported_threat_name = f"{threat_name}.UNOFFICIAL"
                if infected_result != ("INFECTED", unusual_path, reported_threat_name):
                    raise AssertionError(f"signature reload or threat retention failed: {infected_result}")

                entry = file_entry(unusual_path, str(scan_root))
                destination = scanner_module.move_to_quarantine(
                    unusual_path,
                    str(quarantine),
                    [str(scan_root)],
                    expected_entry=entry,
                )
                if os.path.exists(unusual_path):
                    raise AssertionError("infected source remained after quarantine")
                if Path(destination).stat().st_mode & 0o777 != 0o600:
                    raise AssertionError("quarantine destination is not mode 0600")

                results_path = base / "results.jsonl"
                writer = scanner_module.ResultsWriter(str(results_path))
                writer.write(
                    "INFECTED",
                    entry.size_bytes,
                    1,
                    unusual_path,
                    scan_label="FULL",
                    threat_name=reported_threat_name,
                    quarantine_path=destination,
                    quarantine_success=True,
                )
                writer.close()
                payload = json.loads(results_path.read_text(encoding="utf-8"))
                if payload["scan"] != "FULL" or payload["threat"] != reported_threat_name:
                    raise AssertionError(f"structured threat result is incomplete: {payload}")
                if payload["source"] != unusual_path or not payload["quarantine_success"]:
                    raise AssertionError(f"structured quarantine result is incomplete: {payload}")

                print(
                    f"integration passed: {version}; policy_limit={limit_result[2]}; "
                    f"threat={reported_threat_name}; quarantine_mode=0600"
                )
            except Exception:
                output_handle.flush()
                try:
                    print(output_path.read_text(encoding="utf-8", errors="replace"), file=sys.stderr)
                except OSError:
                    pass
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
