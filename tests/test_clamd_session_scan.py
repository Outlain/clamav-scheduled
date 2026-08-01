import importlib.util
import json
import os
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "clamd_session_scan.py"
SPEC = importlib.util.spec_from_file_location("clamd_session_scan", MODULE_PATH)
clamd_session_scan = importlib.util.module_from_spec(SPEC)
assert SPEC is not None
assert SPEC.loader is not None
sys.modules[SPEC.name] = clamd_session_scan
SPEC.loader.exec_module(clamd_session_scan)


class FakeSocket:
    def __init__(self, chunks):
        self._chunks = list(chunks)
        self.sent = []
        self.sendmsg_calls = []
        self.closed = False

    def recv(self, _size):
        if self._chunks:
            return self._chunks.pop(0)
        return b""

    def sendall(self, payload):
        self.sent.append(payload)

    def sendmsg(self, buffers, ancillary):
        self.sendmsg_calls.append((buffers, ancillary))
        return sum(len(buffer) for buffer in buffers)

    def close(self):
        self.closed = True


class SessionScannerTests(unittest.TestCase):
    def test_read_reply_stops_at_nul_terminator(self):
        scanner = clamd_session_scan.SessionScanner("/tmp/clamd.sock")
        scanner.sock = FakeSocket([b"1: /downloads/file.txt: OK\0"])

        reply = scanner.read_reply()

        self.assertEqual(reply, b"1: /downloads/file.txt: OK")

    def test_read_reply_handles_fragmented_messages(self):
        scanner = clamd_session_scan.SessionScanner("/tmp/clamd.sock")
        scanner.sock = FakeSocket([b"1: /downloads/", b"file.txt: Vir", b"us FOUND\0"])

        reply = scanner.read_reply()

        self.assertEqual(reply, b"1: /downloads/file.txt: Virus FOUND")

    def test_reply_returns_vanished_for_missing_file(self):
        with mock.patch.object(os.path, "exists", return_value=False), mock.patch.object(os.path, "isdir", return_value=True):
            status, scanned_path, threat_name = clamd_session_scan.parse_clamd_scan_reply(
                b"1: fd[7]: File path check failure: No such file or directory. ERROR",
                "/downloads/missing.txt",
            )

        self.assertEqual(status, "VANISHED")
        self.assertEqual(scanned_path, "/downloads/missing.txt")
        self.assertEqual(threat_name, "")

    def test_reply_keeps_non_missing_errors_as_error(self):
        with mock.patch.object(os.path, "exists", return_value=True), mock.patch.object(os.path, "isdir", return_value=True):
            status, scanned_path, threat_name = clamd_session_scan.parse_clamd_scan_reply(
                b"1: fd[8]: Permission denied. ERROR",
                "/downloads/denied.txt",
            )

        self.assertEqual(status, "ERROR")
        self.assertEqual(scanned_path, "/downloads/denied.txt")
        self.assertEqual(threat_name, "")

    def test_missing_file_reply_stays_error_when_parent_directory_is_gone(self):
        with mock.patch.object(os.path, "exists", return_value=False), mock.patch.object(os.path, "isdir", return_value=False):
            status, scanned_path, threat_name = clamd_session_scan.parse_clamd_scan_reply(
                b"1: fd[9]: No such file or directory. ERROR",
                "/downloads/subdir/missing.txt",
            )

        self.assertEqual(status, "ERROR")
        self.assertEqual(scanned_path, "/downloads/subdir/missing.txt")
        self.assertEqual(threat_name, "")

    def test_descriptor_reply_preserves_threat_signature_and_requested_path(self):
        status, scanned_path, threat_name = clamd_session_scan.parse_clamd_scan_reply(
            b"1: fd[11]: Win.Trojan.Agent FOUND",
            "/downloads/file.exe",
        )

        self.assertEqual(status, "INFECTED")
        self.assertEqual(scanned_path, "/downloads/file.exe")
        self.assertEqual(threat_name, "Win.Trojan.Agent")

    def test_scan_entry_transfers_verified_descriptor(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "sample.bin"
            source.write_bytes(b"clean")
            stat_result = source.stat()
            entry = clamd_session_scan.FileEntry(
                path=str(source),
                size_bytes=stat_result.st_size,
                root=temp_dir,
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                modified_ns=stat_result.st_mtime_ns,
                changed_ns=stat_result.st_ctime_ns,
            )
            scanner = clamd_session_scan.SessionScanner("/tmp/clamd.sock")
            fake_socket = FakeSocket([b"1: fd[4]: OK\0"])
            scanner.sock = fake_socket

            status, scanned_path, threat_name = scanner.scan_entry(entry)

            self.assertEqual((status, scanned_path, threat_name), ("CLEAN", str(source), ""))
            self.assertEqual(fake_socket.sent, [b"zFILDES\0"])
            self.assertEqual(fake_socket.sendmsg_calls[0][0], [b"x"])
            ancillary = fake_socket.sendmsg_calls[0][1][0]
            self.assertEqual(ancillary[0], clamd_session_scan.socket.SOL_SOCKET)
            self.assertEqual(ancillary[1], clamd_session_scan.socket.SCM_RIGHTS)

    def test_scan_entry_refuses_replaced_file_before_transfer(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "sample.bin"
            source.write_bytes(b"first")
            stat_result = source.stat()
            entry = clamd_session_scan.FileEntry(
                path=str(source),
                size_bytes=stat_result.st_size,
                root=temp_dir,
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                modified_ns=stat_result.st_mtime_ns,
                changed_ns=stat_result.st_ctime_ns,
            )
            source.unlink()
            source.write_bytes(b"replacement")
            scanner = clamd_session_scan.SessionScanner("/tmp/clamd.sock")
            fake_socket = FakeSocket([])
            scanner.sock = fake_socket

            with self.assertRaises(clamd_session_scan.FileChangedError):
                scanner.scan_entry(entry)

            self.assertEqual(fake_socket.sent, [])


class ScanListTests(unittest.TestCase):
    def test_nul_list_preserves_newlines_tabs_colons_and_non_utf8_names(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            root.mkdir()
            root_bytes = os.fsencode(root)
            paths = [
                root_bytes + b"/line\nbreak.bin",
                root_bytes + b"/tab\tname.bin",
                root_bytes + b"/colon:name.bin",
                root_bytes + b"/invalid-\xff.bin",
            ]
            for path in paths:
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.write(descriptor, b"content")
                os.close(descriptor)

            list_file = Path(temp_dir) / "scan-list.nul"
            list_file.write_bytes(b"\0".join(paths + [paths[0]]) + b"\0")

            entries, root_stats, total_bytes = clamd_session_scan.build_entries(
                str(list_file),
                [str(root)],
            )

            self.assertCountEqual([os.fsencode(entry.path) for entry in entries], paths)
            self.assertEqual(len(entries), 4)
            self.assertEqual(root_stats[str(root)]["files"], 4)
            self.assertEqual(total_bytes, 4 * len(b"content"))

    def test_nul_list_must_end_with_terminator(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            list_file = Path(temp_dir) / "scan-list.nul"
            list_file.write_bytes(b"/downloads/file.bin")
            with self.assertRaises(ValueError):
                list(clamd_session_scan.iter_nul_paths(str(list_file)))

    def test_scan_list_rejects_more_unique_files_than_configured(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            root.mkdir()
            first = root / "first.bin"
            second = root / "second.bin"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            list_file = Path(temp_dir) / "scan-list.nul"
            list_file.write_bytes(os.fsencode(first) + b"\0" + os.fsencode(second) + b"\0")

            with self.assertRaisesRegex(ValueError, "configured maximum of 1"):
                clamd_session_scan.build_entries(str(list_file), [str(root)], max_files=1)

    def test_log_values_escape_line_breaks(self):
        value = clamd_session_scan.format_log_value("name\nnext\trow")
        self.assertNotIn("\n", value)
        self.assertIn("\\n", value)
        self.assertIn("\\t", value)


class RuntimeLimitTests(unittest.TestCase):
    def test_auto_progress_is_capped_by_effective_workers(self):
        workers, interval, mode, _detail = clamd_session_scan.calculate_scan_runtime(10, 8, 0, 100)
        self.assertEqual((workers, interval, mode), (8, 8, "auto"))

    def test_worker_limit_is_rejected(self):
        with self.assertRaises(ValueError):
            clamd_session_scan.calculate_scan_runtime(100, 65, 0, 100)

    def test_quarantine_failure_prevents_successful_scan_outcome(self):
        root_stats = {
            "/downloads": {
                "files": 1,
                "bytes": 10,
                "processed_files": 0,
                "processed_bytes": 0,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
            }
        }
        metrics = clamd_session_scan.Metrics(1, 10, root_stats, 1)
        entry = clamd_session_scan.FileEntry("/downloads/threat.bin", 10, "/downloads")
        metrics.record(entry, "INFECTED", 1, True)

        self.assertFalse(clamd_session_scan.scan_completed_successfully(metrics))


class QuarantineTests(unittest.TestCase):
    def test_move_is_no_overwrite_and_preserves_relative_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            quarantine = Path(temp_dir) / "quarantine"
            source = root / "nested" / "sample.exe"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"infected")
            stat_result = source.stat()
            entry = clamd_session_scan.FileEntry(
                path=str(source),
                size_bytes=stat_result.st_size,
                root=str(root),
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                modified_ns=stat_result.st_mtime_ns,
                changed_ns=stat_result.st_ctime_ns,
            )

            destination = clamd_session_scan.move_to_quarantine(
                str(source), str(quarantine), [str(root)], expected_entry=entry
            )

            self.assertFalse(source.exists())
            self.assertEqual(Path(destination).read_bytes(), b"infected")
            self.assertEqual(Path(destination).relative_to(quarantine), Path("nested/sample.exe"))
            self.assertEqual(Path(destination).stat().st_mode & 0o777, 0o600)

    def test_changed_source_is_not_quarantined(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            quarantine = Path(temp_dir) / "quarantine"
            root.mkdir()
            source = root / "sample.exe"
            source.write_bytes(b"first")
            stat_result = source.stat()
            entry = clamd_session_scan.FileEntry(
                path=str(source),
                size_bytes=stat_result.st_size,
                root=str(root),
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                modified_ns=stat_result.st_mtime_ns,
                changed_ns=stat_result.st_ctime_ns,
            )
            source.unlink()
            source.write_bytes(b"replacement")

            with self.assertRaises(clamd_session_scan.FileChangedError):
                clamd_session_scan.move_to_quarantine(
                    str(source), str(quarantine), [str(root)], expected_entry=entry
                )

            self.assertEqual(source.read_bytes(), b"replacement")
            self.assertFalse(quarantine.exists())

    def test_cross_filesystem_fallback_uses_exclusive_copy_then_removes_source(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            quarantine = Path(temp_dir) / "quarantine"
            source = root / "sample.exe"
            root.mkdir()
            source.write_bytes(b"infected")
            stat_result = source.stat()
            entry = clamd_session_scan.FileEntry(
                path=str(source),
                size_bytes=stat_result.st_size,
                root=str(root),
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                modified_ns=stat_result.st_mtime_ns,
                changed_ns=stat_result.st_ctime_ns,
            )

            with mock.patch.object(
                clamd_session_scan.os,
                "link",
                side_effect=OSError(clamd_session_scan.errno.EXDEV, "cross-device link"),
            ):
                destination = clamd_session_scan.move_to_quarantine(
                    str(source), str(quarantine), [str(root)], expected_entry=entry
                )

            self.assertFalse(source.exists())
            self.assertEqual(Path(destination).read_bytes(), b"infected")
            self.assertEqual(Path(destination).stat().st_mode & 0o777, 0o600)

    def test_broken_symlink_collision_is_not_reused(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            quarantine = Path(temp_dir) / "quarantine"
            source = root / "sample.exe"
            source.parent.mkdir()
            source.write_bytes(b"infected")
            quarantine.mkdir()
            (quarantine / "sample.exe").symlink_to("missing-target")

            destination = clamd_session_scan.unique_quarantine_path(
                str(source), str(quarantine), [str(root)]
            )

            self.assertEqual(Path(destination).name, "sample.1.exe")


class ResultsWriterTests(unittest.TestCase):
    def test_writer_persists_structured_threat_details(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            results_path = Path(temp_dir) / "results.jsonl"
            writer = clamd_session_scan.ResultsWriter(str(results_path))
            writer.write(
                "INFECTED",
                10,
                25,
                "/downloads/name\nwith-newline.exe",
                scan_label="FULL",
                threat_name="Win.Trojan.Agent",
                quarantine_path="/quarantine/sample.exe",
                quarantine_success=True,
            )
            writer.close()

            payload = json.loads(results_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["scan"], "FULL")
            self.assertEqual(payload["threat"], "Win.Trojan.Agent")
            self.assertEqual(payload["source"], "/downloads/name\nwith-newline.exe")
            self.assertTrue(payload["quarantine_success"])

    def test_logger_rotates_before_exceeding_configured_size(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "scan.log"
            logger = clamd_session_scan.Logger(str(log_path), max_bytes=20, rotations=2)
            with mock.patch("builtins.print"):
                logger.log("first-line")
                logger.log("second-line")
            logger.close()

            self.assertEqual(log_path.read_text(encoding="utf-8"), "second-line\n")
            self.assertEqual((Path(str(log_path) + ".1")).read_text(encoding="utf-8"), "first-line\n")


class WorkerTests(unittest.TestCase):
    def test_infected_worker_logs_and_persists_signature_and_quarantine_result(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            source = root / "sample.exe"
            root.mkdir()
            source.write_bytes(b"infected")
            stat_result = source.stat()
            entry = clamd_session_scan.FileEntry(
                path=str(source),
                size_bytes=stat_result.st_size,
                root=str(root),
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                modified_ns=stat_result.st_mtime_ns,
                changed_ns=stat_result.st_ctime_ns,
            )
            work_queue = clamd_session_scan.queue.Queue()
            work_queue.put(entry)
            root_stats = {
                str(root): {
                    "files": 1,
                    "bytes": stat_result.st_size,
                    "processed_files": 0,
                    "processed_bytes": 0,
                    "infected": 0,
                    "vanished": 0,
                    "errors": 0,
                }
            }
            metrics = clamd_session_scan.Metrics(1, stat_result.st_size, root_stats, 1)
            logger = mock.Mock()
            results = mock.Mock()
            fake_scanner = mock.Mock()
            fake_scanner.scan_entry.return_value = ("INFECTED", str(source), "Win.Trojan.Agent")

            with mock.patch.object(clamd_session_scan, "SessionScanner", return_value=fake_scanner), mock.patch.object(
                clamd_session_scan,
                "move_to_quarantine",
                return_value="/quarantine/sample.exe",
            ):
                clamd_session_scan.worker_loop(
                    work_queue,
                    logger,
                    results,
                    metrics,
                    "/tmp/clamd.sock",
                    "/quarantine",
                    [str(root)],
                    "FULL",
                    clamd_session_scan.time.monotonic_ns(),
                )

            threat_payloads = []
            for call in logger.log.call_args_list:
                message = call.args[0]
                if message.startswith("{"):
                    threat_payloads.append(json.loads(message))
            self.assertEqual(len(threat_payloads), 1)
            self.assertEqual(threat_payloads[0]["threat"], "Win.Trojan.Agent")
            self.assertEqual(threat_payloads[0]["source"], str(source))
            self.assertEqual(threat_payloads[0]["quarantine"], "/quarantine/sample.exe")
            self.assertTrue(threat_payloads[0]["quarantine_success"])
            self.assertEqual(results.write.call_args.kwargs["threat_name"], "Win.Trojan.Agent")
            self.assertEqual(results.write.call_args.kwargs["scan_label"], "FULL")
            self.assertTrue(results.write.call_args.kwargs["quarantine_success"])
            self.assertEqual(metrics.infected_files, 1)

    def test_worker_write_failure_is_reported_to_coordinator(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            source = root / "sample.bin"
            root.mkdir()
            source.write_bytes(b"clean")
            stat_result = source.stat()
            entry = clamd_session_scan.FileEntry(
                path=str(source),
                size_bytes=stat_result.st_size,
                root=str(root),
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                modified_ns=stat_result.st_mtime_ns,
                changed_ns=stat_result.st_ctime_ns,
            )
            work_queue = clamd_session_scan.queue.Queue()
            work_queue.put(entry)
            worker_errors = clamd_session_scan.queue.SimpleQueue()
            metrics = clamd_session_scan.Metrics(
                1,
                stat_result.st_size,
                {
                    str(root): {
                        "files": 1,
                        "bytes": stat_result.st_size,
                        "processed_files": 0,
                        "processed_bytes": 0,
                        "infected": 0,
                        "vanished": 0,
                        "errors": 0,
                    }
                },
                1,
            )
            fake_scanner = mock.Mock()
            fake_scanner.scan_entry.return_value = ("CLEAN", str(source), "")
            results = mock.Mock()
            results.write.side_effect = OSError("results volume full")

            with mock.patch.object(clamd_session_scan, "SessionScanner", return_value=fake_scanner):
                clamd_session_scan.worker_loop(
                    work_queue,
                    mock.Mock(),
                    results,
                    metrics,
                    "/tmp/clamd.sock",
                    "/quarantine",
                    [str(root)],
                    "FULL",
                    clamd_session_scan.time.monotonic_ns(),
                    worker_errors,
                )

            self.assertIsInstance(worker_errors.get_nowait(), OSError)
            self.assertEqual(metrics.processed_files, 0)


class MetricsTests(unittest.TestCase):
    def test_progress_snapshot_tracks_window_deltas_between_logs(self):
        root_stats = {
            "/downloads": {
                "files": 2,
                "bytes": 300,
                "processed_files": 0,
                "processed_bytes": 0,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
            }
        }
        metrics = clamd_session_scan.Metrics(total_files=2, total_bytes=300, root_stats=root_stats, progress_interval=1)

        first_entry = clamd_session_scan.FileEntry(path="/downloads/a.txt", size_bytes=100, root="/downloads")
        second_entry = clamd_session_scan.FileEntry(path="/downloads/b.txt", size_bytes=200, root="/downloads")

        metrics.record(first_entry, "CLEAN", 10, False)
        first_snapshot = metrics.progress_snapshot(1000)

        metrics.record(second_entry, "CLEAN", 10, False)
        second_snapshot = metrics.progress_snapshot(3000)

        self.assertEqual(first_snapshot["window_files"], 1)
        self.assertEqual(first_snapshot["window_bytes"], 100)
        self.assertEqual(first_snapshot["window_elapsed_ms"], 1000)
        self.assertEqual(second_snapshot["window_files"], 1)
        self.assertEqual(second_snapshot["window_bytes"], 200)
        self.assertEqual(second_snapshot["window_elapsed_ms"], 2000)


if __name__ == "__main__":
    unittest.main()
