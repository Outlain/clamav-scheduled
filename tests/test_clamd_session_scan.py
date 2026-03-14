import importlib.util
import os
import sys
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
        self.closed = False

    def recv(self, _size):
        if self._chunks:
            return self._chunks.pop(0)
        return b""

    def sendall(self, payload):
        self.sent.append(payload)

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

    def test_scan_path_returns_vanished_for_missing_file_reply(self):
        scanner = clamd_session_scan.SessionScanner("/tmp/clamd.sock")
        scanner.sock = FakeSocket([b"1: /downloads/missing.txt: File path check failure: No such file or directory. ERROR\0"])

        with mock.patch.object(os.path, "exists", return_value=False), mock.patch.object(os.path, "isdir", return_value=True):
            status, scanned_path = scanner.scan_path("/downloads/missing.txt")

        self.assertEqual(status, "VANISHED")
        self.assertEqual(scanned_path, "/downloads/missing.txt")

    def test_scan_path_keeps_non_missing_errors_as_error(self):
        scanner = clamd_session_scan.SessionScanner("/tmp/clamd.sock")
        scanner.sock = FakeSocket([b"1: /downloads/denied.txt: Permission denied. ERROR\0"])

        with mock.patch.object(os.path, "exists", return_value=True), mock.patch.object(os.path, "isdir", return_value=True):
            status, scanned_path = scanner.scan_path("/downloads/denied.txt")

        self.assertEqual(status, "ERROR")
        self.assertEqual(scanned_path, "/downloads/denied.txt")

    def test_missing_file_reply_stays_error_when_parent_directory_is_gone(self):
        scanner = clamd_session_scan.SessionScanner("/tmp/clamd.sock")
        scanner.sock = FakeSocket([b"1: /downloads/subdir/missing.txt: No such file or directory. ERROR\0"])

        with mock.patch.object(os.path, "exists", return_value=False), mock.patch.object(os.path, "isdir", return_value=False):
            status, scanned_path = scanner.scan_path("/downloads/subdir/missing.txt")

        self.assertEqual(status, "ERROR")
        self.assertEqual(scanned_path, "/downloads/subdir/missing.txt")


if __name__ == "__main__":
    unittest.main()
