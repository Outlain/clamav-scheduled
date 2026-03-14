import importlib.util
import sys
import unittest
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


if __name__ == "__main__":
    unittest.main()
