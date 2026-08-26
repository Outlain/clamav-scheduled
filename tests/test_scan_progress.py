import importlib.util
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def load_script_module(name: str):
    module_path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


scan_progress = load_script_module("scan_progress")


class ScanProgressTests(unittest.TestCase):
    def prepare(self, root: Path, **overrides):
        arguments = {
            "fingerprint": "sha256:one",
            "label": "FULL",
            "cutoff_epoch": 100,
            "reference_epoch": 50,
            "request_identity": "scheduled-full-slot-100",
            "request_mode": "scheduled",
            "max_age_seconds": 3600,
            "now_epoch": 200,
        }
        arguments.update(overrides)
        return scan_progress.prepare_session(root, **arguments)

    def populate_and_mark_ready(self, result, *, manifest: bytes = b"/downloads/a\0"):
        result.paths.roots_json.write_text(
            json.dumps(
                {
                    "version": 1,
                    "roots": [
                        {
                            "path": "/downloads",
                            "identity": {"device": 1, "inode": 2, "type": stat.S_IFDIR},
                        }
                    ],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        result.paths.files_nul.write_bytes(manifest)
        return scan_progress.mark_ready(
            result.paths.progress_root,
            result.session["scan_id"],
            now_epoch=210,
        )

    def test_prepare_creates_private_building_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"

            result = self.prepare(root)

            self.assertEqual(result.action, "NEW")
            self.assertEqual(result.session["state"], "BUILDING")
            self.assertEqual(result.session["cutoff_epoch"], 100)
            self.assertEqual(result.session["reference_epoch"], 50)
            self.assertEqual(
                {entry.name for entry in result.paths.session_dir.iterdir()},
                set(scan_progress.KNOWN_FILE_NAMES),
            )
            self.assertEqual(stat.S_IMODE(result.paths.session_dir.stat().st_mode), 0o700)
            for path in (
                result.paths.session_json,
                result.paths.roots_json,
                result.paths.files_nul,
                result.paths.completed_jsonl,
            ):
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_only_ready_compatible_session_resumes_and_preserves_epochs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            created = self.prepare(root)
            ready = self.populate_and_mark_ready(created)

            resumed = self.prepare(
                root,
                cutoff_epoch=999,
                reference_epoch=998,
                now_epoch=300,
            )

            self.assertEqual(resumed.action, "RESUME")
            self.assertEqual(resumed.session["scan_id"], ready.session["scan_id"])
            self.assertEqual(resumed.session["cutoff_epoch"], 100)
            self.assertEqual(resumed.session["reference_epoch"], 50)
            self.assertEqual(resumed.session["resume_count"], 1)
            self.assertEqual(resumed.paths.files_nul.read_bytes(), b"/downloads/a\0")

    def test_building_session_is_replaced_after_interruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            first = self.prepare(root)
            first.paths.files_nul.write_bytes(b"incomplete")

            replacement = self.prepare(root, now_epoch=201)

            self.assertEqual(replacement.action, "NEW")
            self.assertNotEqual(replacement.session["scan_id"], first.session["scan_id"])
            self.assertEqual(replacement.paths.files_nul.read_bytes(), b"")

    def test_incompatible_or_expired_ready_session_is_replaced(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            first = self.populate_and_mark_ready(self.prepare(root))

            incompatible = self.prepare(root, fingerprint="sha256:two", now_epoch=300)
            self.assertEqual(incompatible.action, "NEW")
            self.assertNotEqual(incompatible.session["scan_id"], first.session["scan_id"])
            second = self.populate_and_mark_ready(incompatible)

            expired = self.prepare(root, now_epoch=5000, max_age_seconds=10)
            self.assertEqual(expired.action, "NEW")
            self.assertNotEqual(expired.session["scan_id"], second.session["scan_id"])

    def test_zero_max_age_keeps_ready_session_without_expiry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            first = self.prepare(root, max_age_seconds=0)
            self.populate_and_mark_ready(first)

            resumed = self.prepare(root, max_age_seconds=0, now_epoch=10_000_000)

            self.assertEqual(resumed.action, "RESUME")
            self.assertEqual(resumed.session["scan_id"], first.session["scan_id"])

    def test_truncated_final_completion_record_is_discarded_on_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            ready = self.populate_and_mark_ready(self.prepare(root))
            scan_id = ready.session["scan_id"]
            valid = json.dumps({"scan_id": scan_id, "sequence": 1, "status": "CLEAN"}).encode()
            ready.paths.completed_jsonl.write_bytes(valid + b"\n" + b'{"scan_id":"truncated')

            resumed = self.prepare(root, now_epoch=300)

            self.assertEqual(resumed.action, "RESUME")
            self.assertEqual(resumed.completed_records, 1)
            self.assertEqual(resumed.paths.completed_jsonl.read_bytes(), valid + b"\n")

    def test_status_repairs_only_truncated_final_completion_record(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            ready = self.populate_and_mark_ready(self.prepare(root))
            scan_id = ready.session["scan_id"]
            valid = json.dumps({"scan_id": scan_id, "status": "CLEAN"}).encode()
            ready.paths.completed_jsonl.write_bytes(valid + b"\n" + b'{"scan_id":"torn')

            status = scan_progress.status_session(root)

            self.assertIsNotNone(status)
            self.assertEqual(status.completed_records, 1)
            self.assertEqual(status.paths.completed_jsonl.read_bytes(), valid + b"\n")

            status.paths.completed_jsonl.write_bytes(valid + b"\n" + b"not-json\n")
            with self.assertRaisesRegex(scan_progress.ProgressStateError, "invalid JSON"):
                scan_progress.status_session(root)

    def test_complete_invalid_journal_record_resets_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            ready = self.populate_and_mark_ready(self.prepare(root))
            ready.paths.completed_jsonl.write_bytes(b"not-json\n")

            replacement = self.prepare(root, now_epoch=300)

            self.assertEqual(replacement.action, "NEW")
            self.assertNotEqual(replacement.session["scan_id"], ready.session["scan_id"])

    def test_completion_record_without_exact_scan_id_resets_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            ready = self.populate_and_mark_ready(self.prepare(root))
            ready.paths.completed_jsonl.write_text(
                json.dumps({"schema_version": 2, "scan_id": None, "status": "CLEAN"}) + "\n",
                encoding="utf-8",
            )

            replacement = self.prepare(root, now_epoch=300)

            self.assertEqual(replacement.action, "NEW")
            self.assertNotEqual(replacement.session["scan_id"], ready.session["scan_id"])

    def test_nul_manifest_preserves_newlines_and_non_utf8_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            manifest = b"/downloads/line\nbreak\0/downloads/non-utf8-\xff\0"
            ready = self.populate_and_mark_ready(self.prepare(root), manifest=manifest)

            status = scan_progress.status_session(root)

            self.assertIsNotNone(status)
            self.assertEqual(ready.paths.files_nul.read_bytes(), manifest)
            self.assertEqual(status.session["state"], "READY")

    def test_mark_ready_rejects_unterminated_nul_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            created = self.prepare(root)
            created.paths.files_nul.write_bytes(b"/downloads/incomplete")

            with self.assertRaisesRegex(scan_progress.ProgressStateError, "terminator"):
                scan_progress.mark_ready(root, created.session["scan_id"], now_epoch=210)

    def test_clear_refuses_mismatched_scan_id_then_removes_exact_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            created = self.prepare(root)

            with self.assertRaisesRegex(scan_progress.ProgressStateError, "does not match"):
                scan_progress.clear_session(root, "0" * 32)
            self.assertTrue(created.paths.session_dir.is_dir())

            scan_progress.clear_session(root, created.session["scan_id"])
            self.assertFalse(created.paths.session_dir.exists())
            self.assertTrue(root.is_dir())

    def test_clear_refuses_unexpected_entry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            created = self.prepare(root)
            (created.paths.session_dir / "unexpected").write_text("do not delete", encoding="ascii")

            with self.assertRaisesRegex(scan_progress.ProgressStateError, "unexpected entries"):
                scan_progress.clear_session(root, created.session["scan_id"])
            self.assertTrue((created.paths.session_dir / "unexpected").exists())

    def test_clear_uses_expected_id_but_does_not_follow_corrupt_ancillary_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            created = self.prepare(root)
            outside = Path(temporary) / "outside"
            outside.write_text("preserve", encoding="ascii")
            created.paths.files_nul.unlink()
            created.paths.files_nul.symlink_to(outside)

            scan_progress.clear_session(root, created.session["scan_id"])

            self.assertEqual(outside.read_text(encoding="ascii"), "preserve")
            self.assertFalse(created.paths.session_dir.exists())

    def test_ready_session_with_corrupt_root_guard_is_rebuilt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            ready = self.populate_and_mark_ready(self.prepare(root))
            ready.paths.roots_json.write_text("{}\n", encoding="ascii")

            replacement = self.prepare(root, now_epoch=300)

            self.assertEqual(replacement.action, "NEW")
            self.assertNotEqual(replacement.session["scan_id"], ready.session["scan_id"])

    def test_status_rejects_symlinked_active_directory_or_known_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "progress"
            root.mkdir()
            outside = base / "outside"
            outside.mkdir()
            (root / "active").symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(scan_progress.ProgressStateError, "real directory"):
                scan_progress.status_session(root)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "progress"
            created = self.prepare(root)
            created.paths.files_nul.unlink()
            created.paths.files_nul.symlink_to(created.paths.completed_jsonl)
            with self.assertRaisesRegex(scan_progress.ProgressStateError, "regular file"):
                scan_progress.status_session(root)

    def test_cli_emits_shell_quoted_assignments_and_none_status(self):
        script = Path(__file__).resolve().parents[1] / "scripts" / "scan_progress.py"
        with tempfile.TemporaryDirectory(prefix="progress path ") as temporary:
            root = Path(temporary) / "state"
            prepared = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "prepare",
                    "--progress-root",
                    str(root),
                    "--fingerprint",
                    "sha256:cli",
                    "--label",
                    "CHANGED",
                    "--cutoff-epoch",
                    "100",
                    "--reference-epoch",
                    "50",
                    "--request-identity",
                    "manual-changed",
                    "--request-mode",
                    "manual",
                    "--max-age-seconds",
                    "3600",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("PROGRESS_ACTION=NEW", prepared.stdout)
            self.assertIn("PROGRESS_SESSION_DIR='", prepared.stdout)

            empty_root = Path(temporary) / "empty"
            empty_root.mkdir()
            status = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "status",
                    "--progress-root",
                    str(empty_root),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                status.stdout.splitlines(),
                ["PROGRESS_ACTION=NONE", "PROGRESS_STATE=NONE"],
            )


if __name__ == "__main__":
    unittest.main()
