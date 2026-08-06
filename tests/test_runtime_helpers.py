import importlib.util
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


def load_script_module(name: str):
    module_path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


scan_list_filter = load_script_module("scan_list_filter")
clamav_healthcheck = load_script_module("clamav_healthcheck")
scan_root_guard = load_script_module("scan_root_guard")
checkpoint_state = load_script_module("checkpoint_state")
event_writer = load_script_module("event_writer")
enumerate_scan_files = load_script_module("enumerate_scan_files")


class ScanListFilterTests(unittest.TestCase):
    def test_filter_is_nul_safe_and_respects_path_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = os.fsencode(temp_dir)
            excluded = root + b"/excluded"
            quarantine = root + b"/quarantine[1]"
            paths = [
                root + b"/kept\nname.bin",
                excluded + b"/blocked.bin",
                root + b"/excluded-suffix/kept.bin",
                root + b"/invalid-\xff.bin",
                quarantine + b"/already-isolated.bin",
            ]
            input_path = Path(temp_dir) / "raw.nul"
            output_path = Path(temp_dir) / "filtered.nul"
            input_path.write_bytes(b"\0".join(paths) + b"\0")

            count = scan_list_filter.append_filtered_paths(
                str(input_path),
                str(output_path),
                [os.fsdecode(excluded)],
                [],
                os.fsdecode(quarantine),
            )

            self.assertEqual(count, 3)
            self.assertEqual(
                [os.fsencode(path) for path in scan_list_filter.iter_nul_paths(str(output_path))],
                [paths[0], paths[2], paths[3]],
            )

    def test_filter_rejects_unterminated_input(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / "raw.nul"
            output_path = Path(temp_dir) / "filtered.nul"
            input_path.write_bytes(b"unterminated")
            with self.assertRaises(ValueError):
                scan_list_filter.append_filtered_paths(str(input_path), str(output_path), [], [])

    def test_cli_prints_appended_file_count(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / "raw.nul"
            output_path = Path(temp_dir) / "filtered.nul"
            input_path.write_bytes(b"/downloads/one.bin\0/downloads/two.bin\0")

            with mock.patch.object(
                sys,
                "argv",
                [
                    "scan_list_filter.py",
                    "--input",
                    str(input_path),
                    "--output",
                    str(output_path),
                ],
            ), mock.patch("builtins.print") as print_mock:
                result = scan_list_filter.main()

            self.assertEqual(result, 0)
            print_mock.assert_called_once_with(2)


class EnumerationProgressTests(unittest.TestCase):
    def test_progress_counter_handles_split_nul_delimited_paths(self):
        progress = enumerate_scan_files.TraversalProgress()

        progress.feed(b"/downloads\0/downloads/first")
        progress.feed(b" file.bin\0/downloads/final.bin\0")

        self.assertEqual(progress.visited_entries, 3)
        self.assertEqual(progress.latest_path, b"/downloads/final.bin")
        self.assertEqual(progress.pending, b"")

    def test_find_command_reports_all_entries_but_lists_only_regular_files(self):
        command = enumerate_scan_files.build_find_command(
            "find",
            "/downloads",
            "/tmp/files.nul",
            "/tmp/reference",
        )

        self.assertEqual(
            command,
            [
                "find",
                "--",
                "/downloads",
                "-print0",
                "-type",
                "f",
                "(",
                "-newer",
                "/tmp/reference",
                "-o",
                "-cnewer",
                "/tmp/reference",
                ")",
                "-fprint0",
                "/tmp/files.nul",
            ],
        )

    def test_enumerator_preserves_nul_safe_regular_file_list(self):
        class CaptureLogger:
            def __init__(self):
                self.lines = []

            def emit(self, line):
                self.lines.append(line)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            regular = root / "line\nbreak.bin"
            regular.write_bytes(b"content")
            (root / "directory").mkdir()
            (root / "link").symlink_to(regular)
            output = Path(temp_dir) / "files.nul"
            logger = CaptureLogger()

            result = enumerate_scan_files.run_enumeration(
                label="FULL",
                source_path=str(root),
                output_path=output,
                reference_file=None,
                timeout_seconds=10,
                heartbeat_seconds=1,
                find_binary="find",
                logger=logger,
            )

            self.assertEqual(result, 0)
            self.assertEqual(output.read_bytes(), os.fsencode(regular) + b"\0")

    def test_changed_enumerator_uses_mtime_or_ctime_checkpoint(self):
        class CaptureLogger:
            def emit(self, _line):
                pass

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            old_file = root / "old.bin"
            old_file.write_bytes(b"old")
            time.sleep(0.02)
            reference = Path(temp_dir) / "reference"
            reference.touch()
            time.sleep(0.02)
            new_file = root / "new.bin"
            new_file.write_bytes(b"new")
            output = Path(temp_dir) / "changed.nul"

            result = enumerate_scan_files.run_enumeration(
                label="CHANGED",
                source_path=str(root),
                output_path=output,
                reference_file=str(reference),
                timeout_seconds=10,
                heartbeat_seconds=1,
                find_binary="find",
                logger=CaptureLogger(),
            )

            self.assertEqual(result, 0)
            self.assertEqual(output.read_bytes(), os.fsencode(new_file) + b"\0")

    def test_long_enumeration_emits_bounded_progress_heartbeat(self):
        class CaptureLogger:
            def __init__(self):
                self.lines = []

            def emit(self, line):
                self.lines.append(line)

        with tempfile.TemporaryDirectory() as temp_dir:
            fake_find = Path(temp_dir) / "fake-find"
            fake_find.write_text(
                "#!/bin/sh\nprintf '/downloads\\0'\nsleep 2\n",
                encoding="utf-8",
            )
            fake_find.chmod(0o700)
            output = Path(temp_dir) / "files.nul"
            logger = CaptureLogger()

            result = enumerate_scan_files.run_enumeration(
                label="FULL",
                source_path="/downloads",
                output_path=output,
                reference_file=None,
                timeout_seconds=10,
                heartbeat_seconds=1,
                find_binary=str(fake_find),
                logger=logger,
            )

            self.assertEqual(result, 0)
            self.assertTrue(
                any(
                    "Enumeration progress: visited_entries=1" in line
                    and 'latest_path="/downloads"' in line
                    for line in logger.lines
                )
            )


class DefinitionHealthTests(unittest.TestCase):
    def test_definition_status_requires_main_and_daily_and_reports_daily_age(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            definitions = Path(temp_dir)
            (definitions / "main.cvd").write_bytes(b"main")
            daily = definitions / "daily.cld"
            daily.write_bytes(b"daily")
            os.utime(daily, (900, 900))

            status = clamav_healthcheck.definition_status(temp_dir, now=1000)

            self.assertEqual(status.main_path.name, "main.cvd")
            self.assertEqual(status.daily_path.name, "daily.cld")
            self.assertEqual(status.daily_age_seconds, 100)

    def test_stale_daily_definitions_fail_strict_health(self):
        status = clamav_healthcheck.DefinitionStatus(
            main_path=Path("/defs/main.cvd"),
            daily_path=Path("/defs/daily.cvd"),
            daily_age_seconds=301,
        )
        with self.assertRaises(RuntimeError):
            clamav_healthcheck.validate_definition_age(status, 300)

    def test_incomplete_definitions_fail_readiness(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            (Path(temp_dir) / "daily.cvd").write_bytes(b"daily")
            with self.assertRaises(RuntimeError):
                clamav_healthcheck.definition_status(temp_dir)


class UIHealthTests(unittest.TestCase):
    def test_ui_health_requires_valid_ready_json(self):
        response = mock.MagicMock()
        response.status = 200
        response.read.return_value = b"{\"ok\":true,\"configured\":true}"
        response.__enter__.return_value = response

        with mock.patch.object(clamav_healthcheck.urllib.request, "urlopen", return_value=response):
            payload = clamav_healthcheck.check_ui(8080)

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["configured"])

    def test_ui_health_rejects_non_ready_payload(self):
        response = mock.MagicMock()
        response.status = 200
        response.read.return_value = b"{\"ok\":false}"
        response.__enter__.return_value = response

        with mock.patch.object(clamav_healthcheck.urllib.request, "urlopen", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "did not report a ready application"):
                clamav_healthcheck.check_ui(8080)

    def test_ui_health_reports_reason_from_error_response(self):
        error = clamav_healthcheck.urllib.error.HTTPError(
            "http://127.0.0.1:8080/healthz",
            503,
            "Service Unavailable",
            {},
            None,
        )
        error.read = mock.Mock(
            return_value=b'{"ok":false,"reason":"scanner scheduler is not running","phase":"restart_wait"}'
        )

        with mock.patch.object(clamav_healthcheck.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "scanner scheduler is not running"):
                clamav_healthcheck.check_ui(8080)


class ScanRootGuardTests(unittest.TestCase):
    def test_marker_traversal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "root"
            root.mkdir()
            with self.assertRaisesRegex(RuntimeError, "relative path"):
                scan_root_guard.capture([root], "../outside")

    def test_marker_removal_fails_post_scan_verification(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "downloads"
            root.mkdir()
            marker = root / ".mounted"
            marker.write_text("ok", encoding="ascii")
            captured = scan_root_guard.capture([root], ".mounted")
            scan_root_guard.verify(captured)
            marker.unlink()
            with self.assertRaises(OSError):
                scan_root_guard.verify(captured)


class CheckpointStateTests(unittest.TestCase):
    def test_legacy_epochs_migrate_on_first_atomic_update(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_dir = Path(temp_dir)
            (state_dir / "last_full_scan_epoch").write_text("10\n", encoding="ascii")
            (state_dir / "last_changed_scan_epoch").write_text("20\n", encoding="ascii")
            self.assertEqual(
                checkpoint_state.load_checkpoints(state_dir)["last_changed_scan_epoch"], 20
            )
            checkpoint_state.update_checkpoints(state_dir, 30, 40)
            self.assertEqual(
                checkpoint_state.load_checkpoints(state_dir),
                {"last_full_scan_epoch": 30, "last_changed_scan_epoch": 40},
            )


class EventWriterTests(unittest.TestCase):
    def test_writes_schema_v1_event_atomically(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = event_writer.emit_event(
                Path(temp_dir),
                "scan_failed",
                "warning",
                "failure",
                source_path="/downloads/file.bin",
            )
            payload = __import__("json").loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["service"], "clamav-scheduled")


if __name__ == "__main__":
    unittest.main()
