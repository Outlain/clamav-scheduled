import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "clamav_ui_server.py"
SPEC = importlib.util.spec_from_file_location("clamav_ui_server", MODULE_PATH)
clamav_ui_server = importlib.util.module_from_spec(SPEC)
assert SPEC is not None
assert SPEC.loader is not None
sys.modules[SPEC.name] = clamav_ui_server
SPEC.loader.exec_module(clamav_ui_server)


class UIConfigValidationTests(unittest.TestCase):
    def test_validate_and_normalize_config_accepts_valid_payload(self):
        payload = {
            "tz": "UTC",
            "scan_paths": ["/downloads", "/archive"],
            "exclude_paths": ["/downloads/tmp"],
            "changed_scan_days": [1, 2, 3],
            "changed_scan_times": ["07:00", "14:00"],
            "full_scan_days": [7],
            "full_scan_times": ["03:30"],
        }

        with mock.patch.object(clamav_ui_server.os.path, "isdir", return_value=True):
            normalized = clamav_ui_server.validate_and_normalize_config(payload)

        self.assertEqual(normalized["scan_paths"], ["/downloads", "/archive"])
        self.assertEqual(normalized["changed_scan_times"], ["07:00", "14:00"])
        self.assertEqual(normalized["full_scan_days"], [7])

    def test_validate_and_normalize_config_rejects_invalid_time(self):
        payload = {
            "scan_paths": ["/downloads"],
            "changed_scan_days": [1],
            "changed_scan_times": ["99:00"],
            "full_scan_days": [7],
            "full_scan_times": ["03:30"],
        }

        with mock.patch.object(clamav_ui_server.os.path, "isdir", return_value=True):
            with self.assertRaisesRegex(ValueError, "Invalid time value"):
                clamav_ui_server.validate_and_normalize_config(payload)

    def test_serialize_config_for_scheduler_derives_force_flag(self):
        config = dict(clamav_ui_server.DEFAULT_CONFIG)
        config.update(
            {
                "scan_paths": ["/downloads"],
                "changed_scan_days": [1],
                "changed_scan_times": ["07:00"],
                "full_scan_days": [7],
                "full_scan_times": ["03:30"],
            }
        )

        serialized = clamav_ui_server.serialize_config_for_scheduler(config)

        self.assertEqual(serialized["FORCE_FULL_FLAG"], "/downloads/.clamav_force_full_scan.flag")

    def test_validate_manual_request_paths_rejects_paths_outside_scan_roots(self):
        config = {"scan_paths": ["/downloads"]}

        with mock.patch.object(clamav_ui_server.os.path, "exists", return_value=True):
            with self.assertRaisesRegex(ValueError, "outside configured scan roots"):
                clamav_ui_server.validate_manual_request_paths(
                    config,
                    ["/archive"],
                    field_name="target_paths",
                    require_existing=True,
                )


class UISchedulerManagerTests(unittest.TestCase):
    def test_live_history_append_keeps_nearby_identical_scans_with_different_timestamps(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            first_entry = {
                "label": "FULL",
                "display_label": "Full Scan",
                "cycle_started_at": "Mon Mar 16 01:03:08 UTC 2026",
                "scheduled_files": 42922,
                "indexed_files": 42922,
                "processed_files": 42922,
                "clean": 42922,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
                "quarantine_failures": 0,
                "bytes": "5.47 TiB",
                "elapsed": "23h 46m 2s",
                "avg_throughput": "0.50 files/s",
                "avg_data_rate": "66.90 MiB/s",
                "roots": [],
            }
            second_entry = {
                **first_entry,
                "cycle_started_at": "Mon Mar 16 01:27:20 UTC 2026",
            }

            try:
                manager._append_history_locked(first_entry)
                manager._append_history_locked(second_entry)
            finally:
                manager.shutdown()

            self.assertEqual(len(manager._history), 2)

    def test_manager_dedupes_nearby_history_entries_with_different_timestamps(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            base_entry = {
                "label": "FULL",
                "display_label": "Full Scan",
                "scheduled_files": 42922,
                "indexed_files": 42922,
                "processed_files": 42922,
                "clean": 42922,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
                "quarantine_failures": 0,
                "bytes": "5.47 TiB",
                "elapsed": "23h 46m 2s",
                "avg_throughput": "0.50 files/s",
                "avg_data_rate": "66.90 MiB/s",
                "roots": [],
            }
            entry_a = {**base_entry, "cycle_started_at": "Mon Mar 16 01:03:08 UTC 2026"}
            entry_b = {**base_entry, "cycle_started_at": "2026-03-16T01:27:20Z"}
            entry_c = {**base_entry, "cycle_started_at": "2026-03-16T01:38:18Z"}
            clamav_ui_server.write_json_atomic(config_dir / "ui-history.json", [entry_a, entry_b, entry_c])

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            try:
                history = clamav_ui_server.read_json(config_dir / "ui-history.json", default=[])
            finally:
                manager.shutdown()

            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["cycle_started_at"], "Mon Mar 16 01:03:08 UTC 2026")

    def test_manager_keeps_identical_history_entries_when_far_apart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            base_entry = {
                "label": "FULL",
                "display_label": "Full Scan",
                "scheduled_files": 42922,
                "indexed_files": 42922,
                "processed_files": 42922,
                "clean": 42922,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
                "quarantine_failures": 0,
                "bytes": "5.47 TiB",
                "elapsed": "23h 46m 2s",
                "avg_throughput": "0.50 files/s",
                "avg_data_rate": "66.90 MiB/s",
                "roots": [],
            }
            entry_a = {**base_entry, "cycle_started_at": "Sat Mar 14 23:54:09 UTC 2026"}
            entry_b = {**base_entry, "cycle_started_at": "Mon Mar 16 01:03:08 UTC 2026"}
            clamav_ui_server.write_json_atomic(config_dir / "ui-history.json", [entry_a, entry_b])

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            try:
                history = clamav_ui_server.read_json(config_dir / "ui-history.json", default=[])
            finally:
                manager.shutdown()

            self.assertEqual(len(history), 2)

    def test_manager_keeps_nearby_identical_history_entries_when_both_have_traces(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            base_entry = {
                "label": "FULL",
                "display_label": "Full Scan",
                "scheduled_files": 42922,
                "indexed_files": 42922,
                "processed_files": 42922,
                "clean": 42922,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
                "quarantine_failures": 0,
                "bytes": "5.47 TiB",
                "elapsed": "23h 46m 2s",
                "avg_throughput": "0.50 files/s",
                "avg_data_rate": "66.90 MiB/s",
                "progress_trace": [
                    {
                        "percent": 50,
                        "processed_files": 21461,
                        "total_files": 42922,
                        "elapsed_seconds": 100.0,
                        "avg_throughput_files_per_sec": 1.0,
                        "window_throughput_files_per_sec": 1.1,
                        "avg_data_rate_mib_per_sec": 50.0,
                        "window_data_rate_mib_per_sec": 52.0,
                    }
                ],
                "roots": [],
            }
            entry_a = {**base_entry, "cycle_started_at": "Mon Mar 16 01:03:08 UTC 2026"}
            entry_b = {**base_entry, "cycle_started_at": "Mon Mar 16 01:27:20 UTC 2026"}
            clamav_ui_server.write_json_atomic(config_dir / "ui-history.json", [entry_a, entry_b])

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            try:
                history = clamav_ui_server.read_json(config_dir / "ui-history.json", default=[])
            finally:
                manager.shutdown()

            self.assertEqual(len(history), 2)

    def test_manager_dedupes_existing_history_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            duplicate_entry = {
                "label": "FULL",
                "display_label": "Full Scan",
                "cycle_started_at": "Sun Mar 15 08:30:00 UTC 2026",
                "scheduled_files": 100,
                "indexed_files": 100,
                "processed_files": 100,
                "clean": 100,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
                "quarantine_failures": 0,
                "bytes": "3.0 GiB",
                "elapsed": "1m",
                "avg_throughput": "10 files/s",
                "avg_data_rate": "100 MiB/s",
                "roots": [{"root": "/downloads", "files": 100, "processed_files": 100, "bytes": "3.0 GiB", "processed_bytes": "3.0 GiB", "infected": 0, "vanished": 0, "errors": 0}],
            }
            clamav_ui_server.write_json_atomic(config_dir / "ui-history.json", [duplicate_entry, duplicate_entry])

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            try:
                history = clamav_ui_server.read_json(config_dir / "ui-history.json", default=[])
            finally:
                manager.shutdown()

            self.assertEqual(len(history), 1)

    def test_log_replay_does_not_duplicate_history_summaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            scanlog = temp_path / "clamav.log"
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            existing_entry = {
                "label": "FULL",
                "display_label": "Full Scan",
                "cycle_started_at": "Sun Mar 15 08:30:00 UTC 2026",
                "scheduled_files": 100,
                "indexed_files": 100,
                "processed_files": 100,
                "clean": 100,
                "infected": 0,
                "vanished": 0,
                "errors": 0,
                "quarantine_failures": 0,
                "bytes": "3.0 GiB",
                "elapsed": "1m",
                "avg_throughput": "10 files/s",
                "avg_data_rate": "100 MiB/s",
                "roots": [],
            }
            clamav_ui_server.write_json_atomic(config_dir / "ui-history.json", [existing_entry])
            scanlog.write_text(
                "\n".join(
                    [
                        "=== Sun Mar 15 08:30:00 UTC 2026 Scan cycle starting (full_due=1 changed_due=0) ===",
                        "[FULL] Summary: scheduled_files=100 indexed_files=100 processed_files=100 clean=100 infected=0 vanished=0 errors=0 quarantine_failures=0 bytes=3.0 GiB elapsed=1m avg_throughput=10 files/s avg_data_rate=100 MiB/s",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            manager._log_path = scanlog

            try:
                manager._replay_existing_log()
            finally:
                manager.shutdown()

            history = clamav_ui_server.read_json(config_dir / "ui-history.json", default=[])
            self.assertEqual(len(history), 1)

    def test_no_scans_line_clears_stale_current_scan(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            manager._current_scan = {
                "label": "FULL",
                "display_label": "Full Scan",
                "percent": 31,
            }

            try:
                manager._handle_log_line(
                    "=== Mon Mar 16 12:00:00 UTC 2026 No scans due. Next wake at Mon Mar 16 13:00:00 UTC 2026 ==="
                )
            finally:
                manager.shutdown()

            self.assertEqual(manager._phase, "idle")
            self.assertEqual(manager._next_wake, "Mon Mar 16 13:00:00 UTC 2026")
            self.assertIsNone(manager._current_scan)

    def test_log_replay_does_not_restore_historical_in_progress_scan(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            scanlog = temp_path / "clamav.log"
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()
            scanlog.write_text(
                "\n".join(
                    [
                        "=== FULL SCAN starting ===",
                        "[FULL] Scanning 100 files with persistent_session_workers=8",
                        "[FULL] Progress: 31% (31/100) bytes=1.0 GiB/3.0 GiB clean=31 infected=0 vanished=0 errors=0 elapsed=1m avg_throughput=10 files/s window_throughput=9 files/s avg_data_rate=100 MiB/s window_data_rate=95 MiB/s",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            manager._log_path = scanlog
            manager._phase = "starting"

            try:
                manager._replay_existing_log()
            finally:
                manager.shutdown()

            self.assertIsNone(manager._current_scan)
            self.assertNotEqual(manager._phase, "scanning")

    def test_queue_manual_full_scan_writes_request_file_with_ignore_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            scan_root = temp_path / "downloads"
            target_dir = scan_root / "projects"
            ignore_dir = scan_root / "cache"
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            target_dir.mkdir(parents=True)
            state_dir.mkdir()
            config_dir.mkdir()

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            manager._config = {
                **clamav_ui_server.DEFAULT_CONFIG,
                "scan_paths": [str(scan_root)],
                "changed_scan_days": [1],
                "changed_scan_times": ["07:00"],
                "full_scan_days": [7],
                "full_scan_times": ["03:30"],
            }
            manager._config_error = ""

            try:
                with mock.patch.object(clamav_ui_server.time, "time", return_value=1_700_000_000):
                    status = manager.queue_manual_full_scan(
                        {
                            "target_paths": [str(target_dir)],
                            "ignore_paths": [str(ignore_dir)],
                        }
                    )
            finally:
                manager.shutdown()

            request_text = (state_dir / "manual_full_scan_request.env").read_text(encoding="utf-8")
            self.assertIn(f"REQUEST_TARGET_PATHS={target_dir}", request_text)
            self.assertIn(f"REQUEST_IGNORE_PATHS={ignore_dir}", request_text)
            self.assertEqual(status["pending_manual_full_request"]["target_paths"], [str(target_dir)])
            self.assertEqual(status["pending_manual_full_request"]["ignore_paths"], [str(ignore_dir)])

    def test_queue_manual_changed_scan_writes_relative_request_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            scan_root = temp_path / "downloads"
            target_dir = scan_root / "projects"
            ignore_dir = scan_root / "cache"
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            target_dir.mkdir(parents=True)
            state_dir.mkdir()
            config_dir.mkdir()

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            manager._config = {
                **clamav_ui_server.DEFAULT_CONFIG,
                "scan_paths": [str(scan_root)],
                "changed_scan_days": [1],
                "changed_scan_times": ["07:00"],
                "full_scan_days": [7],
                "full_scan_times": ["03:30"],
            }
            manager._config_error = ""

            try:
                with mock.patch.object(clamav_ui_server.time, "time", return_value=1_700_000_000):
                    status = manager.queue_manual_changed_scan(
                        {
                            "mode": "relative",
                            "lookback_seconds": 7200,
                            "target_paths": [str(target_dir)],
                            "ignore_paths": [str(ignore_dir)],
                        }
                    )
            finally:
                manager.shutdown()

            request_text = (state_dir / "manual_changed_scan_request.env").read_text(encoding="utf-8")
            self.assertIn("REQUEST_MODE=relative", request_text)
            self.assertIn("REQUEST_REFERENCE_EPOCH=1699992800", request_text)
            self.assertIn(f"REQUEST_TARGET_PATHS={target_dir}", request_text)
            self.assertIn(f"REQUEST_IGNORE_PATHS={ignore_dir}", request_text)
            self.assertEqual(status["pending_manual_changed_request"]["lookback_seconds"], 7200)
            self.assertEqual(status["pending_manual_changed_request"]["ignore_paths"], [str(ignore_dir)])

    def test_queue_manual_changed_scan_uses_last_changed_epoch_for_since_last(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            scan_root = temp_path / "downloads"
            scan_root.mkdir(parents=True)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()
            (state_dir / "last_changed_scan_epoch").write_text("12345\n", encoding="utf-8")

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            manager._config = {
                **clamav_ui_server.DEFAULT_CONFIG,
                "scan_paths": [str(scan_root)],
                "changed_scan_days": [1],
                "changed_scan_times": ["07:00"],
                "full_scan_days": [7],
                "full_scan_times": ["03:30"],
            }
            manager._config_error = ""

            try:
                status = manager.queue_manual_changed_scan({"mode": "since_last", "target_paths": []})
            finally:
                manager.shutdown()

            self.assertEqual(status["pending_manual_changed_request"]["reference_epoch"], 12345)

    def test_progress_trace_is_saved_with_completed_scan_history(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)

            try:
                manager._handle_log_line(
                    "=== Mon Mar 16 01:03:08 UTC 2026 Scan cycle starting (full_due=1 changed_due=0) ==="
                )
                manager._handle_log_line("=== FULL SCAN starting ===")
                manager._handle_log_line("[FULL] Scanning 100 files with persistent_session_workers=8")
                manager._handle_log_line(
                    "[FULL] Progress: 25% (25/100) bytes=1.0 GiB/4.0 GiB clean=25 infected=0 vanished=0 errors=0 "
                    "elapsed=30s avg_throughput=0.83 files/s window_throughput=0.83 files/s "
                    "avg_data_rate=34.13 MiB/s window_data_rate=34.13 MiB/s"
                )
                manager._handle_log_line(
                    "[FULL] Progress: 50% (50/100) bytes=2.5 GiB/4.0 GiB clean=50 infected=0 vanished=0 errors=0 "
                    "elapsed=1m 10s avg_throughput=0.71 files/s window_throughput=0.56 files/s "
                    "avg_data_rate=36.57 MiB/s window_data_rate=39.90 MiB/s"
                )
                manager._handle_log_line(
                    "[FULL] Summary: scheduled_files=100 indexed_files=100 processed_files=100 clean=100 infected=0 "
                    "vanished=0 errors=0 quarantine_failures=0 bytes=4.0 GiB elapsed=2m 0s "
                    "avg_throughput=0.83 files/s avg_data_rate=34.13 MiB/s"
                )
            finally:
                manager.shutdown()

            history = clamav_ui_server.read_json(config_dir / "ui-history.json", default=[])
            self.assertEqual(len(history), 1)
            trace = history[0]["progress_trace"]
            self.assertEqual(len(trace), 2)
            self.assertEqual(trace[0]["percent"], 25)
            self.assertAlmostEqual(trace[1]["elapsed_seconds"], 70.0)
            self.assertAlmostEqual(trace[1]["window_throughput_files_per_sec"], 0.56)
            self.assertAlmostEqual(trace[1]["window_data_rate_mib_per_sec"], 39.90)


if __name__ == "__main__":
    unittest.main()
