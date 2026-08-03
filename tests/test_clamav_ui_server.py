import importlib.util
import json
import os
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
        with tempfile.TemporaryDirectory() as temp_dir:
            downloads = Path(temp_dir) / "downloads"
            archive = Path(temp_dir) / "archive"
            downloads.mkdir()
            archive.mkdir()
            payload = {
                "tz": "UTC",
                "scan_paths": [str(downloads), str(archive)],
                "exclude_paths": [str(downloads / "tmp")],
                "changed_scan_days": [1, 2, 3],
                "changed_scan_times": ["07:00", "14:00"],
                "full_scan_days": [7],
                "full_scan_times": ["03:30"],
            }

            normalized = clamav_ui_server.validate_and_normalize_config(payload)

            self.assertEqual(normalized["scan_paths"], [str(downloads.resolve()), str(archive.resolve())])
            self.assertEqual(normalized["changed_scan_times"], ["07:00", "14:00"])
            self.assertEqual(normalized["full_scan_days"], [7])

    def test_health_configuration_error_is_safe_for_one_line_output(self):
        self.assertEqual(
            clamav_ui_server.health_detail("bad value\r\nnext line"),
            "bad value  next line",
        )
        self.assertEqual(len(clamav_ui_server.health_detail("x" * 600)), 512)

    def test_repair_draft_preserves_compatible_saved_values(self):
        draft = clamav_ui_server.config_repair_draft(
            {
                "scan_paths": "/downloads:/missing",
                "maxthreads": "not-a-number",
                "changed_scan_times": ["07:00", "bad"],
                "unknown_field": "ignored",
            }
        )

        self.assertEqual(draft["scan_paths"], ["/downloads", "/missing"])
        self.assertEqual(draft["maxthreads"], "not-a-number")
        self.assertEqual(draft["changed_scan_times"], ["07:00", "bad"])
        self.assertNotIn("unknown_field", draft)

    def test_validate_and_normalize_config_rejects_invalid_time(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = {
                "scan_paths": [temp_dir],
                "changed_scan_days": [1],
                "changed_scan_times": ["99:00"],
                "full_scan_days": [7],
                "full_scan_times": ["03:30"],
            }

            with self.assertRaisesRegex(ValueError, "Invalid time value"):
                clamav_ui_server.validate_and_normalize_config(payload)

    def test_validate_and_normalize_config_rejects_excessive_or_mismatched_workers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_payload = {
                "scan_paths": [temp_dir],
                "changed_scan_days": [1],
                "changed_scan_times": ["07:00"],
                "full_scan_days": [7],
                "full_scan_times": ["03:30"],
            }
            with self.assertRaisesRegex(ValueError, "at most 64"):
                clamav_ui_server.validate_and_normalize_config({**base_payload, "maxthreads": 65})
            with self.assertRaisesRegex(ValueError, "must not exceed maxthreads"):
                clamav_ui_server.validate_and_normalize_config(
                    {
                        **base_payload,
                        "maxthreads": 4,
                        "full_scan_parallel_jobs": 5,
                        "changed_scan_parallel_jobs": 4,
                    }
                )

    def test_validate_and_normalize_config_rejects_scanlog_inside_scan_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scan_root = Path(temp_dir) / "downloads"
            scan_root.mkdir()
            with self.assertRaisesRegex(ValueError, "scanlog must be outside"):
                clamav_ui_server.validate_and_normalize_config(
                    {
                        "scan_paths": [str(scan_root)],
                        "scanlog": str(scan_root / "scanner.log"),
                    }
                )

    def test_validate_and_normalize_config_rejects_quarantine_parent_of_scan_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            quarantine = Path(temp_dir) / "quarantine"
            scan_root = quarantine / "downloads"
            scan_root.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "must not contain a scan root"):
                clamav_ui_server.validate_and_normalize_config(
                    {
                        "scan_paths": [str(scan_root)],
                        "quarantine_dir": str(quarantine),
                    }
                )

    def test_runtime_permission_probe_creates_required_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            scan_root = base / "downloads"
            scan_root.mkdir()
            config = clamav_ui_server.validate_and_normalize_config(
                {
                    "scan_paths": [str(scan_root)],
                    "quarantine_dir": str(scan_root / "quarantine"),
                    "scanlog": str(base / "logs" / "scanner.log"),
                }
            )

            clamav_ui_server.validate_runtime_permissions(
                config,
                base / "config",
                base / "state",
            )

            self.assertTrue((scan_root / "quarantine").is_dir())
            self.assertTrue((base / "logs" / "scanner.log").is_file())

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

        self.assertEqual(serialized["FORCE_FULL_FLAG"], "/state/force_full_scan.flag")

    def test_validate_manual_request_paths_rejects_paths_outside_scan_roots(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            downloads = base / "downloads"
            archive = base / "archive"
            downloads.mkdir()
            archive.mkdir()
            config = {"scan_paths": [str(downloads)]}

            with self.assertRaisesRegex(ValueError, "outside configured scan roots"):
                clamav_ui_server.validate_manual_request_paths(
                    config,
                    [str(archive)],
                    field_name="target_paths",
                    require_existing=True,
                )


class UISchedulerManagerTests(unittest.TestCase):
    def test_invalid_saved_config_opens_with_repair_draft(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            config_dir = base / "config"
            state_dir = base / "state"
            config_dir.mkdir()
            state_dir.mkdir()
            (config_dir / "ui-config.json").write_text(
                json.dumps(
                    {
                        "scan_paths": [str(base / "missing")],
                        "maxthreads": "bad",
                        "full_scan_times": ["04:15"],
                    }
                ),
                encoding="utf-8",
            )

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            try:
                bootstrap = manager.get_bootstrap()
            finally:
                manager.shutdown()

            self.assertTrue(bootstrap["repair_mode"])
            self.assertIn("does not exist", bootstrap["config_error"])
            self.assertEqual(bootstrap["config"]["scan_paths"], [str(base / "missing")])
            self.assertEqual(bootstrap["config"]["maxthreads"], "bad")
            self.assertEqual(bootstrap["config"]["full_scan_times"], ["04:15"])

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
            second_entry = {**first_entry, "cycle_started_at": "Mon Mar 16 01:27:20 UTC 2026"}

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
                "roots": [
                    {
                        "root": "/downloads",
                        "files": 100,
                        "processed_files": 100,
                        "bytes": "3.0 GiB",
                        "processed_bytes": "3.0 GiB",
                        "infected": 0,
                        "vanished": 0,
                        "errors": 0,
                    }
                ],
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

    def test_clamd_ready_line_marks_scheduler_idle(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_dir = temp_path / "state"
            config_dir = temp_path / "config"
            state_dir.mkdir()
            config_dir.mkdir()

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            manager._phase = "starting"
            try:
                manager._handle_log_line("clamd ready.")
            finally:
                manager.shutdown()

            self.assertEqual(manager._phase, "idle")
            self.assertEqual(manager._last_event, "clamd ready.")

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

    def test_scheduler_starts_in_a_new_process_session(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manager = clamav_ui_server.SchedulerManager(
                config_dir=temp_path / "config",
                state_dir=temp_path / "state",
            )
            manager._stop_event.set()
            manager._monitor_thread.join(timeout=2)
            manager._config = {
                **clamav_ui_server.DEFAULT_CONFIG,
                "scan_paths": [temp_dir],
                "scanlog": str(temp_path / "scan.log"),
            }
            fake_process = mock.Mock()
            fake_process.poll.return_value = None

            with mock.patch.object(clamav_ui_server.subprocess, "Popen", return_value=fake_process) as popen:
                manager._start_scheduler_locked(reset_backoff=True)

            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            manager._process = None

    def test_scheduler_stop_signals_the_complete_process_group(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manager = clamav_ui_server.SchedulerManager(
                config_dir=temp_path / "config",
                state_dir=temp_path / "state",
            )
            manager._stop_event.set()
            manager._monitor_thread.join(timeout=2)
            fake_process = mock.Mock()
            fake_process.pid = 4321
            fake_process.poll.return_value = None
            fake_process.wait.return_value = 0
            fake_process.returncode = -15
            manager._process = fake_process

            with mock.patch.object(clamav_ui_server.os, "killpg") as killpg:
                manager._stop_scheduler_locked()

            killpg.assert_called_once_with(4321, clamav_ui_server.signal.SIGTERM)
            self.assertIsNone(manager._process)

    def test_unexpected_scheduler_exit_gets_exponential_restart_delay(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manager = clamav_ui_server.SchedulerManager(
                config_dir=temp_path / "config",
                state_dir=temp_path / "state",
            )
            manager._stop_event.set()
            manager._monitor_thread.join(timeout=2)
            manager._config = {**clamav_ui_server.DEFAULT_CONFIG, "scan_paths": [temp_dir]}
            fake_process = mock.Mock()
            fake_process.poll.return_value = 7
            manager._process = fake_process
            manager._process_started_monotonic = 99.0

            with mock.patch.object(clamav_ui_server.time, "monotonic", return_value=100.0):
                manager._poll_process_locked()

            self.assertEqual(manager._phase, "restart_wait")
            self.assertEqual(manager._next_restart_monotonic, 102.0)
            self.assertIn("Restarting in 2 seconds", manager._last_event)


class PathValidationTests(unittest.TestCase):
    def test_path_line_protocol_rejects_control_characters_and_colons(self):
        for value in (
            "/downloads/good\nREQUEST_MODE=relative",
            "/downloads/trailing-newline\n",
            "/downloads/colon:name",
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    clamav_ui_server.normalize_path_entry(value, "target_paths")

    def test_list_entries_must_be_strings(self):
        with self.assertRaises(ValueError):
            clamav_ui_server.normalize_path_list(["/downloads", 7], "scan_paths", required=True)

    def test_symlink_escape_is_outside_canonical_scan_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            scan_root = base / "downloads"
            outside = base / "outside"
            scan_root.mkdir()
            outside.mkdir()
            escaped_file = outside / "escaped.txt"
            escaped_file.write_text("data", encoding="utf-8")
            (scan_root / "escape").symlink_to(outside, target_is_directory=True)
            config = {"scan_paths": [str(scan_root.resolve())]}

            with self.assertRaises(ValueError):
                clamav_ui_server.validate_manual_request_paths(
                    config,
                    [str(scan_root / "escape" / "escaped.txt")],
                    field_name="target_paths",
                    require_existing=True,
                )

    def test_canonical_in_root_target_is_accepted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scan_root = Path(temp_dir) / "downloads"
            target = scan_root / "nested" / "target.txt"
            target.parent.mkdir(parents=True)
            target.write_text("data", encoding="utf-8")
            config = {"scan_paths": [str(scan_root.resolve())]}

            paths = clamav_ui_server.validate_manual_request_paths(
                config,
                [str(scan_root / "nested" / ".." / "nested" / "target.txt")],
                field_name="target_paths",
                require_existing=True,
            )

            self.assertEqual(paths, [str(target.resolve())])


class PersistenceTests(unittest.TestCase):
    def test_atomic_writer_replaces_content_without_fixed_temp_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = Path(temp_dir) / "config.json"
            destination.write_text("old", encoding="utf-8")

            clamav_ui_server.write_json_atomic(destination, {"value": 2})

            self.assertEqual(json.loads(destination.read_text(encoding="utf-8")), {"value": 2})
            self.assertEqual(list(destination.parent.glob(".config.json.*.tmp")), [])

    def test_request_writer_rejects_embedded_newline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = Path(temp_dir) / "request.env"
            with self.assertRaises(ValueError):
                clamav_ui_server.write_key_value_file(
                    destination,
                    ["REQUEST_TARGET_PATHS=/downloads/good\nREQUEST_MODE=relative"],
                )
            self.assertFalse(destination.exists())

    def test_persisted_json_reader_rejects_oversized_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            oversized = Path(temp_dir) / "oversized.json"
            with oversized.open("wb") as handle:
                handle.truncate(clamav_ui_server.MAX_PERSISTED_JSON_BYTES + 1)
            with self.assertRaisesRegex(ValueError, "Persisted JSON exceeds"):
                clamav_ui_server.read_json(oversized)

    def test_malformed_history_does_not_prevent_ui_manager_startup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            state_dir = Path(temp_dir) / "state"
            config_dir.mkdir()
            history_path = config_dir / "ui-history.json"
            history_path.write_text("{not-json", encoding="utf-8")

            manager = clamav_ui_server.SchedulerManager(config_dir=config_dir, state_dir=state_dir)
            try:
                status = manager.get_status()
                self.assertFalse(status["configured"])
                self.assertIn("could not be loaded", status["last_warning"])
                self.assertEqual(history_path.read_text(encoding="utf-8"), "{not-json")
            finally:
                manager.shutdown()


if __name__ == "__main__":
    unittest.main()
