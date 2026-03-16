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


if __name__ == "__main__":
    unittest.main()
