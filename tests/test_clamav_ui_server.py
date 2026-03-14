import importlib.util
import sys
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


if __name__ == "__main__":
    unittest.main()
