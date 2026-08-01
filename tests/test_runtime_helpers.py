import importlib.util
import os
import sys
import tempfile
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


if __name__ == "__main__":
    unittest.main()
