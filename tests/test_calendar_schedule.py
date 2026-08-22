import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "calendar_schedule.py"
SPEC = importlib.util.spec_from_file_location("calendar_schedule", MODULE_PATH)
calendar_schedule = importlib.util.module_from_spec(SPEC)
assert SPEC is not None
assert SPEC.loader is not None
sys.modules[SPEC.name] = calendar_schedule
SPEC.loader.exec_module(calendar_schedule)


def epoch(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp())


class CalendarScheduleTests(unittest.TestCase):
    def test_existing_weekly_schedule_keeps_weekly_slots(self):
        schedule = calendar_schedule.normalize_schedule(
            "sun", "*", "*", "03:30", prefix="full_scan"
        )

        previous, following = calendar_schedule.calculate_slots(
            schedule, epoch(2026, 8, 21, 12), "UTC"
        )

        self.assertEqual(previous, epoch(2026, 8, 16, 3, 30))
        self.assertEqual(following, epoch(2026, 8, 23, 3, 30))

    def test_monthly_schedule_uses_first_day_of_each_month(self):
        schedule = calendar_schedule.normalize_schedule(
            "*", "*", "1", "03:30", prefix="full_scan"
        )

        previous, following = calendar_schedule.calculate_slots(
            schedule, epoch(2026, 8, 21, 12), "UTC"
        )

        self.assertEqual(previous, epoch(2026, 8, 1, 3, 30))
        self.assertEqual(following, epoch(2026, 9, 1, 3, 30))

    def test_yearly_schedule_uses_selected_month_and_day(self):
        schedule = calendar_schedule.normalize_schedule(
            "*", "1", "1", "03:30", prefix="full_scan"
        )

        previous, following = calendar_schedule.calculate_slots(
            schedule, epoch(2026, 8, 21, 12), "UTC"
        )

        self.assertEqual(previous, epoch(2026, 1, 1, 3, 30))
        self.assertEqual(following, epoch(2027, 1, 1, 3, 30))

    def test_last_day_handles_leap_year(self):
        schedule = calendar_schedule.normalize_schedule(
            "*", "*", "last", "22:00", prefix="full_scan"
        )

        previous, following = calendar_schedule.calculate_slots(
            schedule, epoch(2024, 2, 15, 12), "UTC"
        )

        self.assertEqual(previous, epoch(2024, 1, 31, 22))
        self.assertEqual(following, epoch(2024, 2, 29, 22))

    def test_impossible_calendar_filter_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "never produce a valid date"):
            calendar_schedule.normalize_schedule(
                "*", "2", "30", "03:30", prefix="full_scan"
            )

    def test_wildcard_cannot_be_combined_with_other_values(self):
        with self.assertRaisesRegex(ValueError, "cannot combine"):
            calendar_schedule.normalize_schedule(
                "*", "*,1", "1", "03:30", prefix="full_scan"
            )


if __name__ == "__main__":
    unittest.main()
