#!/usr/bin/env python3

from __future__ import annotations

import argparse
import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Sequence
from zoneinfo import ZoneInfo


DAY_NAME_TO_NUMBER = {
    "1": 1,
    "mon": 1,
    "monday": 1,
    "2": 2,
    "tue": 2,
    "tues": 2,
    "tuesday": 2,
    "3": 3,
    "wed": 3,
    "weds": 3,
    "wednesday": 3,
    "4": 4,
    "thu": 4,
    "thur": 4,
    "thurs": 4,
    "thursday": 4,
    "5": 5,
    "fri": 5,
    "friday": 5,
    "6": 6,
    "sat": 6,
    "saturday": 6,
    "7": 7,
    "sun": 7,
    "sunday": 7,
}
MAX_CALENDAR_SEARCH_DAYS = 146_097


@dataclass(frozen=True)
class CalendarSchedule:
    weekdays: tuple[int, ...]
    months: tuple[int, ...]
    month_days: tuple[int | str, ...]
    times: tuple[str, ...]


def _tokens(value: Any, field_name: str) -> list[str]:
    if value is None or value == "":
        raise ValueError(f"{field_name} must include at least one value.")
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple)):
        values = value
    else:
        raise ValueError(f"{field_name} must be a comma-separated string or list.")
    tokens = [str(item).strip().lower() for item in values if str(item).strip()]
    if not tokens:
        raise ValueError(f"{field_name} must include at least one value.")
    return tokens


def normalize_weekdays(value: Any, field_name: str) -> list[int]:
    tokens = _tokens(value, field_name)
    if "*" in tokens:
        if len(tokens) != 1:
            raise ValueError(f"{field_name} cannot combine '*' with other values.")
        return list(range(1, 8))
    normalized: set[int] = set()
    for token in tokens:
        try:
            normalized.add(DAY_NAME_TO_NUMBER[token])
        except KeyError:
            raise ValueError(f"Invalid weekday value in {field_name}: {token}") from None
    return sorted(normalized)


def _normalize_number_filter(
    value: Any,
    field_name: str,
    minimum: int,
    maximum: int,
) -> list[int]:
    tokens = _tokens(value, field_name)
    if "*" in tokens:
        if len(tokens) != 1:
            raise ValueError(f"{field_name} cannot combine '*' with other values.")
        return list(range(minimum, maximum + 1))
    normalized: set[int] = set()
    for token in tokens:
        if not token.isascii() or not token.isdigit():
            raise ValueError(
                f"Invalid value in {field_name}: {token}; expected {minimum}-{maximum} or *."
            )
        number = int(token)
        if not minimum <= number <= maximum:
            raise ValueError(
                f"Invalid value in {field_name}: {token}; expected {minimum}-{maximum} or *."
            )
        normalized.add(number)
    return sorted(normalized)


def normalize_months(value: Any, field_name: str) -> list[int]:
    return _normalize_number_filter(value, field_name, 1, 12)


def normalize_month_days(value: Any, field_name: str) -> list[int | str]:
    tokens = _tokens(value, field_name)
    if "*" in tokens:
        if len(tokens) != 1:
            raise ValueError(f"{field_name} cannot combine '*' with other values.")
        return list(range(1, 32))
    include_last = False
    normalized: set[int] = set()
    for token in tokens:
        if token == "last":
            include_last = True
            continue
        if not token.isascii() or not token.isdigit() or not 1 <= int(token) <= 31:
            raise ValueError(
                f"Invalid value in {field_name}: {token}; expected 1-31, last, or *."
            )
        normalized.add(int(token))
    result: list[int | str] = sorted(normalized)
    if include_last:
        result.append("last")
    return result


def normalize_times(value: Any, field_name: str) -> list[str]:
    tokens = _tokens(value, field_name)
    normalized: set[str] = set()
    for token in tokens:
        parts = token.split(":")
        if (
            len(parts) != 2
            or len(parts[0]) != 2
            or len(parts[1]) != 2
            or not all(part.isascii() and part.isdigit() for part in parts)
            or not 0 <= int(parts[0]) <= 23
            or not 0 <= int(parts[1]) <= 59
        ):
            raise ValueError(f"Invalid time value in {field_name}: {token}")
        normalized.add(token)
    return sorted(normalized)


def normalize_schedule(
    weekdays: Any,
    months: Any,
    month_days: Any,
    times: Any,
    *,
    prefix: str = "schedule",
) -> CalendarSchedule:
    schedule = CalendarSchedule(
        weekdays=tuple(normalize_weekdays(weekdays, f"{prefix}_days")),
        months=tuple(normalize_months(months, f"{prefix}_months")),
        month_days=tuple(normalize_month_days(month_days, f"{prefix}_month_days")),
        times=tuple(normalize_times(times, f"{prefix}_times")),
    )
    _validate_possible_date(schedule, prefix)
    return schedule


def _date_matches(schedule: CalendarSchedule, candidate: date) -> bool:
    if candidate.isoweekday() not in schedule.weekdays or candidate.month not in schedule.months:
        return False
    last_day = calendar.monthrange(candidate.year, candidate.month)[1]
    return candidate.day in schedule.month_days or (
        "last" in schedule.month_days and candidate.day == last_day
    )


def _validate_possible_date(schedule: CalendarSchedule, prefix: str) -> None:
    for year in range(2000, 2400):
        for month in schedule.months:
            last_day = calendar.monthrange(year, month)[1]
            for day in range(1, last_day + 1):
                if _date_matches(schedule, date(year, month, day)):
                    return
    raise ValueError(
        f"{prefix} calendar filters never produce a valid date; check weekdays, months, and month days."
    )


def _candidate_epochs(schedule: CalendarSchedule, candidate_date: date, timezone: ZoneInfo) -> list[int]:
    epochs: list[int] = []
    for time_value in schedule.times:
        hour, minute = (int(part) for part in time_value.split(":"))
        candidate = datetime(
            candidate_date.year,
            candidate_date.month,
            candidate_date.day,
            hour,
            minute,
            tzinfo=timezone,
        )
        epoch = int(candidate.timestamp())
        round_trip = datetime.fromtimestamp(epoch, timezone)
        if (
            round_trip.year,
            round_trip.month,
            round_trip.day,
            round_trip.hour,
            round_trip.minute,
        ) == (
            candidate.year,
            candidate.month,
            candidate.day,
            candidate.hour,
            candidate.minute,
        ):
            epochs.append(epoch)
    return sorted(set(epochs))


def calculate_slots(
    schedule: CalendarSchedule,
    reference_epoch: int,
    timezone_name: str,
) -> tuple[int, int]:
    try:
        timezone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError(f"Invalid timezone: {timezone_name}") from exc
    reference_date = datetime.fromtimestamp(reference_epoch, timezone).date()
    last_epoch: int | None = None
    next_epoch: int | None = None

    for offset in range(MAX_CALENDAR_SEARCH_DAYS + 1):
        if last_epoch is None:
            candidate_date = reference_date - timedelta(days=offset)
            if _date_matches(schedule, candidate_date):
                eligible = [
                    epoch
                    for epoch in _candidate_epochs(schedule, candidate_date, timezone)
                    if epoch <= reference_epoch
                ]
                if eligible:
                    last_epoch = max(eligible)
        if next_epoch is None:
            candidate_date = reference_date + timedelta(days=offset)
            if _date_matches(schedule, candidate_date):
                eligible = [
                    epoch
                    for epoch in _candidate_epochs(schedule, candidate_date, timezone)
                    if epoch > reference_epoch
                ]
                if eligible:
                    next_epoch = min(eligible)
        if last_epoch is not None and next_epoch is not None:
            return last_epoch, next_epoch

    raise ValueError("No previous and next schedule slots were found within a complete calendar cycle.")


def _add_schedule_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--weekdays", required=True)
    parser.add_argument("--months", required=True)
    parser.add_argument("--month-days", required=True)
    parser.add_argument("--times", required=True)
    parser.add_argument("--prefix", default="schedule")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and calculate calendar scan slots")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    _add_schedule_arguments(validate_parser)
    slots_parser = subparsers.add_parser("slots")
    _add_schedule_arguments(slots_parser)
    slots_parser.add_argument("--reference", required=True, type=int)
    slots_parser.add_argument("--timezone", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        schedule = normalize_schedule(
            args.weekdays,
            args.months,
            args.month_days,
            args.times,
            prefix=args.prefix,
        )
        if args.command == "slots":
            last_epoch, next_epoch = calculate_slots(schedule, args.reference, args.timezone)
            print(f"{last_epoch} {next_epoch}")
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
