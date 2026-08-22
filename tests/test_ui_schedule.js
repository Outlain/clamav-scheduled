"use strict";

const assert = require("node:assert/strict");
const {
  describeScheduleFrequency,
  filtersForScheduleFrequency,
  formatOrdinal,
  inferScheduleFrequency,
} = require("../ui/app.js");

const everyWeekday = [1, 2, 3, 4, 5, 6, 7];
const everyMonth = Array.from({ length: 12 }, (_, index) => index + 1);
const everyMonthDay = Array.from({ length: 31 }, (_, index) => index + 1);

assert.equal(
  inferScheduleFrequency(
    {
      full_scan_days: [7],
      full_scan_months: everyMonth,
      full_scan_month_days: everyMonthDay,
    },
    "full",
  ),
  "weekly",
);

assert.equal(
  inferScheduleFrequency(
    {
      full_scan_days: everyWeekday,
      full_scan_months: everyMonth,
      full_scan_month_days: [3],
    },
    "full",
  ),
  "monthly",
);

assert.equal(
  inferScheduleFrequency(
    {
      full_scan_days: everyWeekday,
      full_scan_months: [1],
      full_scan_month_days: [3],
    },
    "full",
  ),
  "yearly",
);

assert.equal(
  inferScheduleFrequency(
    {
      full_scan_days: [1, 3, 5],
      full_scan_months: everyMonth,
      full_scan_month_days: everyMonthDay,
    },
    "full",
  ),
  "advanced",
);

assert.equal(
  inferScheduleFrequency(
    {
      full_scan_days: "*",
      full_scan_months: "*",
      full_scan_month_days: "*",
    },
    "full",
  ),
  "daily",
);

assert.deepEqual(filtersForScheduleFrequency("monthly", { monthDay: "3" }), {
  weekdays: everyWeekday,
  months: ["*"],
  monthDays: ["3"],
});

assert.deepEqual(filtersForScheduleFrequency("yearly", { month: "6", monthDay: "last" }), {
  weekdays: everyWeekday,
  months: [6],
  monthDays: ["last"],
});

assert.equal(formatOrdinal(3), "3rd");
assert.equal(formatOrdinal(11), "11th");
assert.equal(formatOrdinal("last"), "last day");
assert.equal(
  describeScheduleFrequency("monthly", { monthDay: "3" }, ["03:30"], "America/New_York"),
  "Every month on the 3rd at 03:30 (America/New_York).",
);
assert.equal(
  describeScheduleFrequency("weekly", { weekday: "7" }, ["03:30"], "UTC"),
  "Every Sunday at 03:30 (UTC).",
);

console.log("UI schedule mapping tests passed");
