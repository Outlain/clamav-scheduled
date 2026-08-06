#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPOSITORY_DIR=$(dirname -- "$SCRIPT_DIR")
SCHEDULER_SCRIPT="$REPOSITORY_DIR/scripts/clamav_scheduled.sh"

TEST_ROOT=$(mktemp -d)
trap 'rm -rf -- "$TEST_ROOT"' EXIT HUP INT TERM

FUNCTION_SOURCE=$(sed -n '/^append_filtered_scan_list()/,/^build_scan_list()/p' "$SCHEDULER_SCRIPT" | sed '$d')
eval "$FUNCTION_SOURCE"

append_filtered_scan_list() {
  python3 "$REPOSITORY_DIR/scripts/scan_list_filter.py" \
    --input "$1" \
    --output "$2" \
    --exclude-paths "$EXCLUDE_PATHS" \
    --ignore-paths "$3" \
    --quarantine-path "$QUARANTINE_DIR"
}

mkdir -p "$TEST_ROOT/root" "$TEST_ROOT/quarantine" "$TEST_ROOT/work"
: > "$TEST_ROOT/root/one.bin"
: > "$TEST_ROOT/root/two.bin"

SCANLOG="$TEST_ROOT/scan.log"
TMP_DIR="$TEST_ROOT/work"
EXCLUDE_PATHS=""
QUARANTINE_DIR="$TEST_ROOT/quarantine"
PATH_ENUMERATION_TIMEOUT=5
ENUMERATION_HELPER="$REPOSITORY_DIR/scripts/enumerate_scan_files.py"

: > "$TEST_ROOT/list.nul"
append_scan_path_list FULL "$TEST_ROOT/root" "$TEST_ROOT/list.nul" 0 ""
test "$APPENDED_FILE_COUNT" -eq 2
grep -q 'eligible_files=2' "$SCANLOG"

TIMEOUT_HELPER="$TEST_ROOT/timeout-helper.py"
printf '%s\n' 'raise SystemExit(124)' > "$TIMEOUT_HELPER"
ENUMERATION_HELPER="$TIMEOUT_HELPER"

: > "$TEST_ROOT/timeout-list.nul"
if append_scan_path_list FULL "$TEST_ROOT/root" "$TEST_ROOT/timeout-list.nul" 0 ""; then
  echo "Enumeration unexpectedly succeeded after the timeout stub returned 124." >&2
  exit 1
else
  ENUMERATION_RESULT=$?
fi

test "$ENUMERATION_RESULT" -eq 2
grep -q 'Timed out after 5s' "$SCANLOG"
if grep -q 'exit=0' "$SCANLOG"; then
  echo "Enumeration failure was incorrectly reported as exit=0." >&2
  exit 1
fi
