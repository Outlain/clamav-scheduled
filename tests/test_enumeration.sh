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
SCAN_PAUSE_FILE="$TEST_ROOT/scan.pause"

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

# A READY session may be discarded only when all current roots pass their
# health/marker checks, the saved identity differs, and a marker is configured.
RESUME_FUNCTION_SOURCE=$(sed -n '/^resume_manifest_is_safe()/,/^run_scan_list()/p' "$SCHEDULER_SCRIPT" | sed '$d')
eval "$RESUME_FUNCTION_SOURCE"

HEALTH_RESULT=0
VERIFY_RESULT=1
CLEAR_RESULT=0
CLEAR_CALL=""

check_all_scan_paths_health() {
  return "$HEALTH_RESULT"
}

verify_scan_root_guard() {
  return "$VERIFY_RESULT"
}

python3() {
  CLEAR_CALL="$*"
  return "$CLEAR_RESULT"
}

SCAN_PROGRESS_HELPER="$REPOSITORY_DIR/scripts/scan_progress.py"
SCAN_PROGRESS_DIR="$TEST_ROOT/progress"
SCAN_PATH_MARKER=".mounted"
RESUME_RESULT=0
resume_manifest_is_safe FULL "$TEST_ROOT/roots.json" 0123456789abcdef0123456789abcdef || RESUME_RESULT=$?
test "$RESUME_RESULT" -eq 3
case " $CLEAR_CALL " in *" clear "*) ;; *) exit 1 ;; esac
case " $CLEAR_CALL " in *" --progress-root $SCAN_PROGRESS_DIR "*) ;; *) exit 1 ;; esac
case " $CLEAR_CALL " in *" --scan-id 0123456789abcdef0123456789abcdef "*) ;;
  *)
    echo "Verified remount did not clear the exact expected scan session." >&2
    exit 1
    ;;
esac

SCAN_PATH_MARKER=""
CLEAR_CALL=""
RESUME_RESULT=0
resume_manifest_is_safe CHANGED "$TEST_ROOT/roots.json" fedcba9876543210fedcba9876543210 || RESUME_RESULT=$?
test "$RESUME_RESULT" -eq 2
test -z "$CLEAR_CALL"

SCAN_PATH_MARKER=".mounted"
HEALTH_RESULT=1
CLEAR_CALL=""
RESUME_RESULT=0
resume_manifest_is_safe FULL "$TEST_ROOT/roots.json" 0123456789abcdef0123456789abcdef || RESUME_RESULT=$?
test "$RESUME_RESULT" -eq 2
test -z "$CLEAR_CALL"
