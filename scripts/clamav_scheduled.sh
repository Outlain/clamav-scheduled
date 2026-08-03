#!/bin/sh
set -eu
set -f
umask 077

# Tunables from environment
: "${TZ:=UTC}"
: "${MAXTHREADS:=13}"
: "${FULL_SCAN_PARALLEL_JOBS:=8}"
: "${CHANGED_SCAN_PARALLEL_JOBS:=8}"
: "${FULL_CHUNK_SIZE:=0}"
: "${CHANGED_CHUNK_SIZE:=0}"
: "${FULL_PROGRESS_STEPS:=100}"
: "${CHANGED_PROGRESS_STEPS:=25}"
: "${CHANGED_SCAN_TIMES:=}"
: "${CHANGED_SCAN_DAYS:=*}"
: "${FULL_SCAN_TIMES:=}"
: "${FULL_SCAN_DAYS:=sun}"
: "${SCAN_FAILURE_RETRY_INTERVAL:=300}"
: "${SCAN_PATHS:=/downloads}"
: "${EXCLUDE_PATHS:=}"
: "${FORCE_FULL_POLL_INTERVAL:=60}"
: "${STATE_DIR:=/state}"
: "${RUNTIME_DIR:=/tmp/clamav-runtime}"
: "${TMP_DIR:=${RUNTIME_DIR}/work}"
: "${DEFINITIONS_DIR:=/var/lib/clamav}"
: "${DEFINITIONS_WAIT_TIMEOUT:=300}"
: "${DEFINITIONS_MAX_AGE_SECONDS:=172800}"
: "${DEFINITIONS_STALE_ACTION:=warn}"
: "${CLAMD_MAX_SCAN_SIZE:=2000M}"
: "${CLAMD_MAX_FILE_SIZE:=2000M}"
: "${CLAMD_LOG_MAX_SIZE:=10M}"
: "${CLAMD_MAX_RECURSION:=32}"
: "${CLAMD_MAX_FILES:=10000}"
: "${CLAMD_MAX_SCAN_TIME:=900000}"
: "${CLAMD_READ_TIMEOUT:=900}"
: "${CLAMD_COMMAND_READ_TIMEOUT:=30}"
: "${CLAMD_SELF_CHECK:=300}"
: "${CLAMD_START_TIMEOUT:=180}"
: "${MAX_SCHEDULED_FILES:=1000000}"
: "${SCANLOG_MAX_BYTES:=104857600}"
: "${SCANLOG_ROTATIONS:=5}"
: "${PATH_CHECK_TIMEOUT:=10}"
: "${PATH_ENUMERATION_TIMEOUT:=300}"
: "${PATH_UNAVAILABLE_RETRY_INTERVAL:=300}"
: "${SCAN_PATH_MARKER:=}"
: "${EVENT_DIR:=/events}"
: "${VANISHED_FILE_FAILURE_COUNT:=100}"
: "${VANISHED_FILE_FAILURE_PERCENT:=10}"
: "${VANISHED_FILE_FAILURE_MINIMUM:=10}"
: "${LARGE_MEDIA_ENABLED:=true}"
: "${LARGE_MEDIA_MAX_FILE_GIB:=100}"
: "${LARGE_MEDIA_CHUNK_MIB:=1024}"
: "${LARGE_MEDIA_OVERLAP_KIB:=1024}"
: "${LARGE_MEDIA_PROBE_TIMEOUT_SECONDS:=120}"
: "${LARGE_MEDIA_SCAN_TIMEOUT_SECONDS:=21600}"
: "${MAX_LARGE_MEDIA_WORKERS:=1}"
: "${FFPROBE_BINARY:=/usr/bin/ffprobe}"

reject_deprecated_env() {
  VAR_NAME="$1"
  MESSAGE="$2"
  eval "VAR_IS_SET=\${${VAR_NAME}+set}"

  if [ -n "$VAR_IS_SET" ]; then
    echo "[ERROR] ${VAR_NAME} is no longer supported. ${MESSAGE}" >&2
    exit 1
  fi
}

reject_control_characters() {
  NAME="$1"
  VALUE="$2"
  CLEAN_VALUE=$(printf '%s' "$VALUE" | LC_ALL=C tr -d '\001-\037\177')
  if [ "$CLEAN_VALUE" != "$VALUE" ]; then
    echo "[ERROR] ${NAME} must not contain control characters." >&2
    exit 1
  fi
}

normalize_absolute_path() {
  NAME="$1"
  VALUE="$2"
  reject_control_characters "$NAME" "$VALUE"

  case "$VALUE" in
    /*)
      ;;
    *)
      echo "[ERROR] ${NAME} must be an absolute path (got: ${VALUE})" >&2
      exit 1
      ;;
  esac

  if ! realpath -m -- "$VALUE" 2>/dev/null; then
    echo "[ERROR] Unable to canonicalize ${NAME}: ${VALUE}" >&2
    exit 1
  fi
}

validate_scan_paths_config() {
  VALUE="$1"

  case "$VALUE" in
    ''|:*|*::|*:)
      echo "[ERROR] SCAN_PATHS must be a colon-separated list with no empty entries (got: ${VALUE})" >&2
      exit 1
      ;;
  esac
}

validate_optional_path_list_config() {
  NAME="$1"
  VALUE="$2"

  [ -n "$VALUE" ] || return 0

  case "$VALUE" in
    :*|*::|*:)
      echo "[ERROR] ${NAME} must be a colon-separated list with no empty entries (got: ${VALUE})" >&2
      exit 1
      ;;
  esac
}

normalize_absolute_path_list() {
  NAME="$1"
  VALUE="$2"

  [ -n "$VALUE" ] || {
    printf '\n'
    return 0
  }

  NORMALIZED=""
  OLD_IFS="$IFS"
  case $- in
    *f*) RESTORE_PATHNAME_EXPANSION=0 ;;
    *) set -f; RESTORE_PATHNAME_EXPANSION=1 ;;
  esac
  IFS=':'
  set -- $VALUE
  IFS="$OLD_IFS"
  [ "$RESTORE_PATHNAME_EXPANSION" -eq 0 ] || set +f

  for PATH_ENTRY do
    reject_control_characters "$NAME" "$PATH_ENTRY"
    case "$PATH_ENTRY" in
      /*)
        ;;
      *)
        echo "[ERROR] ${NAME} entries must be absolute paths (got: ${PATH_ENTRY})" >&2
        exit 1
        ;;
    esac

    if ! NORMALIZED_ENTRY=$(realpath -m -- "$PATH_ENTRY" 2>/dev/null); then
      echo "[ERROR] Unable to canonicalize ${NAME} entry: ${PATH_ENTRY}" >&2
      exit 1
    fi

    NORMALIZED="${NORMALIZED}${NORMALIZED:+:}${NORMALIZED_ENTRY}"
  done

  printf '%s\n' "$NORMALIZED"
}

get_primary_scan_path() {
  PATH_LIST="$1"
  OLD_IFS="$IFS"
  IFS=':'
  set -- $PATH_LIST
  IFS="$OLD_IFS"
  printf '%s\n' "${1:-}"
}

normalize_schedule_times() {
  RAW_VALUE=$(printf '%s' "$1" | tr -d ' ')

  [ -n "$RAW_VALUE" ] || {
    printf '\n'
    return 0
  }

  NORMALIZED=""
  OLD_IFS="$IFS"
  IFS=','
  set -- $RAW_VALUE
  IFS="$OLD_IFS"

  for TOKEN do
    case "$TOKEN" in
      [0-1][0-9]:[0-5][0-9]|2[0-3]:[0-5][0-9])
        case ",$NORMALIZED," in
          *,"$TOKEN",*)
            ;;
          *)
            NORMALIZED="${NORMALIZED}${NORMALIZED:+,}${TOKEN}"
            ;;
        esac
        ;;
      *)
        echo "[ERROR] Invalid schedule time '$TOKEN'. Use HH:MM in 24-hour format." >&2
        exit 1
        ;;
    esac
  done

  printf '%s\n' "$NORMALIZED"
}

normalize_schedule_days() {
  RAW_VALUE=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -d ' ')

  [ -n "$RAW_VALUE" ] || RAW_VALUE='*'

  if [ "$RAW_VALUE" = "*" ]; then
    printf '1,2,3,4,5,6,7\n'
    return 0
  fi

  NORMALIZED=""
  OLD_IFS="$IFS"
  IFS=','
  set -- $RAW_VALUE
  IFS="$OLD_IFS"

  for TOKEN do
    case "$TOKEN" in
      1|mon|monday) DAY_NUMBER=1 ;;
      2|tue|tues|tuesday) DAY_NUMBER=2 ;;
      3|wed|weds|wednesday) DAY_NUMBER=3 ;;
      4|thu|thur|thurs|thursday) DAY_NUMBER=4 ;;
      5|fri|friday) DAY_NUMBER=5 ;;
      6|sat|saturday) DAY_NUMBER=6 ;;
      7|sun|sunday) DAY_NUMBER=7 ;;
      *)
        echo "[ERROR] Invalid schedule day '$TOKEN'. Use mon-sun, monday-sunday, 1-7, or *." >&2
        exit 1
        ;;
    esac

    case ",$NORMALIZED," in
      *,"$DAY_NUMBER",*)
        ;;
      *)
        NORMALIZED="${NORMALIZED}${NORMALIZED:+,}${DAY_NUMBER}"
        ;;
    esac
  done

  printf '%s\n' "$NORMALIZED"
}

: "${SCANLOG:=/var/log/clamav/clamav_scheduled.log}"

reject_deprecated_env "DOWNLOADS_DIR" "Use SCAN_PATHS instead."
reject_deprecated_env "PARALLEL_JOBS" "Set FULL_SCAN_PARALLEL_JOBS and CHANGED_SCAN_PARALLEL_JOBS explicitly."
reject_deprecated_env "CHUNK_SIZE" "Set FULL_CHUNK_SIZE and CHANGED_CHUNK_SIZE explicitly."
reject_deprecated_env "SCAN_INTERVAL" "Use CHANGED_SCAN_DAYS/CHANGED_SCAN_TIMES and FULL_SCAN_DAYS/FULL_SCAN_TIMES."
reject_deprecated_env "CHANGED_SCAN_INTERVAL" "Use CHANGED_SCAN_DAYS and CHANGED_SCAN_TIMES."
reject_deprecated_env "FULL_SCAN_INTERVAL" "Use FULL_SCAN_DAYS and FULL_SCAN_TIMES."

validate_scan_paths_config "$SCAN_PATHS"
validate_optional_path_list_config "EXCLUDE_PATHS" "$EXCLUDE_PATHS"
SCAN_PATHS=$(normalize_absolute_path_list "SCAN_PATHS" "$SCAN_PATHS")
EXCLUDE_PATHS=$(normalize_absolute_path_list "EXCLUDE_PATHS" "$EXCLUDE_PATHS")
PRIMARY_SCAN_PATH=$(get_primary_scan_path "$SCAN_PATHS")
[ -n "$PRIMARY_SCAN_PATH" ] || {
  echo "[ERROR] Unable to determine a primary scan path from SCAN_PATHS=${SCAN_PATHS}" >&2
  exit 1
}

: "${QUARANTINE_DIR:=/quarantine}"
: "${FORCE_FULL_FLAG:=${STATE_DIR}/force_full_scan.flag}"

reject_control_characters "TZ" "$TZ"
reject_control_characters "SCAN_PATH_MARKER" "$SCAN_PATH_MARKER"
case "$SCAN_PATH_MARKER" in
  ''|.|..|*/*)
    if [ -n "$SCAN_PATH_MARKER" ]; then
      echo "[ERROR] SCAN_PATH_MARKER must be one file or directory name, not a path." >&2
      exit 1
    fi
    ;;
esac

STATE_DIR=$(normalize_absolute_path "STATE_DIR" "$STATE_DIR")
RUNTIME_DIR=$(normalize_absolute_path "RUNTIME_DIR" "$RUNTIME_DIR")
TMP_DIR=$(normalize_absolute_path "TMP_DIR" "$TMP_DIR")
DEFINITIONS_DIR=$(normalize_absolute_path "DEFINITIONS_DIR" "$DEFINITIONS_DIR")
QUARANTINE_DIR=$(normalize_absolute_path "QUARANTINE_DIR" "$QUARANTINE_DIR")
FORCE_FULL_FLAG=$(normalize_absolute_path "FORCE_FULL_FLAG" "$FORCE_FULL_FLAG")
SCANLOG=$(normalize_absolute_path "SCANLOG" "$SCANLOG")
EVENT_DIR=$(normalize_absolute_path "EVENT_DIR" "$EVENT_DIR")
FFPROBE_BINARY=$(normalize_absolute_path "FFPROBE_BINARY" "$FFPROBE_BINARY")
CLAMD_SOCKET="$RUNTIME_DIR/clamd.sock"
CLAMD_CONFIG="$RUNTIME_DIR/clamd.conf"
CLAMD_PID_FILE="$RUNTIME_DIR/clamd.pid"
CLAMD_LOG_FILE="$RUNTIME_DIR/clamd.log"
CLAMD_OUTPUT_FILE="$RUNTIME_DIR/clamd.out"
export CLAMD_SOCKET DEFINITIONS_DIR DEFINITIONS_MAX_AGE_SECONDS DEFINITIONS_WAIT_TIMEOUT DEFINITIONS_STALE_ACTION

CHANGED_SCAN_TIMES=$(normalize_schedule_times "$CHANGED_SCAN_TIMES")
CHANGED_SCAN_DAYS=$(normalize_schedule_days "$CHANGED_SCAN_DAYS")
FULL_SCAN_TIMES=$(normalize_schedule_times "$FULL_SCAN_TIMES")
FULL_SCAN_DAYS=$(normalize_schedule_days "$FULL_SCAN_DAYS")

validate_bounded_int() {
  NAME="$1"
  VALUE="$2"
  MINIMUM="$3"
  MAXIMUM="$4"

  case "$VALUE" in
    ''|*[!0-9]*)
      echo "[ERROR] ${NAME} must be an integer between ${MINIMUM} and ${MAXIMUM} (got: ${VALUE})" >&2
      exit 1
      ;;
  esac

  if [ "${#VALUE}" -gt 9 ] || [ "$VALUE" -lt "$MINIMUM" ] || [ "$VALUE" -gt "$MAXIMUM" ]; then
    echo "[ERROR] ${NAME} must be between ${MINIMUM} and ${MAXIMUM} (got: ${VALUE})" >&2
    exit 1
  fi
}

normalize_clamd_size() {
  NAME="$1"
  VALUE="$2"
  NORMALIZED_VALUE=$(printf '%s' "$VALUE" | tr '[:lower:]' '[:upper:]')

  case "$NORMALIZED_VALUE" in
    *K|*M|*G)
      NUMBER=${NORMALIZED_VALUE%?}
      ;;
    *)
      NUMBER=$NORMALIZED_VALUE
      ;;
  esac
  case "$NUMBER" in
    ''|*[!0-9]*)
      echo "[ERROR] ${NAME} must be a positive byte count with an optional K, M, or G suffix (got: ${VALUE})" >&2
      exit 1
      ;;
  esac
  if [ "${#NUMBER}" -gt 9 ] || [ "$NUMBER" -le 0 ]; then
    echo "[ERROR] ${NAME} must be a positive bounded size (got: ${VALUE})" >&2
    exit 1
  fi
  printf '%s\n' "$NORMALIZED_VALUE"
}

clamd_size_bytes() {
  VALUE="$1"
  case "$VALUE" in
    *K)
      printf '%s\n' "$((${VALUE%?} * 1024))"
      ;;
    *M)
      printf '%s\n' "$((${VALUE%?} * 1024 * 1024))"
      ;;
    *G)
      printf '%s\n' "$((${VALUE%?} * 1024 * 1024 * 1024))"
      ;;
    *)
      printf '%s\n' "$VALUE"
      ;;
  esac
}

validate_bounded_int "MAXTHREADS" "$MAXTHREADS" 1 64
validate_bounded_int "FULL_SCAN_PARALLEL_JOBS" "$FULL_SCAN_PARALLEL_JOBS" 1 64
validate_bounded_int "CHANGED_SCAN_PARALLEL_JOBS" "$CHANGED_SCAN_PARALLEL_JOBS" 1 64
validate_bounded_int "FULL_PROGRESS_STEPS" "$FULL_PROGRESS_STEPS" 1 10000
validate_bounded_int "CHANGED_PROGRESS_STEPS" "$CHANGED_PROGRESS_STEPS" 1 10000
validate_bounded_int "FULL_CHUNK_SIZE" "$FULL_CHUNK_SIZE" 0 1000000
validate_bounded_int "CHANGED_CHUNK_SIZE" "$CHANGED_CHUNK_SIZE" 0 1000000
validate_bounded_int "SCAN_FAILURE_RETRY_INTERVAL" "$SCAN_FAILURE_RETRY_INTERVAL" 1 86400
validate_bounded_int "FORCE_FULL_POLL_INTERVAL" "$FORCE_FULL_POLL_INTERVAL" 1 3600
validate_bounded_int "PATH_CHECK_TIMEOUT" "$PATH_CHECK_TIMEOUT" 1 300
validate_bounded_int "PATH_ENUMERATION_TIMEOUT" "$PATH_ENUMERATION_TIMEOUT" 1 86400
validate_bounded_int "PATH_UNAVAILABLE_RETRY_INTERVAL" "$PATH_UNAVAILABLE_RETRY_INTERVAL" 1 86400
validate_bounded_int "DEFINITIONS_WAIT_TIMEOUT" "$DEFINITIONS_WAIT_TIMEOUT" 1 3600
validate_bounded_int "DEFINITIONS_MAX_AGE_SECONDS" "$DEFINITIONS_MAX_AGE_SECONDS" 60 2678400
validate_bounded_int "CLAMD_MAX_RECURSION" "$CLAMD_MAX_RECURSION" 1 100
validate_bounded_int "CLAMD_MAX_FILES" "$CLAMD_MAX_FILES" 1 1000000
validate_bounded_int "CLAMD_MAX_SCAN_TIME" "$CLAMD_MAX_SCAN_TIME" 1000 3600000
validate_bounded_int "CLAMD_READ_TIMEOUT" "$CLAMD_READ_TIMEOUT" 1 3600
validate_bounded_int "CLAMD_COMMAND_READ_TIMEOUT" "$CLAMD_COMMAND_READ_TIMEOUT" 1 300
validate_bounded_int "CLAMD_SELF_CHECK" "$CLAMD_SELF_CHECK" 30 3600
validate_bounded_int "CLAMD_START_TIMEOUT" "$CLAMD_START_TIMEOUT" 30 900
validate_bounded_int "MAX_SCHEDULED_FILES" "$MAX_SCHEDULED_FILES" 1 5000000
validate_bounded_int "SCANLOG_MAX_BYTES" "$SCANLOG_MAX_BYTES" 1048576 1073741824
validate_bounded_int "SCANLOG_ROTATIONS" "$SCANLOG_ROTATIONS" 1 20
validate_bounded_int "VANISHED_FILE_FAILURE_COUNT" "$VANISHED_FILE_FAILURE_COUNT" 0 1000000
validate_bounded_int "VANISHED_FILE_FAILURE_PERCENT" "$VANISHED_FILE_FAILURE_PERCENT" 0 100
validate_bounded_int "VANISHED_FILE_FAILURE_MINIMUM" "$VANISHED_FILE_FAILURE_MINIMUM" 1 1000000
validate_bounded_int "LARGE_MEDIA_MAX_FILE_GIB" "$LARGE_MEDIA_MAX_FILE_GIB" 1 1000
validate_bounded_int "LARGE_MEDIA_CHUNK_MIB" "$LARGE_MEDIA_CHUNK_MIB" 1 2000
validate_bounded_int "LARGE_MEDIA_OVERLAP_KIB" "$LARGE_MEDIA_OVERLAP_KIB" 0 2000000
validate_bounded_int "LARGE_MEDIA_PROBE_TIMEOUT_SECONDS" "$LARGE_MEDIA_PROBE_TIMEOUT_SECONDS" 1 3600
validate_bounded_int "LARGE_MEDIA_SCAN_TIMEOUT_SECONDS" "$LARGE_MEDIA_SCAN_TIMEOUT_SECONDS" 60 86400
validate_bounded_int "MAX_LARGE_MEDIA_WORKERS" "$MAX_LARGE_MEDIA_WORKERS" 1 64

if [ "$FULL_SCAN_PARALLEL_JOBS" -gt "$MAXTHREADS" ] || [ "$CHANGED_SCAN_PARALLEL_JOBS" -gt "$MAXTHREADS" ]; then
  echo "[ERROR] Scan parallel jobs must not exceed MAXTHREADS=${MAXTHREADS}." >&2
  exit 1
fi

: "${CLAMD_MAX_QUEUE:=$((MAXTHREADS * 2))}"
validate_bounded_int "CLAMD_MAX_QUEUE" "$CLAMD_MAX_QUEUE" "$MAXTHREADS" 128
CLAMD_MAX_SCAN_SIZE=$(normalize_clamd_size "CLAMD_MAX_SCAN_SIZE" "$CLAMD_MAX_SCAN_SIZE")
CLAMD_MAX_FILE_SIZE=$(normalize_clamd_size "CLAMD_MAX_FILE_SIZE" "$CLAMD_MAX_FILE_SIZE")
CLAMD_LOG_MAX_SIZE=$(normalize_clamd_size "CLAMD_LOG_MAX_SIZE" "$CLAMD_LOG_MAX_SIZE")
CLAMD_MAX_SCAN_SIZE_BYTES=$(clamd_size_bytes "$CLAMD_MAX_SCAN_SIZE")
CLAMD_MAX_FILE_SIZE_BYTES=$(clamd_size_bytes "$CLAMD_MAX_FILE_SIZE")
CLAMD_LOG_MAX_SIZE_BYTES=$(clamd_size_bytes "$CLAMD_LOG_MAX_SIZE")
CLAMD_ABSOLUTE_SIZE_LIMIT=$((2000 * 1024 * 1024))
if [ "$CLAMD_MAX_SCAN_SIZE_BYTES" -gt "$CLAMD_ABSOLUTE_SIZE_LIMIT" ] || [ "$CLAMD_MAX_FILE_SIZE_BYTES" -gt "$CLAMD_ABSOLUTE_SIZE_LIMIT" ]; then
  echo "[ERROR] CLAMD_MAX_SCAN_SIZE and CLAMD_MAX_FILE_SIZE must not exceed 2000M." >&2
  exit 1
fi
if [ "$CLAMD_MAX_FILE_SIZE_BYTES" -gt "$CLAMD_MAX_SCAN_SIZE_BYTES" ]; then
  echo "[ERROR] CLAMD_MAX_FILE_SIZE must not exceed CLAMD_MAX_SCAN_SIZE." >&2
  exit 1
fi
LARGE_MEDIA_CHUNK_BYTES=$((LARGE_MEDIA_CHUNK_MIB * 1024 * 1024))
LARGE_MEDIA_OVERLAP_BYTES=$((LARGE_MEDIA_OVERLAP_KIB * 1024))
if [ "$LARGE_MEDIA_CHUNK_BYTES" -gt "$CLAMD_MAX_FILE_SIZE_BYTES" ]; then
  echo "[ERROR] LARGE_MEDIA_CHUNK_MIB must not exceed CLAMD_MAX_FILE_SIZE." >&2
  exit 1
fi
if [ "$LARGE_MEDIA_OVERLAP_BYTES" -ge "$LARGE_MEDIA_CHUNK_BYTES" ]; then
  echo "[ERROR] LARGE_MEDIA_OVERLAP_KIB must be smaller than LARGE_MEDIA_CHUNK_MIB." >&2
  exit 1
fi
if [ "$MAX_LARGE_MEDIA_WORKERS" -gt "$MAXTHREADS" ]; then
  echo "[ERROR] MAX_LARGE_MEDIA_WORKERS must not exceed MAXTHREADS." >&2
  exit 1
fi
LARGE_MEDIA_ENABLED=$(printf '%s' "$LARGE_MEDIA_ENABLED" | tr '[:upper:]' '[:lower:]')
case "$LARGE_MEDIA_ENABLED" in
  true|false) ;;
  *)
    echo "[ERROR] LARGE_MEDIA_ENABLED must be 'true' or 'false'." >&2
    exit 1
    ;;
esac
if [ "$LARGE_MEDIA_ENABLED" = "true" ] && [ ! -x "$FFPROBE_BINARY" ]; then
  echo "[ERROR] FFPROBE_BINARY is not executable: ${FFPROBE_BINARY}" >&2
  exit 1
fi
if [ "$CLAMD_LOG_MAX_SIZE_BYTES" -gt 1073741824 ]; then
  echo "[ERROR] CLAMD_LOG_MAX_SIZE must not exceed 1G." >&2
  exit 1
fi

DEFINITIONS_STALE_ACTION=$(printf '%s' "$DEFINITIONS_STALE_ACTION" | tr '[:upper:]' '[:lower:]')
case "$DEFINITIONS_STALE_ACTION" in
  warn|fail)
    ;;
  *)
    echo "[ERROR] DEFINITIONS_STALE_ACTION must be 'warn' or 'fail'." >&2
    exit 1
    ;;
esac

if [ -z "$CHANGED_SCAN_TIMES" ]; then
  echo "[ERROR] CHANGED_SCAN_TIMES must contain one or more HH:MM values." >&2
  exit 1
fi

if [ -z "$FULL_SCAN_TIMES" ]; then
  echo "[ERROR] FULL_SCAN_TIMES must contain one or more HH:MM values." >&2
  exit 1
fi

ensure_writable_directory() {
  DIRECTORY="$1"
  DESCRIPTION="$2"
  if [ ! -d "$DIRECTORY" ] || [ ! -r "$DIRECTORY" ] || [ ! -w "$DIRECTORY" ] || [ ! -x "$DIRECTORY" ]; then
    echo "[ERROR] ${DESCRIPTION} must be a readable, writable, searchable directory: ${DIRECTORY}" >&2
    exit 1
  fi
  if ! PROBE_FILE=$(mktemp "$DIRECTORY/.clamav-write-probe.XXXXXX"); then
    echo "[ERROR] ${DESCRIPTION} failed an actual create-file permission check: ${DIRECTORY}" >&2
    exit 1
  fi
  if ! rm -f -- "$PROBE_FILE"; then
    echo "[ERROR] ${DESCRIPTION} failed the delete portion of its permission check: ${DIRECTORY}" >&2
    exit 1
  fi
}

SCANLOG_DIR=$(dirname -- "$SCANLOG")
for REQUIRED_DIRECTORY in "$QUARANTINE_DIR" "$STATE_DIR" "$RUNTIME_DIR" "$TMP_DIR" "$SCANLOG_DIR" "$EVENT_DIR"; do
  if ! mkdir -p -- "$REQUIRED_DIRECTORY"; then
    echo "[ERROR] Could not create required writable directory: ${REQUIRED_DIRECTORY}" >&2
    exit 1
  fi
done
ensure_writable_directory "$QUARANTINE_DIR" "Quarantine directory"
ensure_writable_directory "$STATE_DIR" "State directory"
ensure_writable_directory "$RUNTIME_DIR" "Runtime directory"
ensure_writable_directory "$TMP_DIR" "Temporary work directory"
ensure_writable_directory "$SCANLOG_DIR" "Scan-log directory"
ensure_writable_directory "$EVENT_DIR" "Structured-event directory"

emit_scan_event() {
  if ! python3 /usr/local/bin/event_writer.py --event-dir "$EVENT_DIR" "$@"; then
    echo "[ERROR] Could not persist a structured notification event." | tee -a "$SCANLOG"
    return 1
  fi
}

SCHEDULED_FAILURE_MARKER="$STATE_DIR/.scheduled-service-failure"

mark_scheduled_failure() {
  if mkdir -- "$SCHEDULED_FAILURE_MARKER" 2>/dev/null; then
    return 0
  fi
  if [ -d "$SCHEDULED_FAILURE_MARKER" ] && [ ! -L "$SCHEDULED_FAILURE_MARKER" ]; then
    return 0
  fi
  echo "[ERROR] Could not persist the scheduled-service failure marker." | tee -a "$SCANLOG"
  return 1
}

emit_scheduled_recovery() {
  RECOVERY_MESSAGE="$1"
  if [ ! -d "$SCHEDULED_FAILURE_MARKER" ] || [ -L "$SCHEDULED_FAILURE_MARKER" ]; then
    return 0
  fi
  if emit_scan_event --event-type service_recovered --severity info --message "$RECOVERY_MESSAGE" --action-success true; then
    if rmdir -- "$SCHEDULED_FAILURE_MARKER"; then
      return 0
    fi
    echo "[ERROR] Recovery event was written but its failure marker could not be cleared." | tee -a "$SCANLOG"
  fi
  return 1
}

OLD_IFS="$IFS"
IFS=':'
set -- $SCAN_PATHS
IFS="$OLD_IFS"
for SCAN_PATH_PERMISSION_CHECK do
  ensure_writable_directory "$SCAN_PATH_PERMISSION_CHECK" "Scan root (write access is required for quarantine removal)"
  case "$SCAN_PATH_PERMISSION_CHECK" in
    "$QUARANTINE_DIR"|"$QUARANTINE_DIR"/*)
      echo "[ERROR] QUARANTINE_DIR must not be equal to or contain a scan root (quarantine: ${QUARANTINE_DIR}, root: ${SCAN_PATH_PERMISSION_CHECK})." >&2
      exit 1
      ;;
  esac
  for INTERNAL_PATH in "$STATE_DIR" "$RUNTIME_DIR" "$TMP_DIR" "$DEFINITIONS_DIR" "$SCANLOG" "$EVENT_DIR"; do
    case "$INTERNAL_PATH" in
      "$SCAN_PATH_PERMISSION_CHECK"|"$SCAN_PATH_PERMISSION_CHECK"/*)
        echo "[ERROR] Mutable state, runtime files, definitions, and logs must be outside scan roots (path: ${INTERNAL_PATH}, root: ${SCAN_PATH_PERMISSION_CHECK})." >&2
        exit 1
        ;;
    esac
  done
done

if ! : >> "$SCANLOG"; then
  echo "[ERROR] Scan log is not writable: ${SCANLOG}" >&2
  exit 1
fi

echo "=== Starting scheduled ClamAV scanner ===" | tee -a "$SCANLOG"
echo "TZ=$TZ MAXTHREADS=$MAXTHREADS FULL_SCAN_PARALLEL_JOBS=$FULL_SCAN_PARALLEL_JOBS CHANGED_SCAN_PARALLEL_JOBS=$CHANGED_SCAN_PARALLEL_JOBS FULL_CHUNK_SIZE=$FULL_CHUNK_SIZE CHANGED_CHUNK_SIZE=$CHANGED_CHUNK_SIZE FULL_PROGRESS_STEPS=$FULL_PROGRESS_STEPS CHANGED_PROGRESS_STEPS=$CHANGED_PROGRESS_STEPS CHANGED_SCAN_DAYS=$CHANGED_SCAN_DAYS CHANGED_SCAN_TIMES=$CHANGED_SCAN_TIMES FULL_SCAN_DAYS=$FULL_SCAN_DAYS FULL_SCAN_TIMES=$FULL_SCAN_TIMES SCAN_FAILURE_RETRY_INTERVAL=$SCAN_FAILURE_RETRY_INTERVAL FORCE_FULL_POLL_INTERVAL=$FORCE_FULL_POLL_INTERVAL SCAN_PATHS=$SCAN_PATHS EXCLUDE_PATHS=$EXCLUDE_PATHS QUARANTINE_DIR=$QUARANTINE_DIR STATE_DIR=$STATE_DIR EVENT_DIR=$EVENT_DIR DEFINITIONS_DIR=$DEFINITIONS_DIR DEFINITIONS_MAX_AGE_SECONDS=$DEFINITIONS_MAX_AGE_SECONDS PATH_CHECK_TIMEOUT=$PATH_CHECK_TIMEOUT PATH_ENUMERATION_TIMEOUT=$PATH_ENUMERATION_TIMEOUT PATH_UNAVAILABLE_RETRY_INTERVAL=$PATH_UNAVAILABLE_RETRY_INTERVAL SCAN_PATH_MARKER=$SCAN_PATH_MARKER VANISHED_FILE_FAILURE_COUNT=$VANISHED_FILE_FAILURE_COUNT VANISHED_FILE_FAILURE_PERCENT=$VANISHED_FILE_FAILURE_PERCENT VANISHED_FILE_FAILURE_MINIMUM=$VANISHED_FILE_FAILURE_MINIMUM LARGE_MEDIA_ENABLED=$LARGE_MEDIA_ENABLED LARGE_MEDIA_MAX_FILE_GIB=$LARGE_MEDIA_MAX_FILE_GIB LARGE_MEDIA_CHUNK_MIB=$LARGE_MEDIA_CHUNK_MIB LARGE_MEDIA_OVERLAP_KIB=$LARGE_MEDIA_OVERLAP_KIB MAX_LARGE_MEDIA_WORKERS=$MAX_LARGE_MEDIA_WORKERS" | tee -a "$SCANLOG"

echo "Waiting up to ${DEFINITIONS_WAIT_TIMEOUT}s for external definitions..." | tee -a "$SCANLOG"
if ! python3 /usr/local/bin/clamav_healthcheck.py --wait; then
  echo "[ERROR] ClamAV definitions did not pass startup readiness checks." | tee -a "$SCANLOG"
  emit_scan_event --event-type definitions_stale --severity critical --message "Scheduled scanner definitions failed startup readiness" --action-success false || true
  exit 1
fi

cat > "$CLAMD_CONFIG" <<EOF2
DatabaseDirectory ${DEFINITIONS_DIR}
LocalSocket ${CLAMD_SOCKET}
LocalSocketMode 600
PidFile ${CLAMD_PID_FILE}
LogFile ${CLAMD_LOG_FILE}
LogTime yes
LogFileMaxSize ${CLAMD_LOG_MAX_SIZE}
LogRotate yes
Foreground yes

MaxThreads ${MAXTHREADS}
MaxQueue ${CLAMD_MAX_QUEUE}
ReadTimeout ${CLAMD_READ_TIMEOUT}
CommandReadTimeout ${CLAMD_COMMAND_READ_TIMEOUT}
SelfCheck ${CLAMD_SELF_CHECK}
MaxScanTime ${CLAMD_MAX_SCAN_TIME}
MaxScanSize ${CLAMD_MAX_SCAN_SIZE}
MaxFileSize ${CLAMD_MAX_FILE_SIZE}
StreamMaxLength ${CLAMD_MAX_FILE_SIZE}
MaxRecursion ${CLAMD_MAX_RECURSION}
MaxFiles ${CLAMD_MAX_FILES}
TemporaryDirectory ${TMP_DIR}
AlertExceedsMax yes
ConcurrentDatabaseReload no
EOF2

echo "Starting clamd with MaxThreads=${MAXTHREADS}, MaxQueue=${CLAMD_MAX_QUEUE}, MaxScanSize=${CLAMD_MAX_SCAN_SIZE}, MaxFileSize=${CLAMD_MAX_FILE_SIZE}, SelfCheck=${CLAMD_SELF_CHECK}s..." | tee -a "$SCANLOG"
clamd -c "$CLAMD_CONFIG" >"$CLAMD_OUTPUT_FILE" 2>&1 &
CLAMD_PID=$!

release_lock() {
  flock -u 9 2>/dev/null || true
  exec 9>&- 2>/dev/null || true
}

cleanup() {
  [ "${CLEANUP_DONE:-0}" -eq 0 ] || return 0
  CLEANUP_DONE=1
  release_lock
  echo "Stopping clamd..." | tee -a "$SCANLOG"
  kill "$CLAMD_PID" 2>/dev/null || true
  wait "$CLAMD_PID" 2>/dev/null || true
}

handle_termination() {
  cleanup
  trap - EXIT
  exit 0
}

trap cleanup EXIT
trap handle_termination INT TERM

i=0
while [ ! -S "$CLAMD_SOCKET" ] && [ $i -lt "$CLAMD_START_TIMEOUT" ]; do
  i=$((i+1))
  sleep 1
done

if [ ! -S "$CLAMD_SOCKET" ]; then
  echo "[ERROR] clamd socket never appeared. Last clamd output:" | tee -a "$SCANLOG"
  mark_scheduled_failure || true
  emit_scan_event --event-type scan_failed --severity warning --message "Scheduled scanner ClamD socket did not become ready" --action-success false || true
  tail -n 200 "$CLAMD_OUTPUT_FILE" >&2 2>/dev/null || true
  exit 1
fi

READY_TEST_FILE="$TMP_DIR/clamav_readytest.txt"
echo "readytest" > "$READY_TEST_FILE"
i=0
while [ $i -lt "$CLAMD_START_TIMEOUT" ]; do
  if clamdscan --config-file="$CLAMD_CONFIG" --fdpass --no-summary "$READY_TEST_FILE" >/dev/null 2>&1; then
    echo "clamd ready." | tee -a "$SCANLOG"
    break
  fi
  i=$((i+1))
  sleep 1
done

rm -f -- "$READY_TEST_FILE"

if [ $i -ge "$CLAMD_START_TIMEOUT" ]; then
  echo "[ERROR] clamd started but never accepted descriptor scans. Last clamd output:" | tee -a "$SCANLOG"
  mark_scheduled_failure || true
  emit_scan_event --event-type scan_failed --severity warning --message "Scheduled scanner ClamD did not accept descriptor scans" --action-success false || true
  tail -n 200 "$CLAMD_OUTPUT_FILE" >&2 2>/dev/null || true
  exit 1
fi

ensure_clamd_alive() {
  if kill -0 "$CLAMD_PID" 2>/dev/null; then
    return 0
  fi

  echo "[ERROR] clamd exited unexpectedly; the scheduler must restart the complete scanner process group." | tee -a "$SCANLOG"
  mark_scheduled_failure || true
  emit_scan_event --event-type scan_failed --severity warning --message "Scheduled scanner ClamD exited unexpectedly" --action-success false || true
  tail -n 200 "$CLAMD_OUTPUT_FILE" >&2 2>/dev/null || true
  return 1
}

MANUAL_FULL_REQUEST_FILE="$STATE_DIR/manual_full_scan_request.env"
MANUAL_CHANGED_REQUEST_FILE="$STATE_DIR/manual_changed_scan_request.env"
NEXT_CHANGED_RETRY_EPOCH=0
NEXT_FULL_RETRY_EPOCH=0

read_checkpoint() {
  python3 /usr/local/bin/checkpoint_state.py read --state-dir "$STATE_DIR" --field "$1"
}

update_checkpoints() {
  python3 /usr/local/bin/checkpoint_state.py update --state-dir "$STATE_DIR" --full "$1" --changed "$2"
}

min_int() {
  A="$1"
  B="$2"

  if [ "$A" -le "$B" ]; then
    echo "$A"
  else
    echo "$B"
  fi
}

validate_nonnegative_numeric_string() {
  VALUE="$1"
  case "$VALUE" in
    ''|*[!0-9]*)
      return 1
      ;;
  esac
  return 0
}

path_in_quarantine() {
  TARGET_PATH="$1"

  case "$TARGET_PATH" in
    "$QUARANTINE_DIR"|"$QUARANTINE_DIR"/*)
      return 0
      ;;
  esac

  return 1
}

path_under_scan_root() {
  TARGET_PATH="$1"
  ROOT_PATH="$2"

  case "$TARGET_PATH" in
    "$ROOT_PATH"|"$ROOT_PATH"/*)
      return 0
      ;;
  esac

  return 1
}

get_scan_root_for_path() {
  TARGET_PATH="$1"
  MATCHED_ROOT=""
  MATCHED_LENGTH=0
  OLD_IFS="$IFS"

  IFS=':'
  set -- $SCAN_PATHS
  IFS="$OLD_IFS"

  for ROOT_PATH do
    if path_under_scan_root "$TARGET_PATH" "$ROOT_PATH"; then
      ROOT_LENGTH=$(printf '%s' "$ROOT_PATH" | wc -c | tr -d ' ')
      if [ "$ROOT_LENGTH" -gt "$MATCHED_LENGTH" ]; then
        MATCHED_ROOT="$ROOT_PATH"
        MATCHED_LENGTH="$ROOT_LENGTH"
      fi
    fi
  done

  printf '%s\n' "$MATCHED_ROOT"
}

load_manual_full_request() {
  MANUAL_FULL_REQUEST_TARGET_PATHS=""
  MANUAL_FULL_REQUEST_IGNORE_PATHS=""
  MANUAL_FULL_REQUEST_CREATED_AT=""

  [ -f "$MANUAL_FULL_REQUEST_FILE" ] || return 1

  while IFS='=' read -r KEY VALUE || [ -n "$KEY" ]; do
    case "$KEY" in
      REQUEST_TARGET_PATHS)
        MANUAL_FULL_REQUEST_TARGET_PATHS="$VALUE"
        ;;
      REQUEST_PATHS)
        [ -n "$MANUAL_FULL_REQUEST_TARGET_PATHS" ] || MANUAL_FULL_REQUEST_TARGET_PATHS="$VALUE"
        ;;
      REQUEST_IGNORE_PATHS)
        MANUAL_FULL_REQUEST_IGNORE_PATHS="$VALUE"
        ;;
      REQUEST_CREATED_AT)
        MANUAL_FULL_REQUEST_CREATED_AT="$VALUE"
        ;;
    esac
  done < "$MANUAL_FULL_REQUEST_FILE"

  validate_optional_path_list_config "MANUAL_FULL_REQUEST_TARGET_PATHS" "$MANUAL_FULL_REQUEST_TARGET_PATHS"
  validate_optional_path_list_config "MANUAL_FULL_REQUEST_IGNORE_PATHS" "$MANUAL_FULL_REQUEST_IGNORE_PATHS"
  MANUAL_FULL_REQUEST_TARGET_PATHS=$(normalize_absolute_path_list "MANUAL_FULL_REQUEST_TARGET_PATHS" "$MANUAL_FULL_REQUEST_TARGET_PATHS")
  MANUAL_FULL_REQUEST_IGNORE_PATHS=$(normalize_absolute_path_list "MANUAL_FULL_REQUEST_IGNORE_PATHS" "$MANUAL_FULL_REQUEST_IGNORE_PATHS")

  return 0
}

load_manual_changed_request() {
  MANUAL_CHANGED_REQUEST_MODE=""
  MANUAL_CHANGED_REFERENCE_EPOCH=""
  MANUAL_CHANGED_LOOKBACK_SECONDS=0
  MANUAL_CHANGED_REQUEST_TARGET_PATHS=""
  MANUAL_CHANGED_REQUEST_IGNORE_PATHS=""
  MANUAL_CHANGED_REQUEST_CREATED_AT=""

  [ -f "$MANUAL_CHANGED_REQUEST_FILE" ] || return 1

  while IFS='=' read -r KEY VALUE || [ -n "$KEY" ]; do
    case "$KEY" in
      REQUEST_MODE)
        MANUAL_CHANGED_REQUEST_MODE="$VALUE"
        ;;
      REQUEST_REFERENCE_EPOCH)
        MANUAL_CHANGED_REFERENCE_EPOCH="$VALUE"
        ;;
      REQUEST_LOOKBACK_SECONDS)
        MANUAL_CHANGED_LOOKBACK_SECONDS="$VALUE"
        ;;
      REQUEST_TARGET_PATHS)
        MANUAL_CHANGED_REQUEST_TARGET_PATHS="$VALUE"
        ;;
      REQUEST_PATHS)
        [ -n "$MANUAL_CHANGED_REQUEST_TARGET_PATHS" ] || MANUAL_CHANGED_REQUEST_TARGET_PATHS="$VALUE"
        ;;
      REQUEST_IGNORE_PATHS)
        MANUAL_CHANGED_REQUEST_IGNORE_PATHS="$VALUE"
        ;;
      REQUEST_CREATED_AT)
        MANUAL_CHANGED_REQUEST_CREATED_AT="$VALUE"
        ;;
    esac
  done < "$MANUAL_CHANGED_REQUEST_FILE"

  case "$MANUAL_CHANGED_REQUEST_MODE" in
    since_last|relative)
      ;;
    *)
      return 2
      ;;
  esac

  validate_nonnegative_numeric_string "$MANUAL_CHANGED_REFERENCE_EPOCH" || return 2
  validate_nonnegative_numeric_string "$MANUAL_CHANGED_LOOKBACK_SECONDS" || return 2

  validate_optional_path_list_config "MANUAL_CHANGED_REQUEST_TARGET_PATHS" "$MANUAL_CHANGED_REQUEST_TARGET_PATHS"
  validate_optional_path_list_config "MANUAL_CHANGED_REQUEST_IGNORE_PATHS" "$MANUAL_CHANGED_REQUEST_IGNORE_PATHS"
  MANUAL_CHANGED_REQUEST_TARGET_PATHS=$(normalize_absolute_path_list "MANUAL_CHANGED_REQUEST_TARGET_PATHS" "$MANUAL_CHANGED_REQUEST_TARGET_PATHS")
  MANUAL_CHANGED_REQUEST_IGNORE_PATHS=$(normalize_absolute_path_list "MANUAL_CHANGED_REQUEST_IGNORE_PATHS" "$MANUAL_CHANGED_REQUEST_IGNORE_PATHS")

  return 0
}

manual_full_request_should_wake() {
  CURRENT_EPOCH="$1"

  [ -f "$MANUAL_FULL_REQUEST_FILE" ] || return 1
  [ "$NEXT_FULL_RETRY_EPOCH" -le "$CURRENT_EPOCH" ]
}

manual_changed_request_should_wake() {
  CURRENT_EPOCH="$1"

  [ -f "$MANUAL_CHANGED_REQUEST_FILE" ] || return 1
  [ "$NEXT_CHANGED_RETRY_EPOCH" -le "$CURRENT_EPOCH" ]
}

force_full_should_wake() {
  CURRENT_EPOCH="$1"

  [ -f "$FORCE_FULL_FLAG" ] || return 1
  [ "$NEXT_FULL_RETRY_EPOCH" -le "$CURRENT_EPOCH" ]
}

schedule_day_allowed() {
  WEEKDAY="$1"
  SCHEDULE_DAYS="$2"

  case ",$SCHEDULE_DAYS," in
    *,"$WEEKDAY",*)
      return 0
      ;;
  esac

  return 1
}

get_relative_date() {
  BASE_DATE="$1"
  DAY_OFFSET="$2"

  if [ "$DAY_OFFSET" -ge 0 ]; then
    date -d "$BASE_DATE +${DAY_OFFSET} day" +%F
  else
    date -d "$BASE_DATE ${DAY_OFFSET} day" +%F
  fi
}

get_last_scheduled_epoch() {
  SCHEDULE_DAYS="$1"
  SCHEDULE_TIMES="$2"
  REFERENCE_EPOCH="$3"
  BASE_DATE=$(date -d "@$REFERENCE_EPOCH" +%F)
  LAST_EPOCH=0
  DAY_OFFSET=0

  while [ "$DAY_OFFSET" -ge -7 ]; do
    CANDIDATE_DATE=$(get_relative_date "$BASE_DATE" "$DAY_OFFSET")
    WEEKDAY=$(date -d "$CANDIDATE_DATE" +%u)

    if schedule_day_allowed "$WEEKDAY" "$SCHEDULE_DAYS"; then
      OLD_IFS="$IFS"
      IFS=','
      set -- $SCHEDULE_TIMES
      IFS="$OLD_IFS"

      for TIME_VALUE do
        CANDIDATE_EPOCH=$(date -d "$CANDIDATE_DATE ${TIME_VALUE}:00" +%s)
        if [ "$CANDIDATE_EPOCH" -le "$REFERENCE_EPOCH" ] && [ "$CANDIDATE_EPOCH" -gt "$LAST_EPOCH" ]; then
          LAST_EPOCH="$CANDIDATE_EPOCH"
        fi
      done
    fi

    DAY_OFFSET=$((DAY_OFFSET - 1))
  done

  printf '%s\n' "$LAST_EPOCH"
}

get_next_scheduled_epoch() {
  SCHEDULE_DAYS="$1"
  SCHEDULE_TIMES="$2"
  REFERENCE_EPOCH="$3"
  BASE_DATE=$(date -d "@$REFERENCE_EPOCH" +%F)
  NEXT_EPOCH=0
  DAY_OFFSET=0

  while [ "$DAY_OFFSET" -le 7 ]; do
    CANDIDATE_DATE=$(get_relative_date "$BASE_DATE" "$DAY_OFFSET")
    WEEKDAY=$(date -d "$CANDIDATE_DATE" +%u)

    if schedule_day_allowed "$WEEKDAY" "$SCHEDULE_DAYS"; then
      OLD_IFS="$IFS"
      IFS=','
      set -- $SCHEDULE_TIMES
      IFS="$OLD_IFS"

      for TIME_VALUE do
        CANDIDATE_EPOCH=$(date -d "$CANDIDATE_DATE ${TIME_VALUE}:00" +%s)
        if [ "$CANDIDATE_EPOCH" -gt "$REFERENCE_EPOCH" ] && { [ "$NEXT_EPOCH" -eq 0 ] || [ "$CANDIDATE_EPOCH" -lt "$NEXT_EPOCH" ]; }; then
          NEXT_EPOCH="$CANDIDATE_EPOCH"
        fi
      done
    fi

    DAY_OFFSET=$((DAY_OFFSET + 1))
  done

  printf '%s\n' "$NEXT_EPOCH"
}

evaluate_changed_trigger() {
  CHANGED_DUE=0
  CHANGED_MANUAL_REQUEST=0

  if [ "$MANUAL_CHANGED_REQUEST" -eq 1 ]; then
    if [ "$NEXT_CHANGED_RETRY_EPOCH" -gt "$NOW" ]; then
      CHANGED_NEXT_WAKE_EPOCH="$NEXT_CHANGED_RETRY_EPOCH"
    else
      CHANGED_DUE=1
      CHANGED_MANUAL_REQUEST=1
      CHANGED_NEXT_WAKE_EPOCH="$NOW"
    fi
    return 0
  fi

  CHANGED_LAST_SLOT_EPOCH=$(get_last_scheduled_epoch "$CHANGED_SCAN_DAYS" "$CHANGED_SCAN_TIMES" "$NOW")
  CHANGED_NEXT_SLOT_EPOCH=$(get_next_scheduled_epoch "$CHANGED_SCAN_DAYS" "$CHANGED_SCAN_TIMES" "$NOW")

  if [ "$NEXT_CHANGED_RETRY_EPOCH" -gt "$NOW" ]; then
    CHANGED_NEXT_WAKE_EPOCH="$NEXT_CHANGED_RETRY_EPOCH"
  elif [ "$CHANGED_LAST_SLOT_EPOCH" -gt "$LAST_CHANGED_EPOCH" ]; then
    CHANGED_DUE=1
    CHANGED_NEXT_WAKE_EPOCH="$NOW"
  else
    CHANGED_NEXT_WAKE_EPOCH="$CHANGED_NEXT_SLOT_EPOCH"
  fi
}

evaluate_full_trigger() {
  FULL_DUE=0
  FULL_MANUAL_REQUEST=0

  if [ "$MANUAL_FULL_REQUEST" -eq 1 ]; then
    if [ "$NEXT_FULL_RETRY_EPOCH" -gt "$NOW" ]; then
      FULL_NEXT_WAKE_EPOCH="$NEXT_FULL_RETRY_EPOCH"
    else
      FULL_DUE=1
      FULL_MANUAL_REQUEST=1
      FULL_NEXT_WAKE_EPOCH="$NOW"
    fi
    return 0
  fi

  if [ "$FORCE" -eq 1 ]; then
    if [ "$NEXT_FULL_RETRY_EPOCH" -gt "$NOW" ]; then
      FULL_NEXT_WAKE_EPOCH="$NEXT_FULL_RETRY_EPOCH"
    else
      FULL_DUE=1
      FULL_NEXT_WAKE_EPOCH="$NOW"
    fi
    return 0
  fi

  FULL_LAST_SLOT_EPOCH=$(get_last_scheduled_epoch "$FULL_SCAN_DAYS" "$FULL_SCAN_TIMES" "$NOW")
  FULL_NEXT_SLOT_EPOCH=$(get_next_scheduled_epoch "$FULL_SCAN_DAYS" "$FULL_SCAN_TIMES" "$NOW")

  if [ "$NEXT_FULL_RETRY_EPOCH" -gt "$NOW" ]; then
    FULL_NEXT_WAKE_EPOCH="$NEXT_FULL_RETRY_EPOCH"
  elif [ "$FULL_LAST_SLOT_EPOCH" -gt "$LAST_FULL_EPOCH" ]; then
    FULL_DUE=1
    FULL_NEXT_WAKE_EPOCH="$NOW"
  else
    FULL_NEXT_WAKE_EPOCH="$FULL_NEXT_SLOT_EPOCH"
  fi
}

sleep_until_epoch() {
  TARGET_EPOCH="$1"
  CURRENT_EPOCH="$2"

  SLEEP_SECONDS=$((TARGET_EPOCH - CURRENT_EPOCH))
  [ "$SLEEP_SECONDS" -lt 1 ] && SLEEP_SECONDS=1
  echo "Sleeping ${SLEEP_SECONDS}s..." | tee -a "$SCANLOG"

  while [ "$CURRENT_EPOCH" -lt "$TARGET_EPOCH" ]; do
    if manual_full_request_should_wake "$CURRENT_EPOCH"; then
      echo "[MANUAL] On-demand full scan request detected during sleep; waking early." | tee -a "$SCANLOG"
      return 0
    fi

    if force_full_should_wake "$CURRENT_EPOCH"; then
      echo "[FORCE] Force-full flag detected during sleep; waking early." | tee -a "$SCANLOG"
      return 0
    fi

    if manual_changed_request_should_wake "$CURRENT_EPOCH"; then
      echo "[MANUAL] On-demand changed scan request detected during sleep; waking early." | tee -a "$SCANLOG"
      return 0
    fi

    REMAINING_SECONDS=$((TARGET_EPOCH - CURRENT_EPOCH))
    [ "$REMAINING_SECONDS" -lt 1 ] && return 0

    SLEEP_STEP=$(min_int "$REMAINING_SECONDS" "$FORCE_FULL_POLL_INTERVAL")
    sleep "$SLEEP_STEP"
    CURRENT_EPOCH=$(date +%s)
  done
}

check_scan_path_health() {
  LABEL="$1"
  SCAN_PATH="$2"

  if timeout "${PATH_CHECK_TIMEOUT}" sh -c '
    SCAN_PATH="$1"
    MARKER="$2"

    test -d "$SCAN_PATH" || exit 10
    test -r "$SCAN_PATH" || exit 11
    find "$SCAN_PATH" -mindepth 0 -maxdepth 0 -print >/dev/null 2>&1 || exit 12

    if [ -n "$MARKER" ]; then
      test -e "$SCAN_PATH/$MARKER" || exit 13
    fi
  ' sh "$SCAN_PATH" "$SCAN_PATH_MARKER" >/dev/null 2>&1; then
    return 0
  fi

  RC=$?
  if [ "$RC" -eq 124 ] || [ "$RC" -eq 137 ]; then
    echo "[WARN] [$LABEL] Scan path health check timed out for $SCAN_PATH. The mount may be unavailable." | tee -a "$SCANLOG"
  else
    echo "[WARN] [$LABEL] Scan path health check failed for $SCAN_PATH (exit=${RC})." | tee -a "$SCANLOG"
  fi

  if [ -n "$SCAN_PATH_MARKER" ]; then
    echo "[WARN] [$LABEL] Expected marker '$SCAN_PATH_MARKER' under $SCAN_PATH." | tee -a "$SCANLOG"
  fi

  emit_scan_event --event-type mount_unavailable --severity warning --message "Scheduled scan root is unavailable" --source-path "$SCAN_PATH" --action-success false || true
  mark_scheduled_failure || true

  return 1
}

capture_scan_root_guard() {
  LABEL="$1"
  GUARD_FILE="$TMP_DIR/${LABEL}_scan_roots.json"
  if python3 /usr/local/bin/scan_root_guard.py capture --roots "$SCAN_PATHS" --marker "$SCAN_PATH_MARKER" --output "$GUARD_FILE"; then
    return 0
  fi
  echo "[WARN] [$LABEL] Could not capture scan-root identities before enumeration." | tee -a "$SCANLOG"
  emit_scan_event --event-type mount_unavailable --severity warning --message "Scheduled scan root identity could not be captured" --action-success false --scan-type "$LABEL" || true
  mark_scheduled_failure || true
  return 1
}

verify_scan_root_guard() {
  LABEL="$1"
  GUARD_FILE="$TMP_DIR/${LABEL}_scan_roots.json"
  if python3 /usr/local/bin/scan_root_guard.py verify --input "$GUARD_FILE"; then
    return 0
  fi
  echo "[WARN] [$LABEL] A scan root or configured marker changed during the scan." | tee -a "$SCANLOG"
  emit_scan_event --event-type mount_unavailable --severity warning --message "Scheduled scan root or marker changed during scan" --action-success false --scan-type "$LABEL" || true
  mark_scheduled_failure || true
  return 1
}

append_filtered_scan_list() {
  RAW_LIST_FILE="$1"
  LIST_FILE="$2"
  EXTRA_IGNORE_PATHS="$3"

  python3 /usr/local/bin/scan_list_filter.py --input "$RAW_LIST_FILE" --output "$LIST_FILE" --exclude-paths "$EXCLUDE_PATHS" --ignore-paths "$EXTRA_IGNORE_PATHS" --quarantine-path "$QUARANTINE_DIR"
}

append_scan_path_list() {
  LABEL="$1"
  SCAN_PATH="$2"
  LIST_FILE="$3"
  REFERENCE_EPOCH="$4"
  EXTRA_IGNORE_PATHS="$5"
  RAW_LIST_FILE="$TMP_DIR/${LABEL}_raw_list.nul"
  REFERENCE_FILE="$TMP_DIR/${LABEL}_reference.timestamp"

  : > "$RAW_LIST_FILE"

  if [ "$LABEL" = "CHANGED" ]; then
    touch -d "@${REFERENCE_EPOCH}" "$REFERENCE_FILE"
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$SCAN_PATH" -type f \( -newer "$REFERENCE_FILE" -o -cnewer "$REFERENCE_FILE" \) -print0 > "$RAW_LIST_FILE" 2>>"$SCANLOG"; then
      if ! append_filtered_scan_list "$RAW_LIST_FILE" "$LIST_FILE" "$EXTRA_IGNORE_PATHS"; then
        rm -f -- "$RAW_LIST_FILE" "$REFERENCE_FILE"
        return 1
      fi
      rm -f "$RAW_LIST_FILE"
      rm -f "$REFERENCE_FILE"
      return 0
    fi
  else
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$SCAN_PATH" -type f -print0 > "$RAW_LIST_FILE" 2>>"$SCANLOG"; then
      if ! append_filtered_scan_list "$RAW_LIST_FILE" "$LIST_FILE" "$EXTRA_IGNORE_PATHS"; then
        rm -f -- "$RAW_LIST_FILE"
        return 1
      fi
      rm -f "$RAW_LIST_FILE"
      return 0
    fi
  fi

  RC=$?
  rm -f "$RAW_LIST_FILE"
  rm -f "$REFERENCE_FILE"
  if [ "$RC" -eq 124 ] || [ "$RC" -eq 137 ]; then
    echo "[WARN] [$LABEL] Timed out while enumerating files under $SCAN_PATH. The mount may be unavailable." | tee -a "$SCANLOG"
    return 2
  fi

  echo "[WARN] [$LABEL] Failed enumerating files under $SCAN_PATH (exit=${RC})." | tee -a "$SCANLOG"
  return 1
}

build_scan_list() {
  LABEL="$1"
  LIST_FILE="$2"
  REFERENCE_EPOCH="$3"
  EXTRA_IGNORE_PATHS="${4:-}"
  PATH_COUNT=0
  OLD_IFS="$IFS"

  capture_scan_root_guard "$LABEL" || return 2
  : > "$LIST_FILE"

  IFS=':'
  set -- $SCAN_PATHS
  IFS="$OLD_IFS"

  for SCAN_PATH do
    PATH_COUNT=$((PATH_COUNT + 1))

    if check_scan_path_health "$LABEL" "$SCAN_PATH"; then
      :
    else
      return 2
    fi

    if append_scan_path_list "$LABEL" "$SCAN_PATH" "$LIST_FILE" "$REFERENCE_EPOCH" "$EXTRA_IGNORE_PATHS"; then
      :
    else
      RC=$?
      [ "$RC" -eq 2 ] && return 2
      return 1
    fi
  done

  echo "[$LABEL] Built file list from ${PATH_COUNT} scan path(s)." | tee -a "$SCANLOG"
}

append_target_scan_path_list() {
  LABEL="$1"
  TARGET_PATH="$2"
  LIST_FILE="$3"
  REFERENCE_EPOCH="$4"
  EXTRA_IGNORE_PATHS="$5"
  RAW_LIST_FILE="$TMP_DIR/${LABEL}_target_raw_list.nul"
  REFERENCE_FILE="$TMP_DIR/${LABEL}_target_reference.timestamp"

  if [ ! -e "$TARGET_PATH" ]; then
    echo "[WARN] [$LABEL] Requested target no longer exists: $TARGET_PATH" | tee -a "$SCANLOG"
    return 0
  fi

  : > "$RAW_LIST_FILE"

  if [ "$LABEL" = "CHANGED" ]; then
    touch -d "@${REFERENCE_EPOCH}" "$REFERENCE_FILE"
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$TARGET_PATH" -type f \( -newer "$REFERENCE_FILE" -o -cnewer "$REFERENCE_FILE" \) -print0 > "$RAW_LIST_FILE" 2>>"$SCANLOG"; then
      if ! append_filtered_scan_list "$RAW_LIST_FILE" "$LIST_FILE" "$EXTRA_IGNORE_PATHS"; then
        rm -f -- "$RAW_LIST_FILE" "$REFERENCE_FILE"
        return 1
      fi
      rm -f "$RAW_LIST_FILE" "$REFERENCE_FILE"
      return 0
    fi
  else
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$TARGET_PATH" -type f -print0 > "$RAW_LIST_FILE" 2>>"$SCANLOG"; then
      if ! append_filtered_scan_list "$RAW_LIST_FILE" "$LIST_FILE" "$EXTRA_IGNORE_PATHS"; then
        rm -f -- "$RAW_LIST_FILE"
        return 1
      fi
      rm -f "$RAW_LIST_FILE"
      return 0
    fi
  fi

  RC=$?
  rm -f "$RAW_LIST_FILE" "$REFERENCE_FILE"
  if [ "$RC" -eq 124 ] || [ "$RC" -eq 137 ]; then
    echo "[WARN] [$LABEL] Timed out while enumerating requested target $TARGET_PATH. The mount may be unavailable." | tee -a "$SCANLOG"
    return 2
  fi

  echo "[WARN] [$LABEL] Failed enumerating requested target $TARGET_PATH (exit=${RC})." | tee -a "$SCANLOG"
  return 1
}

build_targeted_scan_list() {
  LABEL="$1"
  LIST_FILE="$2"
  REFERENCE_EPOCH="$3"
  TARGET_PATHS="$4"
  EXTRA_IGNORE_PATHS="${5:-}"
  TARGET_COUNT=0
  OLD_IFS="$IFS"

  capture_scan_root_guard "$LABEL" || return 2
  : > "$LIST_FILE"

  IFS=':'
  set -- $TARGET_PATHS
  IFS="$OLD_IFS"

  for TARGET_PATH do
    [ -n "$TARGET_PATH" ] || continue

    SCAN_ROOT=$(get_scan_root_for_path "$TARGET_PATH")
    if [ -z "$SCAN_ROOT" ]; then
      echo "[WARN] [$LABEL] Requested target is outside configured scan roots and will not be scanned: $TARGET_PATH" | tee -a "$SCANLOG"
      return 1
    fi

    if check_scan_path_health "$LABEL" "$SCAN_ROOT"; then
      :
    else
      return 2
    fi

    if append_target_scan_path_list "$LABEL" "$TARGET_PATH" "$LIST_FILE" "$REFERENCE_EPOCH" "$EXTRA_IGNORE_PATHS"; then
      TARGET_COUNT=$((TARGET_COUNT + 1))
    else
      RC=$?
      [ "$RC" -eq 2 ] && return 2
      return 1
    fi
  done

  echo "[$LABEL] Built file list from ${TARGET_COUNT} requested target path(s)." | tee -a "$SCANLOG"
}

run_scan_list() {
  LIST_FILE="$1"
  LABEL="$2"
  CONFIGURED_PARALLEL_JOBS="$3"
  REQUESTED_CHUNK_SIZE="$4"
  PROGRESS_STEPS="$5"

  RESULTS_FILE="$TMP_DIR/${LABEL}_results.jsonl"
  if python3 /usr/local/bin/clamd_session_scan.py \
    --socket "$CLAMD_SOCKET" \
    --list-file "$LIST_FILE" \
    --results-file "$RESULTS_FILE" \
    --quarantine-dir "$QUARANTINE_DIR" \
    --configured-workers "$CONFIGURED_PARALLEL_JOBS" \
    --requested-progress-interval "$REQUESTED_CHUNK_SIZE" \
    --progress-steps "$PROGRESS_STEPS" \
    --max-files "$MAX_SCHEDULED_FILES" \
    --scanlog-max-bytes "$SCANLOG_MAX_BYTES" \
    --scanlog-rotations "$SCANLOG_ROTATIONS" \
    --label "$LABEL" \
    --scanlog "$SCANLOG" \
    --scan-paths "$SCAN_PATHS" \
    --event-dir "$EVENT_DIR" \
    --vanished-failure-count "$VANISHED_FILE_FAILURE_COUNT" \
    --vanished-failure-percent "$VANISHED_FILE_FAILURE_PERCENT" \
    --vanished-failure-minimum "$VANISHED_FILE_FAILURE_MINIMUM" \
    --native-max-bytes "$CLAMD_MAX_FILE_SIZE_BYTES" \
    --large-media-enabled "$LARGE_MEDIA_ENABLED" \
    --large-media-max-gib "$LARGE_MEDIA_MAX_FILE_GIB" \
    --large-media-window-mib "$LARGE_MEDIA_CHUNK_MIB" \
    --large-media-overlap-kib "$LARGE_MEDIA_OVERLAP_KIB" \
    --large-media-probe-timeout "$LARGE_MEDIA_PROBE_TIMEOUT_SECONDS" \
    --large-media-scan-timeout "$LARGE_MEDIA_SCAN_TIMEOUT_SECONDS" \
    --large-media-workers "$MAX_LARGE_MEDIA_WORKERS" \
    --ffprobe-binary "$FFPROBE_BINARY"; then
    if ensure_clamd_alive && verify_scan_root_guard "$LABEL"; then
      echo "[$LABEL] Completed successfully." | tee -a "$SCANLOG"
      return 0
    fi
  fi

  echo "[WARN] ${LABEL} scan incomplete. The structured results file is ${RESULTS_FILE}." | tee -a "$SCANLOG"
  mark_scheduled_failure || true
  emit_scan_event --event-type scan_failed --severity warning --message "Scheduled scan did not complete" --action-success false --scan-type "$LABEL" || true
  return 1
}

while true; do
  if ! ensure_clamd_alive; then
    exit 1
  fi

  exec 9>"$STATE_DIR/scan.lock"
  if ! flock -n 9; then
    release_lock
    echo "[LOCKED] Previous scan still running; sleeping 5m..." | tee -a "$SCANLOG"
    sleep 300
    continue
  fi

  NOW=$(date +%s)
  LAST_CHANGED_EPOCH=$(read_checkpoint changed)
  LAST_FULL_EPOCH=$(read_checkpoint full)

  FORCE=0
  MANUAL_FULL_REQUEST=0
  MANUAL_CHANGED_REQUEST=0
  if [ -f "$FORCE_FULL_FLAG" ]; then
    FORCE=1
    echo "[FORCE] Full scan requested (flag detected): $FORCE_FULL_FLAG" | tee -a "$SCANLOG"
  fi

  if load_manual_full_request; then
    MANUAL_FULL_REQUEST=1
    TARGET_DESC="$MANUAL_FULL_REQUEST_TARGET_PATHS"
    IGNORE_DESC="$MANUAL_FULL_REQUEST_IGNORE_PATHS"
    [ -n "$TARGET_DESC" ] || TARGET_DESC="$SCAN_PATHS"
    [ -n "$IGNORE_DESC" ] || IGNORE_DESC="<none>"
    echo "[MANUAL] On-demand full scan requested (target_paths=${TARGET_DESC} ignore_paths=${IGNORE_DESC})" | tee -a "$SCANLOG"
  else
    RC=$?
    if [ "$RC" -eq 2 ] && [ -f "$MANUAL_FULL_REQUEST_FILE" ]; then
      echo "[WARN] [MANUAL] Ignoring invalid on-demand full scan request and deleting ${MANUAL_FULL_REQUEST_FILE}." | tee -a "$SCANLOG"
      rm -f -- "$MANUAL_FULL_REQUEST_FILE"
    fi
  fi

  if load_manual_changed_request; then
    MANUAL_CHANGED_REQUEST=1
    TARGET_DESC="$MANUAL_CHANGED_REQUEST_TARGET_PATHS"
    IGNORE_DESC="$MANUAL_CHANGED_REQUEST_IGNORE_PATHS"
    [ -n "$TARGET_DESC" ] || TARGET_DESC="$SCAN_PATHS"
    [ -n "$IGNORE_DESC" ] || IGNORE_DESC="<none>"
    echo "[MANUAL] On-demand changed scan requested (mode=${MANUAL_CHANGED_REQUEST_MODE} reference_epoch=${MANUAL_CHANGED_REFERENCE_EPOCH} lookback_seconds=${MANUAL_CHANGED_LOOKBACK_SECONDS} target_paths=${TARGET_DESC} ignore_paths=${IGNORE_DESC})" | tee -a "$SCANLOG"
  else
    RC=$?
    if [ "$RC" -eq 2 ] && [ -f "$MANUAL_CHANGED_REQUEST_FILE" ]; then
      echo "[WARN] [MANUAL] Ignoring invalid on-demand changed scan request and deleting ${MANUAL_CHANGED_REQUEST_FILE}." | tee -a "$SCANLOG"
      rm -f -- "$MANUAL_CHANGED_REQUEST_FILE"
    fi
  fi

  evaluate_changed_trigger
  evaluate_full_trigger

  CYCLE_ABORT=0

  if [ "$FULL_DUE" -eq 0 ] && [ "$CHANGED_DUE" -eq 0 ]; then
    NEXT_WAKE_EPOCH="$CHANGED_NEXT_WAKE_EPOCH"
    [ "$FULL_NEXT_WAKE_EPOCH" -lt "$NEXT_WAKE_EPOCH" ] && NEXT_WAKE_EPOCH="$FULL_NEXT_WAKE_EPOCH"
    release_lock
    echo "=== $(date) No scans due. Next wake at $(date -d "@$NEXT_WAKE_EPOCH") ===" | tee -a "$SCANLOG"
    sleep_until_epoch "$NEXT_WAKE_EPOCH" "$NOW"
    continue
  fi

  echo "=== $(date) Scan cycle starting (full_due=${FULL_DUE} changed_due=${CHANGED_DUE}) ===" | tee -a "$SCANLOG"

  if [ "$FULL_DUE" -eq 1 ]; then
    echo "=== FULL SCAN starting ===" | tee -a "$SCANLOG"

    FULL_LIST="$TMP_DIR/full_list.nul"
    FULL_SCAN_CUTOFF=$(date +%s)
    FULL_TARGET_PATHS="$SCAN_PATHS"
    FULL_IGNORE_PATHS=""
    FULL_ADVANCES_CHECKPOINTS=1
    FULL_BUILD_MODE="scan_paths"

    if [ "$FULL_MANUAL_REQUEST" -eq 1 ]; then
      [ -n "$MANUAL_FULL_REQUEST_TARGET_PATHS" ] && FULL_TARGET_PATHS="$MANUAL_FULL_REQUEST_TARGET_PATHS"
      FULL_IGNORE_PATHS="$MANUAL_FULL_REQUEST_IGNORE_PATHS"
      if [ -n "$MANUAL_FULL_REQUEST_TARGET_PATHS" ]; then
        FULL_BUILD_MODE="target_paths"
      fi
      if [ -n "$MANUAL_FULL_REQUEST_TARGET_PATHS" ] || [ -n "$MANUAL_FULL_REQUEST_IGNORE_PATHS" ]; then
        FULL_ADVANCES_CHECKPOINTS=0
      fi
      IGNORE_DESC="$FULL_IGNORE_PATHS"
      [ -n "$IGNORE_DESC" ] || IGNORE_DESC="<none>"
      echo "[MANUAL] Starting on-demand full scan over target paths ${FULL_TARGET_PATHS} with ignore paths ${IGNORE_DESC}." | tee -a "$SCANLOG"
    fi

    if [ "$FULL_BUILD_MODE" = "target_paths" ]; then
      FULL_BUILD_OK=0
      if build_targeted_scan_list "FULL" "$FULL_LIST" 0 "$FULL_TARGET_PATHS" "$FULL_IGNORE_PATHS"; then
        FULL_BUILD_OK=1
      else
        RC=$?
      fi
    else
      FULL_BUILD_OK=0
      if build_scan_list "FULL" "$FULL_LIST" 0 "$FULL_IGNORE_PATHS"; then
        FULL_BUILD_OK=1
      else
        RC=$?
      fi
    fi

    if [ "$FULL_BUILD_OK" -eq 1 ]; then
      if run_scan_list "$FULL_LIST" "FULL" "$FULL_SCAN_PARALLEL_JOBS" "$FULL_CHUNK_SIZE" "$FULL_PROGRESS_STEPS"; then
        if [ "$FULL_ADVANCES_CHECKPOINTS" -eq 1 ]; then
          FULL_SUCCESS_EPOCH=$(date +%s)
          NEW_CHANGED_EPOCH="$LAST_CHANGED_EPOCH"
          if [ "$FULL_SCAN_CUTOFF" -gt "$LAST_CHANGED_EPOCH" ]; then
            NEW_CHANGED_EPOCH="$FULL_SCAN_CUTOFF"
          fi
          if ! update_checkpoints "$FULL_SUCCESS_EPOCH" "$NEW_CHANGED_EPOCH"; then
            echo "[ERROR] Atomic checkpoint update failed after the full scan; stopping so it will be retried." | tee -a "$SCANLOG"
            mark_scheduled_failure || true
            emit_scan_event --event-type scan_failed --severity warning --message "Scheduled full-scan checkpoint could not be persisted" --action-success false --scan-type FULL || true
            exit 1
          fi
          LAST_FULL_EPOCH="$FULL_SUCCESS_EPOCH"
          if [ "$NEW_CHANGED_EPOCH" -gt "$LAST_CHANGED_EPOCH" ]; then
            LAST_CHANGED_EPOCH="$NEW_CHANGED_EPOCH"
            echo "[CHANGED] Updated changed-file checkpoint to successful full-scan cutoff $(date -d "@$FULL_SCAN_CUTOFF")." | tee -a "$SCANLOG"
          fi
          NEXT_CHANGED_RETRY_EPOCH=0
        else
          echo "[FULL] Scoped on-demand full scan completed; scheduled full and changed checkpoints were left unchanged." | tee -a "$SCANLOG"
        fi
        emit_scheduled_recovery "Scheduled scanning recovered after a successful full scan" || true
        NEXT_FULL_RETRY_EPOCH=0
        echo "=== FULL SCAN finished ===" | tee -a "$SCANLOG"

        if [ "$FULL_ADVANCES_CHECKPOINTS" -eq 1 ] && [ "$CHANGED_DUE" -eq 1 ] && [ "$CHANGED_MANUAL_REQUEST" -eq 0 ]; then
          CHANGED_DUE=0
          echo "[CHANGED] Skipping changed-files scan because the successful full scan already covered files through $(date -d "@$FULL_SCAN_CUTOFF")." | tee -a "$SCANLOG"
        fi

        if [ "$FULL_MANUAL_REQUEST" -eq 1 ] && [ -f "$MANUAL_FULL_REQUEST_FILE" ]; then
          rm -f -- "$MANUAL_FULL_REQUEST_FILE"
          echo "[MANUAL] On-demand full scan finished successfully; request consumed." | tee -a "$SCANLOG"
        fi

        if [ "$FORCE" -eq 1 ] && [ -f "$FORCE_FULL_FLAG" ] && [ "$FULL_ADVANCES_CHECKPOINTS" -eq 1 ]; then
          rm -f -- "$FORCE_FULL_FLAG"
          echo "[FORCE] Flag consumed (deleted): $FORCE_FULL_FLAG" | tee -a "$SCANLOG"
        fi
      else
        NEXT_FULL_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        if [ "$FULL_MANUAL_REQUEST" -eq 1 ]; then
          echo "[WARN] On-demand full scan did not complete. Keeping the manual request and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        else
          echo "[WARN] Full scan did not complete. Retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        fi
      fi
    else
      mark_scheduled_failure || true
      emit_scan_event --event-type scan_failed --severity warning --message "Scheduled full-scan file enumeration did not complete" --action-success false --scan-type FULL || true
      if [ "$RC" -eq 2 ]; then
        CYCLE_ABORT=1
        NEXT_FULL_RETRY_EPOCH=$(( $(date +%s) + PATH_UNAVAILABLE_RETRY_INTERVAL ))
        NEXT_CHANGED_RETRY_EPOCH="$NEXT_FULL_RETRY_EPOCH"
        if [ "$FULL_MANUAL_REQUEST" -eq 1 ]; then
          echo "[WARN] On-demand full scan paused because a scan path is unavailable. Keeping the manual request and retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        else
          echo "[WARN] Full scan paused because a scan path is unavailable. Retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        fi
      else
        NEXT_FULL_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        if [ "$FULL_MANUAL_REQUEST" -eq 1 ]; then
          echo "[WARN] On-demand full scan file-list build failed. Keeping the manual request and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        else
          echo "[WARN] Full scan file-list build failed. Retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        fi
      fi
    fi
  fi

  if [ "$CHANGED_DUE" -eq 1 ] && [ "$CYCLE_ABORT" -eq 0 ]; then
    echo "=== CHANGED-FILES scan starting ===" | tee -a "$SCANLOG"
    CHANGED_LIST="$TMP_DIR/changed_list.nul"
    CHANGED_SCAN_CUTOFF=$(date +%s)
    CHANGED_REFERENCE_EPOCH="$LAST_CHANGED_EPOCH"
    CHANGED_TARGET_PATHS="$SCAN_PATHS"
    CHANGED_IGNORE_PATHS=""
    CHANGED_BUILD_MODE="scan_paths"

    if [ "$CHANGED_MANUAL_REQUEST" -eq 1 ]; then
      CHANGED_REFERENCE_EPOCH="$MANUAL_CHANGED_REFERENCE_EPOCH"
      [ -n "$MANUAL_CHANGED_REQUEST_TARGET_PATHS" ] && CHANGED_TARGET_PATHS="$MANUAL_CHANGED_REQUEST_TARGET_PATHS"
      CHANGED_IGNORE_PATHS="$MANUAL_CHANGED_REQUEST_IGNORE_PATHS"
      if [ -n "$MANUAL_CHANGED_REQUEST_TARGET_PATHS" ]; then
        CHANGED_BUILD_MODE="target_paths"
      fi
      IGNORE_DESC="$CHANGED_IGNORE_PATHS"
      [ -n "$IGNORE_DESC" ] || IGNORE_DESC="<none>"
      echo "[MANUAL] Starting on-demand changed scan from reference epoch ${CHANGED_REFERENCE_EPOCH} over target paths ${CHANGED_TARGET_PATHS} with ignore paths ${IGNORE_DESC}." | tee -a "$SCANLOG"
    fi

    if [ "$CHANGED_BUILD_MODE" = "target_paths" ]; then
      CHANGED_BUILD_OK=0
      if build_targeted_scan_list "CHANGED" "$CHANGED_LIST" "$CHANGED_REFERENCE_EPOCH" "$CHANGED_TARGET_PATHS" "$CHANGED_IGNORE_PATHS"; then
        CHANGED_BUILD_OK=1
      else
        RC=$?
      fi
    else
      CHANGED_BUILD_OK=0
      if build_scan_list "CHANGED" "$CHANGED_LIST" "$CHANGED_REFERENCE_EPOCH" "$CHANGED_IGNORE_PATHS"; then
        CHANGED_BUILD_OK=1
      else
        RC=$?
      fi
    fi

    if [ "$CHANGED_BUILD_OK" -eq 1 ]; then
      if run_scan_list "$CHANGED_LIST" "CHANGED" "$CHANGED_SCAN_PARALLEL_JOBS" "$CHANGED_CHUNK_SIZE" "$CHANGED_PROGRESS_STEPS"; then
        NEXT_CHANGED_RETRY_EPOCH=0
        if [ "$CHANGED_MANUAL_REQUEST" -eq 1 ]; then
          rm -f -- "$MANUAL_CHANGED_REQUEST_FILE"
          echo "[MANUAL] On-demand changed scan finished successfully; request consumed." | tee -a "$SCANLOG"
        else
          if ! update_checkpoints "$LAST_FULL_EPOCH" "$CHANGED_SCAN_CUTOFF"; then
            echo "[ERROR] Atomic checkpoint update failed after the changed-files scan; stopping so it will be retried." | tee -a "$SCANLOG"
            mark_scheduled_failure || true
            emit_scan_event --event-type scan_failed --severity warning --message "Changed-file scan checkpoint could not be persisted" --action-success false --scan-type CHANGED || true
            exit 1
          fi
          LAST_CHANGED_EPOCH="$CHANGED_SCAN_CUTOFF"
        fi
        emit_scheduled_recovery "Scheduled scanning recovered after a successful changed-file scan" || true
      else
        NEXT_CHANGED_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        if [ "$CHANGED_MANUAL_REQUEST" -eq 1 ]; then
          echo "[WARN] On-demand changed scan did not complete. Keeping the manual request and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        else
          echo "[WARN] Changed-files scan did not complete. Keeping previous changed-scan checkpoint and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        fi
      fi
    else
      mark_scheduled_failure || true
      emit_scan_event --event-type scan_failed --severity warning --message "Scheduled changed-file enumeration did not complete" --action-success false --scan-type CHANGED || true
      if [ "$RC" -eq 2 ]; then
        CYCLE_ABORT=1
        NEXT_CHANGED_RETRY_EPOCH=$(( $(date +%s) + PATH_UNAVAILABLE_RETRY_INTERVAL ))
        NEXT_FULL_RETRY_EPOCH="$NEXT_CHANGED_RETRY_EPOCH"
        if [ "$CHANGED_MANUAL_REQUEST" -eq 1 ]; then
          echo "[WARN] On-demand changed scan paused because a scan path is unavailable. Keeping the manual request and retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        else
          echo "[WARN] Changed-files scan paused because a scan path is unavailable. Retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        fi
      else
        NEXT_CHANGED_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        if [ "$CHANGED_MANUAL_REQUEST" -eq 1 ]; then
          echo "[WARN] On-demand changed scan file-list build failed. Keeping the manual request and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        else
          echo "[WARN] Changed-files scan file-list build failed. Keeping previous changed-scan checkpoint and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
        fi
      fi
    fi
  fi

  if [ "$CYCLE_ABORT" -eq 1 ]; then
    echo "=== Scan cycle paused due to unavailable scan path ===" | tee -a "$SCANLOG"
  else
    echo "=== Scan cycle finished ===" | tee -a "$SCANLOG"
  fi

  release_lock

  END=$(date +%s)
  NOW="$END"
  LAST_CHANGED_EPOCH=$(read_checkpoint changed)
  LAST_FULL_EPOCH=$(read_checkpoint full)
  FORCE=0
  MANUAL_FULL_REQUEST=0
  MANUAL_CHANGED_REQUEST=0
  [ -f "$FORCE_FULL_FLAG" ] && FORCE=1
  if load_manual_full_request; then
    MANUAL_FULL_REQUEST=1
  else
    RC=$?
    if [ "$RC" -eq 2 ] && [ -f "$MANUAL_FULL_REQUEST_FILE" ]; then
      echo "[WARN] [MANUAL] Ignoring invalid on-demand full scan request and deleting ${MANUAL_FULL_REQUEST_FILE}." | tee -a "$SCANLOG"
      rm -f -- "$MANUAL_FULL_REQUEST_FILE"
    fi
  fi
  if load_manual_changed_request; then
    MANUAL_CHANGED_REQUEST=1
  else
    RC=$?
    if [ "$RC" -eq 2 ] && [ -f "$MANUAL_CHANGED_REQUEST_FILE" ]; then
      echo "[WARN] [MANUAL] Ignoring invalid on-demand changed scan request and deleting ${MANUAL_CHANGED_REQUEST_FILE}." | tee -a "$SCANLOG"
      rm -f -- "$MANUAL_CHANGED_REQUEST_FILE"
    fi
  fi
  evaluate_changed_trigger
  evaluate_full_trigger
  NEXT_WAKE_EPOCH="$CHANGED_NEXT_WAKE_EPOCH"
  [ "$FULL_NEXT_WAKE_EPOCH" -lt "$NEXT_WAKE_EPOCH" ] && NEXT_WAKE_EPOCH="$FULL_NEXT_WAKE_EPOCH"
  sleep_until_epoch "$NEXT_WAKE_EPOCH" "$END"
done
