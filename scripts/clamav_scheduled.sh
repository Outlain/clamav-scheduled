#!/bin/sh
set -eu

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
: "${STATE_DIR:=/state}"
: "${TMP_DIR:=/tmp/clamav}"
: "${PATH_CHECK_TIMEOUT:=10}"
: "${PATH_ENUMERATION_TIMEOUT:=300}"
: "${PATH_UNAVAILABLE_RETRY_INTERVAL:=300}"
: "${SCAN_PATH_MARKER:=}"

reject_deprecated_env() {
  VAR_NAME="$1"
  MESSAGE="$2"
  eval "VAR_IS_SET=\${${VAR_NAME}+set}"

  if [ -n "$VAR_IS_SET" ]; then
    echo "[ERROR] ${VAR_NAME} is no longer supported. ${MESSAGE}" >&2
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
  IFS=':'
  set -- $VALUE
  IFS="$OLD_IFS"

  for PATH_ENTRY do
    case "$PATH_ENTRY" in
      /*)
        ;;
      *)
        echo "[ERROR] ${NAME} entries must be absolute paths (got: ${PATH_ENTRY})" >&2
        exit 1
        ;;
    esac

    NORMALIZED_ENTRY="$PATH_ENTRY"
    while [ "$NORMALIZED_ENTRY" != "/" ] && [ "${NORMALIZED_ENTRY%/}" != "$NORMALIZED_ENTRY" ]; do
      NORMALIZED_ENTRY=${NORMALIZED_ENTRY%/}
    done

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
EXCLUDE_PATHS=$(normalize_absolute_path_list "EXCLUDE_PATHS" "$EXCLUDE_PATHS")
PRIMARY_SCAN_PATH=$(get_primary_scan_path "$SCAN_PATHS")
[ -n "$PRIMARY_SCAN_PATH" ] || {
  echo "[ERROR] Unable to determine a primary scan path from SCAN_PATHS=${SCAN_PATHS}" >&2
  exit 1
}

: "${QUARANTINE_DIR:=${PRIMARY_SCAN_PATH}/quarantine}"
: "${FORCE_FULL_FLAG:=${PRIMARY_SCAN_PATH}/.clamav_force_full_scan.flag}"

CHANGED_SCAN_TIMES=$(normalize_schedule_times "$CHANGED_SCAN_TIMES")
CHANGED_SCAN_DAYS=$(normalize_schedule_days "$CHANGED_SCAN_DAYS")
FULL_SCAN_TIMES=$(normalize_schedule_times "$FULL_SCAN_TIMES")
FULL_SCAN_DAYS=$(normalize_schedule_days "$FULL_SCAN_DAYS")

mkdir -p "$QUARANTINE_DIR" "$STATE_DIR" "$TMP_DIR" /var/log/clamav /var/lib/clamav

echo "=== Starting scheduled ClamAV scanner ===" | tee -a "$SCANLOG"
echo "TZ=$TZ MAXTHREADS=$MAXTHREADS FULL_SCAN_PARALLEL_JOBS=$FULL_SCAN_PARALLEL_JOBS CHANGED_SCAN_PARALLEL_JOBS=$CHANGED_SCAN_PARALLEL_JOBS FULL_CHUNK_SIZE=$FULL_CHUNK_SIZE CHANGED_CHUNK_SIZE=$CHANGED_CHUNK_SIZE FULL_PROGRESS_STEPS=$FULL_PROGRESS_STEPS CHANGED_PROGRESS_STEPS=$CHANGED_PROGRESS_STEPS CHANGED_SCAN_DAYS=$CHANGED_SCAN_DAYS CHANGED_SCAN_TIMES=$CHANGED_SCAN_TIMES FULL_SCAN_DAYS=$FULL_SCAN_DAYS FULL_SCAN_TIMES=$FULL_SCAN_TIMES SCAN_FAILURE_RETRY_INTERVAL=$SCAN_FAILURE_RETRY_INTERVAL SCAN_PATHS=$SCAN_PATHS EXCLUDE_PATHS=$EXCLUDE_PATHS QUARANTINE_DIR=$QUARANTINE_DIR STATE_DIR=$STATE_DIR PATH_CHECK_TIMEOUT=$PATH_CHECK_TIMEOUT PATH_ENUMERATION_TIMEOUT=$PATH_ENUMERATION_TIMEOUT PATH_UNAVAILABLE_RETRY_INTERVAL=$PATH_UNAVAILABLE_RETRY_INTERVAL SCAN_PATH_MARKER=$SCAN_PATH_MARKER" | tee -a "$SCANLOG"

validate_positive_int() {
  NAME="$1"
  VALUE="$2"

  case "$VALUE" in
    ''|*[!0-9]*)
      echo "[ERROR] ${NAME} must be a positive integer (got: ${VALUE})" | tee -a "$SCANLOG"
      exit 1
      ;;
  esac

  if [ "$VALUE" -le 0 ]; then
    echo "[ERROR] ${NAME} must be greater than 0 (got: ${VALUE})" | tee -a "$SCANLOG"
    exit 1
  fi
}

validate_nonnegative_int() {
  NAME="$1"
  VALUE="$2"

  case "$VALUE" in
    ''|*[!0-9]*)
      echo "[ERROR] ${NAME} must be a non-negative integer (got: ${VALUE})" | tee -a "$SCANLOG"
      exit 1
      ;;
  esac
}

validate_positive_int "MAXTHREADS" "$MAXTHREADS"
validate_positive_int "FULL_SCAN_PARALLEL_JOBS" "$FULL_SCAN_PARALLEL_JOBS"
validate_positive_int "CHANGED_SCAN_PARALLEL_JOBS" "$CHANGED_SCAN_PARALLEL_JOBS"
validate_positive_int "FULL_PROGRESS_STEPS" "$FULL_PROGRESS_STEPS"
validate_positive_int "CHANGED_PROGRESS_STEPS" "$CHANGED_PROGRESS_STEPS"
validate_positive_int "SCAN_FAILURE_RETRY_INTERVAL" "$SCAN_FAILURE_RETRY_INTERVAL"
validate_positive_int "PATH_CHECK_TIMEOUT" "$PATH_CHECK_TIMEOUT"
validate_positive_int "PATH_ENUMERATION_TIMEOUT" "$PATH_ENUMERATION_TIMEOUT"
validate_positive_int "PATH_UNAVAILABLE_RETRY_INTERVAL" "$PATH_UNAVAILABLE_RETRY_INTERVAL"
validate_nonnegative_int "FULL_CHUNK_SIZE" "$FULL_CHUNK_SIZE"
validate_nonnegative_int "CHANGED_CHUNK_SIZE" "$CHANGED_CHUNK_SIZE"

if [ -z "$CHANGED_SCAN_TIMES" ]; then
  echo "[ERROR] CHANGED_SCAN_TIMES must contain one or more HH:MM values." | tee -a "$SCANLOG"
  exit 1
fi

if [ -z "$FULL_SCAN_TIMES" ]; then
  echo "[ERROR] FULL_SCAN_TIMES must contain one or more HH:MM values." | tee -a "$SCANLOG"
  exit 1
fi

cat > /etc/clamav/clamd.conf <<EOF2
DatabaseDirectory /var/lib/clamav
LocalSocket /tmp/clamd.sock
PidFile /tmp/clamd.pid
LogFile /tmp/clamd.log
LogTime yes
Foreground yes

MaxThreads ${MAXTHREADS}
MaxQueue 200
ReadTimeout 900
CommandReadTimeout 900
EOF2

echo "Starting clamd with MaxThreads=${MAXTHREADS}..." | tee -a "$SCANLOG"
clamd -c /etc/clamav/clamd.conf >/tmp/clamd.out 2>&1 &
CLAMD_PID=$!

release_lock() {
  flock -u 9 2>/dev/null || true
  exec 9>&- 2>/dev/null || true
}

cleanup() {
  release_lock
  echo "Stopping clamd..." | tee -a "$SCANLOG"
  kill "$CLAMD_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

i=0
while [ ! -S /tmp/clamd.sock ] && [ $i -lt 60 ]; do
  i=$((i+1))
  sleep 1
done

if [ ! -S /tmp/clamd.sock ]; then
  echo "[ERROR] clamd socket never appeared. Last /tmp/clamd.out:" | tee -a "$SCANLOG"
  tail -n 200 /tmp/clamd.out 2>/dev/null | tee -a "$SCANLOG" || true
  exit 1
fi

echo "readytest" > /tmp/clamav_readytest.txt
i=0
while [ $i -lt 60 ]; do
  if clamdscan --fdpass --no-summary /tmp/clamav_readytest.txt >/dev/null 2>&1; then
    echo "clamd ready." | tee -a "$SCANLOG"
    break
  fi
  i=$((i+1))
  sleep 1
done

if [ $i -ge 60 ]; then
  echo "[ERROR] clamd started but never accepted scans. Last /tmp/clamd.out:" | tee -a "$SCANLOG"
  tail -n 200 /tmp/clamd.out 2>/dev/null | tee -a "$SCANLOG" || true
  exit 1
fi

LAST_CHANGED="$STATE_DIR/last_changed_scan_epoch"
LAST_FULL="$STATE_DIR/last_full_scan_epoch"
NEXT_CHANGED_RETRY_EPOCH=0
NEXT_FULL_RETRY_EPOCH=0

run_clamdscan_chunk() {
  CHUNK_FILE="$1"
  CHUNK_PARALLEL_JOBS="$2"

  xargs -d '\n' -a "$CHUNK_FILE" -r -n 1 -P "$CHUNK_PARALLEL_JOBS" \
    sh -c '
      QUARANTINE_DIR="$1"
      SCANLOG="$2"
      FILE_TO_SCAN="$3"

      if clamdscan --fdpass -i --move="$QUARANTINE_DIR" --no-summary -- "$FILE_TO_SCAN" >> "$SCANLOG" 2>&1; then
        exit 0
      fi

      RC=$?
      if [ "$RC" -eq 1 ]; then
        exit 0
      fi

      printf "[ERROR] clamdscan failed for %s (exit=%s)\n" "$FILE_TO_SCAN" "$RC" >> "$SCANLOG"
      exit 1
    ' sh "$QUARANTINE_DIR" "$SCANLOG"
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

get_chunk_size() {
  TOTAL="$1"
  REQUESTED_CHUNK_SIZE="$2"
  PROGRESS_STEPS="$3"
  PARALLEL_JOBS="$4"

  if [ "$REQUESTED_CHUNK_SIZE" -gt 0 ]; then
    echo "$REQUESTED_CHUNK_SIZE"
    return 0
  fi

  CHUNK_SIZE_AUTO=$(((TOTAL + PROGRESS_STEPS - 1) / PROGRESS_STEPS))
  [ "$CHUNK_SIZE_AUTO" -lt 1 ] && CHUNK_SIZE_AUTO=1
  [ "$CHUNK_SIZE_AUTO" -lt "$PARALLEL_JOBS" ] && CHUNK_SIZE_AUTO="$PARALLEL_JOBS"
  echo "$CHUNK_SIZE_AUTO"
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
  sleep "$SLEEP_SECONDS"
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

  return 1
}

path_is_excluded() {
  FILE_PATH="$1"
  OLD_IFS="$IFS"

  [ -n "$EXCLUDE_PATHS" ] || return 1

  IFS=':'
  set -- $EXCLUDE_PATHS
  IFS="$OLD_IFS"

  for EXCLUDED_PATH do
    case "$FILE_PATH" in
      "$EXCLUDED_PATH"|"$EXCLUDED_PATH"/*)
        return 0
        ;;
    esac
  done

  return 1
}

append_filtered_scan_list() {
  RAW_LIST_FILE="$1"
  LIST_FILE="$2"

  while IFS= read -r FILE_PATH || [ -n "$FILE_PATH" ]; do
    if path_is_excluded "$FILE_PATH"; then
      continue
    fi

    printf '%s\n' "$FILE_PATH" >> "$LIST_FILE"
  done < "$RAW_LIST_FILE"
}

append_scan_path_list() {
  LABEL="$1"
  SCAN_PATH="$2"
  LIST_FILE="$3"
  REFERENCE_EPOCH="$4"
  RAW_LIST_FILE="$TMP_DIR/${LABEL}_raw_list.txt"

  : > "$RAW_LIST_FILE"

  if [ "$LABEL" = "CHANGED" ]; then
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$SCAN_PATH" -type f -not -path "$QUARANTINE_DIR/*" -newermt "@${REFERENCE_EPOCH}" > "$RAW_LIST_FILE" 2>>"$SCANLOG"; then
      append_filtered_scan_list "$RAW_LIST_FILE" "$LIST_FILE"
      rm -f "$RAW_LIST_FILE"
      return 0
    fi
  else
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$SCAN_PATH" -type f -not -path "$QUARANTINE_DIR/*" > "$RAW_LIST_FILE" 2>>"$SCANLOG"; then
      append_filtered_scan_list "$RAW_LIST_FILE" "$LIST_FILE"
      rm -f "$RAW_LIST_FILE"
      return 0
    fi
  fi

  RC=$?
  rm -f "$RAW_LIST_FILE"
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
  PATH_COUNT=0
  OLD_IFS="$IFS"

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

    if append_scan_path_list "$LABEL" "$SCAN_PATH" "$LIST_FILE" "$REFERENCE_EPOCH"; then
      :
    else
      RC=$?
      [ "$RC" -eq 2 ] && return 2
      return 1
    fi
  done

  echo "[$LABEL] Built file list from ${PATH_COUNT} scan path(s)." | tee -a "$SCANLOG"
}

run_scan_list() {
  LIST_FILE="$1"
  LABEL="$2"
  CONFIGURED_PARALLEL_JOBS="$3"
  REQUESTED_CHUNK_SIZE="$4"
  PROGRESS_STEPS="$5"

  TOTAL=$(wc -l < "$LIST_FILE" | tr -d ' ')

  if [ "$TOTAL" -le 0 ]; then
    echo "[$LABEL] No files found to scan." | tee -a "$SCANLOG"
    return 0
  fi

  EFFECTIVE_PARALLEL_JOBS=$(min_int "$TOTAL" "$CONFIGURED_PARALLEL_JOBS")
  EFFECTIVE_CHUNK_SIZE=$(get_chunk_size "$TOTAL" "$REQUESTED_CHUNK_SIZE" "$PROGRESS_STEPS" "$EFFECTIVE_PARALLEL_JOBS")

  echo "[$LABEL] Scanning ${TOTAL} files with parallel_jobs=${EFFECTIVE_PARALLEL_JOBS} chunk_size=${EFFECTIVE_CHUNK_SIZE} progress_steps=${PROGRESS_STEPS}" | tee -a "$SCANLOG"

  CHUNKDIR="$TMP_DIR/${LABEL}_chunks"
  rm -rf "$CHUNKDIR"
  mkdir -p "$CHUNKDIR"

  split -l "$EFFECTIVE_CHUNK_SIZE" "$LIST_FILE" "$CHUNKDIR/chunk_"

  DONE=0
  START=$(date +%s)

  for CHUNK in "$CHUNKDIR"/chunk_*; do
    [ -f "$CHUNK" ] || continue
    C=$(wc -l < "$CHUNK" | tr -d ' ')

    if ! run_clamdscan_chunk "$CHUNK" "$EFFECTIVE_PARALLEL_JOBS"; then
      echo "[ERROR] clamdscan failed during ${LABEL} scan. Not counting this chunk." | tee -a "$SCANLOG"
      return 1
    fi

    DONE=$((DONE + C))
    NOW2=$(date +%s)
    ELAPSED=$((NOW2 - START))
    [ "$ELAPSED" -lt 1 ] && ELAPSED=1
    RATE=$((DONE / ELAPSED))
    PCT=$((DONE * 100 / TOTAL))

    echo "[$LABEL] Progress: ${PCT}% (${DONE}/${TOTAL}) ~${RATE} files/s" | tee -a "$SCANLOG"
  done

  if [ "$DONE" -eq "$TOTAL" ]; then
    echo "[$LABEL] Completed successfully." | tee -a "$SCANLOG"
    return 0
  fi

  echo "[WARN] ${LABEL} scan incomplete (${DONE}/${TOTAL})." | tee -a "$SCANLOG"
  return 1
}

while true; do
  exec 9>"$STATE_DIR/scan.lock"
  if ! flock -n 9; then
    release_lock
    echo "[LOCKED] Previous scan still running; sleeping 5m..." | tee -a "$SCANLOG"
    sleep 300
    continue
  fi

  NOW=$(date +%s)
  [ -f "$LAST_CHANGED" ] || echo 0 > "$LAST_CHANGED"
  [ -f "$LAST_FULL" ] || echo 0 > "$LAST_FULL"

  LAST_CHANGED_EPOCH=$(cat "$LAST_CHANGED" 2>/dev/null || echo 0)
  LAST_FULL_EPOCH=$(cat "$LAST_FULL" 2>/dev/null || echo 0)

  FORCE=0
  if [ -f "$FORCE_FULL_FLAG" ]; then
    FORCE=1
    echo "[FORCE] Full scan requested (flag detected): $FORCE_FULL_FLAG" | tee -a "$SCANLOG"
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

    FULL_LIST="$TMP_DIR/full_list.txt"

    if build_scan_list "FULL" "$FULL_LIST" 0; then
      if run_scan_list "$FULL_LIST" "FULL" "$FULL_SCAN_PARALLEL_JOBS" "$FULL_CHUNK_SIZE" "$FULL_PROGRESS_STEPS"; then
        date +%s > "$LAST_FULL" || true
        NEXT_FULL_RETRY_EPOCH=0
        echo "=== FULL SCAN finished ===" | tee -a "$SCANLOG"

        if [ "$FORCE" -eq 1 ] && [ -f "$FORCE_FULL_FLAG" ]; then
          rm -f -- "$FORCE_FULL_FLAG"
          echo "[FORCE] Flag consumed (deleted): $FORCE_FULL_FLAG" | tee -a "$SCANLOG"
        fi
      else
        NEXT_FULL_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        echo "[WARN] Full scan did not complete. Retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
      fi
    else
      RC=$?

      if [ "$RC" -eq 2 ]; then
        CYCLE_ABORT=1
        NEXT_FULL_RETRY_EPOCH=$(( $(date +%s) + PATH_UNAVAILABLE_RETRY_INTERVAL ))
        NEXT_CHANGED_RETRY_EPOCH="$NEXT_FULL_RETRY_EPOCH"
        echo "[WARN] Full scan paused because a scan path is unavailable. Retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
      else
        NEXT_FULL_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        echo "[WARN] Full scan file-list build failed. Retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
      fi
    fi
  fi

  if [ "$CHANGED_DUE" -eq 1 ] && [ "$CYCLE_ABORT" -eq 0 ]; then
    echo "=== CHANGED-FILES scan starting ===" | tee -a "$SCANLOG"
    CHANGED_LIST="$TMP_DIR/changed_list.txt"
    CHANGED_SCAN_CUTOFF=$(date +%s)

    if build_scan_list "CHANGED" "$CHANGED_LIST" "$LAST_CHANGED_EPOCH"; then
      if run_scan_list "$CHANGED_LIST" "CHANGED" "$CHANGED_SCAN_PARALLEL_JOBS" "$CHANGED_CHUNK_SIZE" "$CHANGED_PROGRESS_STEPS"; then
        echo "$CHANGED_SCAN_CUTOFF" > "$LAST_CHANGED" || true
        NEXT_CHANGED_RETRY_EPOCH=0
      else
        NEXT_CHANGED_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        echo "[WARN] Changed-files scan did not complete. Keeping previous changed-scan checkpoint and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
      fi
    else
      RC=$?

      if [ "$RC" -eq 2 ]; then
        CYCLE_ABORT=1
        NEXT_CHANGED_RETRY_EPOCH=$(( $(date +%s) + PATH_UNAVAILABLE_RETRY_INTERVAL ))
        NEXT_FULL_RETRY_EPOCH="$NEXT_CHANGED_RETRY_EPOCH"
        echo "[WARN] Changed-files scan paused because a scan path is unavailable. Retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
      else
        NEXT_CHANGED_RETRY_EPOCH=$(( $(date +%s) + SCAN_FAILURE_RETRY_INTERVAL ))
        echo "[WARN] Changed-files scan file-list build failed. Keeping previous changed-scan checkpoint and retrying after ${SCAN_FAILURE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
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
  LAST_CHANGED_EPOCH=$(cat "$LAST_CHANGED" 2>/dev/null || echo 0)
  LAST_FULL_EPOCH=$(cat "$LAST_FULL" 2>/dev/null || echo 0)
  FORCE=0
  [ -f "$FORCE_FULL_FLAG" ] && FORCE=1
  evaluate_changed_trigger
  evaluate_full_trigger
  NEXT_WAKE_EPOCH="$CHANGED_NEXT_WAKE_EPOCH"
  [ "$FULL_NEXT_WAKE_EPOCH" -lt "$NEXT_WAKE_EPOCH" ] && NEXT_WAKE_EPOCH="$FULL_NEXT_WAKE_EPOCH"
  sleep_until_epoch "$NEXT_WAKE_EPOCH" "$END"
done
