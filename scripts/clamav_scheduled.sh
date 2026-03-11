#!/bin/sh
set -eu

# Tunables from environment
: "${TZ:=UTC}"
: "${MAXTHREADS:=13}"
: "${PARALLEL_JOBS:=8}"
: "${FULL_SCAN_PARALLEL_JOBS:=${PARALLEL_JOBS}}"
: "${CHANGED_SCAN_PARALLEL_JOBS:=${PARALLEL_JOBS}}"
: "${CHUNK_SIZE:=0}"
: "${FULL_CHUNK_SIZE:=${CHUNK_SIZE}}"
: "${CHANGED_CHUNK_SIZE:=${CHUNK_SIZE}}"
: "${FULL_PROGRESS_STEPS:=100}"
: "${CHANGED_PROGRESS_STEPS:=25}"
: "${SCAN_INTERVAL:=3600}"
: "${CHANGED_SCAN_INTERVAL:=${SCAN_INTERVAL}}"
: "${FULL_SCAN_INTERVAL:=259200}"
: "${DOWNLOADS_DIR:=/downloads}"
: "${SCAN_PATHS:=${DOWNLOADS_DIR}}"
: "${STATE_DIR:=/state}"
: "${TMP_DIR:=/tmp/clamav}"
: "${PATH_CHECK_TIMEOUT:=10}"
: "${PATH_ENUMERATION_TIMEOUT:=300}"
: "${PATH_UNAVAILABLE_RETRY_INTERVAL:=300}"
: "${SCAN_PATH_MARKER:=}"

validate_scan_paths_config() {
  VALUE="$1"

  case "$VALUE" in
    ''|:*|*::|*:)
      echo "[ERROR] SCAN_PATHS must be a colon-separated list with no empty entries (got: ${VALUE})" >&2
      exit 1
      ;;
  esac
}

get_primary_scan_path() {
  PATH_LIST="$1"
  OLD_IFS="$IFS"
  IFS=':'
  set -- $PATH_LIST
  IFS="$OLD_IFS"
  printf '%s\n' "${1:-}"
}

: "${SCANLOG:=/var/log/clamav/clamav_scheduled.log}"

validate_scan_paths_config "$SCAN_PATHS"
PRIMARY_SCAN_PATH=$(get_primary_scan_path "$SCAN_PATHS")
[ -n "$PRIMARY_SCAN_PATH" ] || {
  echo "[ERROR] Unable to determine a primary scan path from SCAN_PATHS=${SCAN_PATHS}" >&2
  exit 1
}

: "${QUARANTINE_DIR:=${PRIMARY_SCAN_PATH}/quarantine}"
: "${FORCE_FULL_FLAG:=${PRIMARY_SCAN_PATH}/.clamav_force_full_scan.flag}"

mkdir -p "$QUARANTINE_DIR" "$STATE_DIR" "$TMP_DIR" /var/log/clamav /var/lib/clamav

echo "=== Starting scheduled ClamAV scanner ===" | tee -a "$SCANLOG"
echo "TZ=$TZ MAXTHREADS=$MAXTHREADS FULL_SCAN_PARALLEL_JOBS=$FULL_SCAN_PARALLEL_JOBS CHANGED_SCAN_PARALLEL_JOBS=$CHANGED_SCAN_PARALLEL_JOBS FULL_CHUNK_SIZE=$FULL_CHUNK_SIZE CHANGED_CHUNK_SIZE=$CHANGED_CHUNK_SIZE FULL_PROGRESS_STEPS=$FULL_PROGRESS_STEPS CHANGED_PROGRESS_STEPS=$CHANGED_PROGRESS_STEPS CHANGED_SCAN_INTERVAL=$CHANGED_SCAN_INTERVAL FULL_SCAN_INTERVAL=$FULL_SCAN_INTERVAL SCAN_PATHS=$SCAN_PATHS QUARANTINE_DIR=$QUARANTINE_DIR STATE_DIR=$STATE_DIR PATH_CHECK_TIMEOUT=$PATH_CHECK_TIMEOUT PATH_ENUMERATION_TIMEOUT=$PATH_ENUMERATION_TIMEOUT PATH_UNAVAILABLE_RETRY_INTERVAL=$PATH_UNAVAILABLE_RETRY_INTERVAL SCAN_PATH_MARKER=$SCAN_PATH_MARKER" | tee -a "$SCANLOG"

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
validate_positive_int "CHANGED_SCAN_INTERVAL" "$CHANGED_SCAN_INTERVAL"
validate_positive_int "FULL_SCAN_INTERVAL" "$FULL_SCAN_INTERVAL"
validate_positive_int "PATH_CHECK_TIMEOUT" "$PATH_CHECK_TIMEOUT"
validate_positive_int "PATH_ENUMERATION_TIMEOUT" "$PATH_ENUMERATION_TIMEOUT"
validate_positive_int "PATH_UNAVAILABLE_RETRY_INTERVAL" "$PATH_UNAVAILABLE_RETRY_INTERVAL"
validate_nonnegative_int "FULL_CHUNK_SIZE" "$FULL_CHUNK_SIZE"
validate_nonnegative_int "CHANGED_CHUNK_SIZE" "$CHANGED_CHUNK_SIZE"

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

append_scan_path_list() {
  LABEL="$1"
  SCAN_PATH="$2"
  LIST_FILE="$3"
  REFERENCE_EPOCH="$4"

  if [ "$LABEL" = "CHANGED" ]; then
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$SCAN_PATH" -type f -not -path "$QUARANTINE_DIR/*" -newermt "@${REFERENCE_EPOCH}" >> "$LIST_FILE" 2>>"$SCANLOG"; then
      return 0
    fi
  else
    if timeout "${PATH_ENUMERATION_TIMEOUT}" find "$SCAN_PATH" -type f -not -path "$QUARANTINE_DIR/*" >> "$LIST_FILE" 2>>"$SCANLOG"; then
      return 0
    fi
  fi

  RC=$?
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
  [ -f "$LAST_CHANGED" ] || echo $((NOW - CHANGED_SCAN_INTERVAL)) > "$LAST_CHANGED"
  [ -f "$LAST_FULL" ] || echo 0 > "$LAST_FULL"

  LAST_CHANGED_EPOCH=$(cat "$LAST_CHANGED" 2>/dev/null || echo 0)
  LAST_FULL_EPOCH=$(cat "$LAST_FULL" 2>/dev/null || echo 0)
  NEXT_CHANGED_SCHEDULE_EPOCH="$LAST_CHANGED_EPOCH"
  NEXT_FULL_SCHEDULE_EPOCH="$LAST_FULL_EPOCH"

  DO_CHANGED=0
  if [ $((NOW - LAST_CHANGED_EPOCH)) -ge "$CHANGED_SCAN_INTERVAL" ]; then
    DO_CHANGED=1
  fi

  FORCE=0
  if [ -f "$FORCE_FULL_FLAG" ]; then
    FORCE=1
    echo "[FORCE] Full scan requested (flag detected): $FORCE_FULL_FLAG" | tee -a "$SCANLOG"
  fi

  DO_FULL=0
  if [ "$FORCE" -eq 1 ] || [ $((NOW - LAST_FULL_EPOCH)) -ge "$FULL_SCAN_INTERVAL" ]; then
    DO_FULL=1
  fi

  CYCLE_ABORT=0
  CYCLE_SLEEP_OVERRIDE=""

  if [ "$DO_FULL" -eq 0 ] && [ "$DO_CHANGED" -eq 0 ]; then
    UNTIL_CHANGED=$((CHANGED_SCAN_INTERVAL - (NOW - LAST_CHANGED_EPOCH)))
    UNTIL_FULL=$((FULL_SCAN_INTERVAL - (NOW - LAST_FULL_EPOCH)))
    [ "$UNTIL_CHANGED" -lt 1 ] && UNTIL_CHANGED=1
    [ "$UNTIL_FULL" -lt 1 ] && UNTIL_FULL=1

    SLEEP="$UNTIL_CHANGED"
    [ "$UNTIL_FULL" -lt "$SLEEP" ] && SLEEP="$UNTIL_FULL"

    release_lock
    echo "=== $(date) No scans due. Sleeping ${SLEEP}s... ===" | tee -a "$SCANLOG"
    sleep "$SLEEP"
    continue
  fi

  echo "=== $(date) Scan cycle starting (full_due=${DO_FULL} changed_due=${DO_CHANGED}) ===" | tee -a "$SCANLOG"

  if [ "$DO_FULL" -eq 1 ]; then
    echo "=== FULL SCAN starting ===" | tee -a "$SCANLOG"

    FULL_LIST="$TMP_DIR/full_list.txt"

    if build_scan_list "FULL" "$FULL_LIST" 0; then
      if run_scan_list "$FULL_LIST" "FULL" "$FULL_SCAN_PARALLEL_JOBS" "$FULL_CHUNK_SIZE" "$FULL_PROGRESS_STEPS"; then
        date +%s > "$LAST_FULL" || true
        NEXT_FULL_SCHEDULE_EPOCH=$(cat "$LAST_FULL" 2>/dev/null || echo "$NOW")
        echo "=== FULL SCAN finished ===" | tee -a "$SCANLOG"

        if [ "$FORCE" -eq 1 ] && [ -f "$FORCE_FULL_FLAG" ]; then
          rm -f -- "$FORCE_FULL_FLAG"
          echo "[FORCE] Flag consumed (deleted): $FORCE_FULL_FLAG" | tee -a "$SCANLOG"
        fi
      else
        NEXT_FULL_SCHEDULE_EPOCH=$(date +%s)
        echo "[WARN] Full scan did not complete. Will retry next cycle." | tee -a "$SCANLOG"
      fi
    else
      RC=$?
      NEXT_FULL_SCHEDULE_EPOCH=$(date +%s)

      if [ "$RC" -eq 2 ]; then
        CYCLE_ABORT=1
        CYCLE_SLEEP_OVERRIDE="$PATH_UNAVAILABLE_RETRY_INTERVAL"
        NEXT_CHANGED_SCHEDULE_EPOCH="$NEXT_FULL_SCHEDULE_EPOCH"
        echo "[WARN] Full scan paused because a scan path is unavailable. Retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
      else
        echo "[WARN] Full scan file-list build failed. Will retry next cycle." | tee -a "$SCANLOG"
      fi
    fi
  fi

  if [ "$DO_CHANGED" -eq 1 ] && [ "$CYCLE_ABORT" -eq 0 ]; then
    echo "=== CHANGED-FILES scan starting ===" | tee -a "$SCANLOG"
    CHANGED_LIST="$TMP_DIR/changed_list.txt"
    CHANGED_SCAN_CUTOFF=$(date +%s)

    if build_scan_list "CHANGED" "$CHANGED_LIST" "$LAST_CHANGED_EPOCH"; then
      if run_scan_list "$CHANGED_LIST" "CHANGED" "$CHANGED_SCAN_PARALLEL_JOBS" "$CHANGED_CHUNK_SIZE" "$CHANGED_PROGRESS_STEPS"; then
        echo "$CHANGED_SCAN_CUTOFF" > "$LAST_CHANGED" || true
        NEXT_CHANGED_SCHEDULE_EPOCH="$CHANGED_SCAN_CUTOFF"
      else
        NEXT_CHANGED_SCHEDULE_EPOCH=$(date +%s)
        echo "[WARN] Changed-files scan did not complete. Keeping previous changed-scan checkpoint." | tee -a "$SCANLOG"
      fi
    else
      RC=$?
      NEXT_CHANGED_SCHEDULE_EPOCH=$(date +%s)

      if [ "$RC" -eq 2 ]; then
        CYCLE_ABORT=1
        CYCLE_SLEEP_OVERRIDE="$PATH_UNAVAILABLE_RETRY_INTERVAL"
        NEXT_FULL_SCHEDULE_EPOCH="$NEXT_CHANGED_SCHEDULE_EPOCH"
        echo "[WARN] Changed-files scan paused because a scan path is unavailable. Retrying in ${PATH_UNAVAILABLE_RETRY_INTERVAL}s." | tee -a "$SCANLOG"
      else
        echo "[WARN] Changed-files scan file-list build failed. Keeping previous changed-scan checkpoint." | tee -a "$SCANLOG"
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
  UNTIL_CHANGED=$((CHANGED_SCAN_INTERVAL - (END - NEXT_CHANGED_SCHEDULE_EPOCH)))
  UNTIL_FULL=$((FULL_SCAN_INTERVAL - (END - NEXT_FULL_SCHEDULE_EPOCH)))
  [ "$UNTIL_CHANGED" -lt 1 ] && UNTIL_CHANGED=1
  [ "$UNTIL_FULL" -lt 1 ] && UNTIL_FULL=1
  SLEEP="$UNTIL_CHANGED"
  [ "$UNTIL_FULL" -lt "$SLEEP" ] && SLEEP="$UNTIL_FULL"
  if [ -n "$CYCLE_SLEEP_OVERRIDE" ] && [ "$CYCLE_SLEEP_OVERRIDE" -lt "$SLEEP" ]; then
    SLEEP="$CYCLE_SLEEP_OVERRIDE"
  fi
  echo "Sleeping ${SLEEP}s..." | tee -a "$SCANLOG"
  sleep "$SLEEP"
done
