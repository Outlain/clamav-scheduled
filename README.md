# ClamAV Scheduled

A lightweight scheduled ClamAV scanner container for scanning a downloads folder, quarantining infected files, and keeping scan state between runs.

## Features

- Dual-mode startup: headless env-driven mode or browser-based UI mode
- Runs `clamd` inside the container
- Uses persistent Unix-socket sessions and passes an already-open file descriptor to `clamd` (`FILDES`)
- Validates oversized video containers with `ffprobe` and reads every byte through bounded overlapping ClamD windows
- Verifies device, inode, size, modification time, and change time before scanning and again before quarantine
- Uses NUL-delimited enumeration, so filenames containing newlines, tabs, colons, or non-UTF-8 bytes remain intact
- Calendar-based full-scan schedules supporting weekly, monthly, yearly, quarterly, and last-day rules
- The same calendar flexibility for changed-files scans, with old weekday/time configurations preserved
- Incremental changed-files scans between full scans
- Supports multiple scan roots with `SCAN_PATHS`
- Quarantines infected files
- Force full scan via a flag file
- Dynamic chunk sizing for clearer progress logging
- Separate full-scan and changed-scan concurrency controls
- Richer scan metrics including bytes, infected/error counts, per-root summaries, and slowest files
- Live progress logs report the first completed file, regular throughput checkpoints, and 30-second worker/queue heartbeats
- File-list discovery emits a low-overhead heartbeat every 30 seconds with entries visited, entries added during the last window, temporary-list size, elapsed time, and the latest successfully visited path
- Operator pause/resume for full and changed scans without stopping ClamD or the UI
- Restart-safe full and changed scans that reuse only exact, revalidated per-file results from a completed durable manifest
- Reports isolated files that vanish after list-building and fails the run when the configured count/percentage is suspicious
- Pauses and retries if any configured scan root becomes unavailable
- Captures scan-root and optional marker identities before enumeration and verifies them again after all workers finish
- Stores full/changed checkpoints together in one atomic JSON state file with legacy-file migration
- Emits durable schema-v1 detection, quarantine, mount, definition, scan-failure, and restart-safe recovery events for the central notifier
- Persistent state and ClamAV definitions via bind mounts
- Runs as fixed non-root UID/GID `10001:10001`, supports a read-only root filesystem, and drops all Linux capabilities in the Compose example
- Waits for complete external definitions at startup and exposes strict definition-age/container health checks

## Warning

This container is designed for trusted local/server use. Review paths, permissions, and quarantine behavior before using it on production data.

## Modes

The container can run in one of two modes:

- `APP_MODE=headless` - the current behavior; scanner settings come directly from environment variables
- `APP_MODE=ui` - starts a built-in web UI on port `8080`; scanner settings are loaded from `/config/ui-config.json` instead of from environment variables

In UI mode, scheduler configuration environment variables are intentionally ignored. Container/bootstrap settings (UI bounds, runtime paths, definition readiness, and clamd safety limits) still come from the environment; schedule and scan-path settings come from the saved UI config.

## Environment Variables

### Bootstrap / container mode

- `APP_MODE` - `headless` or `ui`; defaults to `headless`
- `UI_BIND` - bind address for the UI server in UI mode; defaults to `0.0.0.0`
- `UI_PORT` - UI port in UI mode; defaults to `8080`
- `CONFIG_DIR` - persistent UI configuration directory in UI mode; defaults to `/config`
- `STATE_DIR` - persistent runtime state directory; defaults to `/state`
- `SCAN_PROGRESS_DIR` - durable active-scan manifest and per-file result journal; defaults to `/state/scan-progress`
- `SCAN_PAUSE_FILE` - durable cooperative pause flag; defaults to `/state/scan.pause`
- `SCAN_RESUME_MAX_AGE_SECONDS` - maximum age of compatible interrupted work; defaults to `2592000` (30 days), maximum one year; `0` disables age expiry while retaining all identity checks
- `EVENT_DIR` - durable event spool for `clamav-notifier`; defaults to `/events`
- `RUNTIME_DIR` - private ephemeral clamd configuration/socket directory; defaults to `/tmp/clamav-runtime`
- `UI_MAX_REQUEST_THREADS` - maximum simultaneous UI request threads; defaults to `32`, hard maximum `128`
- `UI_REQUEST_QUEUE_SIZE` - listener backlog; defaults to `64`, hard maximum `256`
- `UI_REQUEST_TIMEOUT_SECONDS` - per-connection timeout; defaults to `15`, hard maximum `120`
- `DEFINITIONS_DIR` - read-only ClamAV database mount; defaults to `/var/lib/clamav`
- `DEFINITIONS_WAIT_TIMEOUT` - startup wait for complete `main` and `daily` databases; defaults to `300` seconds
- `DEFINITIONS_MAX_AGE_SECONDS` - maximum accepted `daily` database age; defaults to `172800` (48 hours)
- `DEFINITIONS_STALE_ACTION` - `warn` or `fail` at startup; the container healthcheck is strict regardless
- `MAX_SCHEDULED_FILES` - maximum unique paths indexed in one run; defaults to `1000000`, hard maximum `5000000`
- `SCANLOG_MAX_BYTES` - rotates the main scan log during scans; defaults to `104857600` (100 MiB), hard maximum 1 GiB
- `SCANLOG_ROTATIONS` - retained rotated scan logs; defaults to `5`, hard maximum `20`
- `VANISHED_FILE_FAILURE_COUNT` - absolute vanished-file threshold; defaults to `100`
- `VANISHED_FILE_FAILURE_PERCENT` - vanished percentage threshold; defaults to `10`
- `VANISHED_FILE_FAILURE_MINIMUM` - minimum vanished count before applying the percentage threshold; defaults to `10`
- `LARGE_MEDIA_ENABLED` - enables the oversized-video route; defaults to `true`
- `LARGE_MEDIA_MAX_FILE_GIB` - hard individual-video ceiling; defaults to `100`
- `LARGE_MEDIA_CHUNK_MIB` - independent ClamD window; defaults to `1024` and cannot exceed the native ClamD file limit
- `LARGE_MEDIA_OVERLAP_KIB` - bytes repeated across adjacent windows; defaults to `1024`
- `LARGE_MEDIA_PROBE_TIMEOUT_SECONDS` - ffprobe validation deadline; defaults to `120`
- `LARGE_MEDIA_SCAN_TIMEOUT_SECONDS` - total deadline for one oversized video; defaults to `21600` (six hours)
- `MAX_LARGE_MEDIA_WORKERS` - separate concurrency ceiling for oversized videos; defaults to `1`
- `FFPROBE_BINARY` - image-provided validation executable; defaults to `/usr/bin/ffprobe`

### Headless scanner configuration

These variables apply directly only in `APP_MODE=headless`. In `APP_MODE=ui`, the browser UI stores these settings persistently and the scheduler uses that saved config instead.

- `TZ` - timezone
- `MAXTHREADS` - clamd thread count; hard range `1..64`
- `SCAN_PATHS` - colon-separated scan roots inside the container; defaults to `/downloads` and every listed path must be mounted and healthy before a scan runs
- `EXCLUDE_PATHS` - optional colon-separated in-container file or directory paths to skip during both full and changed scans
- `FULL_SCAN_PARALLEL_JOBS` - parallel persistent scan workers for full scans; hard maximum `64` and cannot exceed `MAXTHREADS`
- `CHANGED_SCAN_PARALLEL_JOBS` - parallel persistent scan workers for changed-file scans; hard maximum `64` and cannot exceed `MAXTHREADS`
- `FULL_PROGRESS_STEPS` - target number of progress updates used to derive the full-scan progress interval
- `CHANGED_PROGRESS_STEPS` - target number of progress updates used to derive the changed-scan progress interval
- `FULL_CHUNK_SIZE` - optional fixed full-scan progress interval override; `0` keeps dynamic sizing
- `CHANGED_CHUNK_SIZE` - optional fixed changed-scan progress interval override; `0` keeps dynamic sizing
- `FULL_SCAN_DAYS` - comma-separated days for scheduled full scans; accepts `mon`-`sun`, full day names, `1`-`7`, or `*`; defaults to `sun`
- `FULL_SCAN_MONTHS` - comma-separated month numbers `1`-`12`, or `*` for every month; defaults to `*`
- `FULL_SCAN_MONTH_DAYS` - comma-separated dates `1`-`31`, `last`, or `*` for every valid date; defaults to `*`
- `FULL_SCAN_TIMES` - required comma-separated `HH:MM` times for scheduled full scans in the container timezone
- `CHANGED_SCAN_DAYS` - comma-separated days for scheduled changed-file scans; accepts `mon`-`sun`, full day names, `1`-`7`, or `*`; defaults to `*`
- `CHANGED_SCAN_MONTHS` - comma-separated month numbers `1`-`12`, or `*` for every month; defaults to `*`
- `CHANGED_SCAN_MONTH_DAYS` - comma-separated dates `1`-`31`, `last`, or `*` for every valid date; defaults to `*`
- `CHANGED_SCAN_TIMES` - required comma-separated `HH:MM` times for scheduled changed-file scans in the container timezone
- `SCAN_FAILURE_RETRY_INTERVAL` - seconds to wait before retrying a scheduled scan after a non-path-related failure
- `FORCE_FULL_POLL_INTERVAL` - seconds between force-full flag checks while the scheduler is otherwise idle; lower values make forced full scans start sooner
- `PATH_CHECK_TIMEOUT` - seconds allowed for each scan-root health check before treating the path as unavailable
- `PATH_ENUMERATION_TIMEOUT` - seconds allowed for each per-root `find` pass before treating the path as unavailable (default `1800`; large NAS trees may need more). While it runs, fixed 30-second heartbeats show traversal progress without reading file contents.
- `PATH_UNAVAILABLE_RETRY_INTERVAL` - seconds to wait before retrying when a configured scan root is unavailable
- `SCAN_PATH_MARKER` - optional file or directory name expected inside every scan root; use this to detect missing NFS mounts that fall back to an empty local directory
- `QUARANTINE_DIR` - infected file destination
- `SCANLOG` - log file path
- `FORCE_FULL_FLAG` - full-scan trigger flag file path; defaults to the first path in `SCAN_PATHS`

### Clamd safety limits

These are bootstrap settings in both modes. They are validated before any clamd configuration is written:

- `CLAMD_MAX_QUEUE` - queued commands; defaults to twice `MAXTHREADS`, must be at least `MAXTHREADS`, hard maximum `128`
- `CLAMD_MAX_SCAN_SIZE` - maximum expanded content scanned per input; defaults to and is capped at `2000M`
- `CLAMD_MAX_FILE_SIZE` - maximum individual file size clamd processes; defaults to and is capped at `2000M`, and cannot exceed `CLAMD_MAX_SCAN_SIZE`
- `CLAMD_LOG_MAX_SIZE` - clamd diagnostic log rotation threshold; defaults to `10M`, hard maximum `1G`
- `CLAMD_MAX_RECURSION` - archive/container nesting depth; defaults to `32`, hard maximum `100`
- `CLAMD_MAX_FILES` - files extracted from one container/archive; defaults to `10000`, hard maximum `1000000`
- `CLAMD_MAX_SCAN_TIME` - milliseconds allowed per scan; defaults to `900000` (15 minutes), hard maximum one hour
- `CLAMD_READ_TIMEOUT` and `CLAMD_COMMAND_READ_TIMEOUT` - socket timeouts; defaults to `900` and `30` seconds
- `CLAMD_SELF_CHECK` - seconds between definition timestamp checks; defaults to `300`
- `CLAMD_START_TIMEOUT` - maximum database-load/readiness wait; defaults to `180` seconds

`AlertExceedsMax yes` is enabled, so a limit-exceeded object is reported instead
of being silently treated as fully scanned. Responses whose signature starts
with `Heuristics.Limits.Exceeded`, and ClamD limit-error replies, are classified
as `POLICY_LIMIT`: they emit `scan_failed`, remain in place, are never
quarantined as malware, and prevent checkpoint advancement.

The `2000M` native limit is now a routing boundary. Above it, an approved video
container must pass `ffprobe`, contain a real video stream, and contain no
unsupported stream or attachment type. The scanner then reads every byte in
independent `1024 MiB` ClamD streams with `1024 KiB` overlap and records
`scan_method=large_media_full_byte_windows`. This supports ordinary 5-50 GiB
MKV/MP4 files without claiming that separate windows are identical to one native
whole-file parser invocation; signatures can see all raw bytes, while whole-file
hashes and parser state cannot cross a window boundary.

Oversized ZIP/RAR/7z archives, disk images, executables, audio-only files,
unknown formats, unsafe media attachments, files above the configured ceiling,
and validation/time/ClamD failures remain `POLICY_LIMIT`. The scheduled scanner
does not automatically expand those archives: it continues checking other files,
leaves the original in place, marks the run incomplete, and keeps the previous
checkpoint. The event ID is derived from the unchanged file identity and reason,
so the notifier does not resend the same archive warning after every retry. A
changed file receives a new identity and a new alert.

Only `MAX_LARGE_MEDIA_WORKERS` oversized-video jobs run at once. When that slot
is busy, standard workers put later oversized entries back behind pending work
instead of all waiting for the same slot. Ordinary files therefore continue to
make progress even when a directory contains several very large files.

## UI mode

In UI mode the application listens on container port `8080`. Docker publishes
that private listener using two Compose-time variables:

- `UI_PUBLISH_IP` defaults to `127.0.0.1` and selects the Docker-host address.
- `UI_HOST_PORT` defaults to `8080` and selects the Docker-host port.

These are Compose interpolation settings, not saved scanner settings and not
container environment variables. Define them in `.env` or in Portainer's stack
environment before deploying. `UI_BIND=0.0.0.0` and `UI_PORT=8080` should
normally remain unchanged inside the container. For example, a Docker host whose
trusted-LAN address is `192.0.2.10` can publish an otherwise unused host port:

```dotenv
UI_PUBLISH_IP=192.0.2.10
UI_HOST_PORT=8094
```

The UI is then reached at `http://192.0.2.10:8094`. It has no application-level
authentication, so do not publish it on an untrusted interface; retaining the
loopback default and using an authenticated reverse proxy or SSH tunnel is safer.

On first run:

1. Open the UI.
2. Fill in the required scan settings.
3. Save the configuration.
4. The scheduler starts from the saved UI config and keeps using it across restarts.

The UI currently includes:

- initial setup and settings editing
- live scheduler state
- current scan progress
- live file-list discovery counts and the latest successfully visited path; this is intentionally an indeterminate traversal because the final total is unknown until `find` finishes
- running-average and since-last-update throughput/data rates
- recent scan history
- recent log tail
- on-demand full scans from the UI, with optional target paths and optional one-off ignore paths
- the separate exact `FORCE_FULL_FLAG` file path for simple NAS-side full-scan triggering
- on-demand changed-file scans using either "since last successful checkpoint" or a custom recent lookback window
- optional target paths and optional one-off ignore paths for on-demand changed-file scans
- pause and resume for an active full or changed scan
- restart-scanner action for restarting only the scanner process inside the container

Enumeration errors from `find` are escaped and written to both the scan log and the UI/container log as they occur. To prevent a damaged mount from flooding logs indefinitely, the first 100 diagnostic lines are shown and any additional count is summarized. A blocked NFS operation may produce no filesystem error of its own; in that case, a heartbeat with zero new entries shows that the process is still alive but has made no observable path progress during that window.

Recommended UI-mode mounts:

```yaml
volumes:
  - /mnt/media:/downloads:rw
  - /opt/docker/clamav-scheduled/config:/config:rw
  - ${SCHEDULED_STATE_HOST_PATH:-/opt/docker/clamav-shared/state/clamav-scheduled}:/state:rw
  - /opt/docker/clamav-shared/events/clamav-scheduled:/events:rw
  - /opt/docker/clamav-shared/logs/clamav-scheduled:/var/log/clamav:rw
  - /opt/docker/clamav-shared/quarantine/clamav-scheduled:/quarantine:rw
  - /opt/docker/clamav-shared/defs:/var/lib/clamav:ro
```

`/opt/docker/clamav-shared/state/clamav-scheduled` is private to this container
despite the parent directory name. It does not need to match another service's
mount. The host filesystem backing `/opt` must be local storage; see
[Local state storage](#local-state-storage).

### ClamAV definitions updater and volume permissions

This scanner intentionally does not run `freshclam`. Your separate `clamav-defs-updater` container is exactly the updater referenced here: it is the single writer to the shared definitions directory, while this scanner and any other consumers mount the same data read-only.

For the arrangement to work reliably:

- the updater must publish complete database files into the shared directory using FreshClam's atomic update behavior
- the scanner must be able to read and traverse the directory (normally directories `0755` and definition files `0644`, or equivalent group permissions)
- both containers must mount the exact same host directory or named volume at `/var/lib/clamav`
- the updater must be the only process writing definitions; do not run a second FreshClam process in the scanner
- startup waits for readable, non-empty `main.cvd/main.cld` and `daily.cvd/daily.cld`; it never creates or writes the definitions mount
- `clamd` checks definition timestamps every `CLAMD_SELF_CHECK` seconds and reloads updates from the shared volume; concurrent reload is disabled to avoid a large temporary memory spike
- an operator can request an immediate reload with `docker exec clamav-scheduled python3 /usr/local/bin/clamav_healthcheck.py --reload`
- the built-in healthcheck fails when definitions are missing, incomplete, unreadable, or older than `DEFINITIONS_MAX_AGE_SECONDS`

The updater does not need access to the scanner's private socket when `SelfCheck` is acceptable. Sharing the socket would couple two containers and expands the trust boundary; use it only if reload latency must be lower than the configured self-check interval.

### Read/write contract

The image runs as UID/GID `10001:10001`. At startup it performs an actual create-and-delete probe instead of trusting permission bits alone.

| Path | Scanner access | Reason |
| --- | --- | --- |
| scan roots | read/write/search | clamd reads content; successful quarantine must unlink the infected source |
| quarantine | read/write/search | creates no-overwrite mode-`0600` quarantine files |
| `/config` (UI mode) | read/write | atomic UI config and history replacement |
| `/state` | read/write | locks, checkpoints, manual requests, pause flag, durable file manifest, and restart journal |
| `/events` | read/write | atomic structured events for the central notifier |
| log directory | read/write | append-only operational log |
| definitions | read-only | the external updater is the only writer |
| `/tmp` | tmpfs | private clamd socket/config and temporary extraction; root filesystem remains read-only |

Prepare bind mounts before starting the container:

```sh
install -d -m 0750 -o 10001 -g 10001 \
  /opt/docker/clamav-scheduled/config \
  /opt/docker/clamav-shared/state/clamav-scheduled \
  /opt/docker/clamav-shared/events/clamav-scheduled \
  /opt/docker/clamav-shared/logs/clamav-scheduled \
  /opt/docker/clamav-shared/quarantine/clamav-scheduled \
  /opt/docker/clamav-shared/defs
defs=/opt/docker/clamav-shared/defs
find "$defs" -type d -exec chmod 0755 {} +
find "$defs" -type f -exec chmod 0644 {} +
```

The numeric owner must match the Compose `user` setting (`SCANNER_UID` and
`SCANNER_GID`). For example, use owner `3000:3000` when the service runs as
`3000:3000`. A `:rw` bind-mount suffix only permits writes at the Docker mount
layer; it does not change normal host ownership or mode bits. If Docker creates
a missing bind source automatically, it is commonly created as `root:root`, so
prepare every writable source directory before the first deployment.

Do not place `STATE_DIR`, `RUNTIME_DIR`, `TMP_DIR`, definitions, or `SCANLOG` inside a scan root. The scanner rejects that layout because its own writes would change files while they are being scanned. Nested directories can still have stricter permissions; a later per-file permission failure is recorded as a scan error and prevents checkpoint advancement.

## Pause and restart recovery

Both full scans and changed-file scans can be paused from the UI. Pause is
cooperative and fail-safe:

- during discovery, the scanner suspends only the active filesystem-walk process
  and paused time does not count against `PATH_ENUMERATION_TIMEOUT`
- during indexing, it stops at a bounded path-check boundary
- during ordinary scanning, each worker finishes its current ClamD request and
  stops before taking another file
- during oversized-video scanning, it stops between bounded byte windows
- ClamD, the scheduler, the health check, and the UI remain running

The pause request is the regular file `/state/scan.pause`. It survives a
container or host restart, so a deliberately paused scan stays paused until an
operator resumes it. In headless mode the equivalent controls are:

```sh
# Pause the active scan (or make the next scan wait).
touch /opt/docker/clamav-shared/state/clamav-scheduled/scan.pause

# Resume it.
rm /opt/docker/clamav-shared/state/clamav-scheduled/scan.pause
```

Run those commands as a user that can write the state directory, and do not
replace the flag with a directory or symlink.

For restart recovery, the scanner does not install SQLite or another database.
It maintains one simple, inspectable active-session directory under
`/state/scan-progress/active` containing:

- `session.json` for the scan identity, original cutoff, and configuration fingerprint
- `roots.json` for the original scan-root and marker identities
- `files.nul` for the NUL-delimited path manifest
- `completed.jsonl` for durable per-file outcomes and exact file identities

If interruption occurs while discovery is still building `files.nul`, discovery
starts over. A half-finished filesystem walk cannot safely prove that it saw the
whole tree. Once the manifest is marked ready, a restart reuses a clean result
only when device, inode, size, modification time, and change time still match.
A successfully quarantined detection is reused only when the source is still
absent and the exact quarantine file still exists. Changed, replaced, vanished,
failed, limit-blocked, and not-yet-durable files are scanned again.

The original full-scan cutoff or changed-scan reference is retained. Checkpoints
advance only after every remaining file, mount/marker recheck, worker, and
quarantine gate succeeds. The manifest is then removed. This may repeat a small
amount of recent work after sudden power loss, but it never treats an unrecorded
file as scanned. A changed scan already in progress is resumed before a newly
due full scan so its saved work is not discarded.

Changing a scan-affecting setting, changing the request, exceeding
`SCAN_RESUME_MAX_AGE_SECONDS`, or finding incompatible/corrupt progress causes a
safe rebuild. If a root is unavailable or its configured marker is missing, the
saved session is retained and cannot resume until the mount is healthy again.
When `SCAN_PATH_MARKER` is configured, every current root is healthy, and every
marker is present but a saved root/marker identity changed across a host reboot
or remount, the old per-file results are discarded and the scanner immediately
builds a fresh manifest. Without a marker, an identity change remains blocked;
the scanner cannot safely distinguish the intended remount from a fallback or
wrong filesystem mounted at the same path.

Routine signature reloads do not invalidate an active manifest. ClamD can also
reload definitions during an uninterrupted multi-day scan, so a completed scan
is a sequence of per-file checks rather than one frozen database snapshot.
Pending or repeated files always use the definitions loaded by ClamD at that
time; an engine or scan-policy change does invalidate saved progress.

### Local state storage

Bind `/state` to a directory on the Docker host's local SSD or NVMe. Do not put
it on `/mnt/media`, NFS, SMB, or another network filesystem, and do not share one
state directory between scanner instances. The recommended Compose setting is:

```dotenv
SCHEDULED_STATE_HOST_PATH=/opt/docker/clamav-shared/state/clamav-scheduled
```

That path is suitable only if `/opt` is actually backed by the Docker host's
local disk. The scanner never copies media content into `/state`; it stores path
names, identities, and compact JSON results. A normal hundred-thousand-file tree
usually needs tens to hundreds of MiB, while multi-million-file manifests can
grow into GiB depending on path lengths. Keep free space available and include
this directory in state backups. Batched journal syncs limit write overhead,
but local SSD/NVMe prevents those durability writes from adding NAS latency.
Media discovery and scanning still read the source data from its original
mount, so local state cannot remove the source NAS's own I/O limit.

## Scan schedules

Each schedule combines four filters: weekdays, months, days of the month, and
times. All selected filters must match. `*` means unrestricted for that filter,
and `last` selects the final valid date of a month. This keeps the original
weekday/time format while permitting calendar intervals without another daemon
or dependency.

`CHANGED_SCAN_TIMES` and `FULL_SCAN_TIMES` remain required. In UI mode, a
human-friendly **How Often** selector provides Every day, Every week, Every
month, and Every year choices. It shows only the applicable weekday, date, or
month control and previews the rule in plain language. **Advanced / custom**
exposes the underlying filters for quarterly, multi-weekday, or other combined
rules. Existing saved schedules are recognized automatically; the UI-only
frequency choice does not change the persisted scheduler format.

For example, to run a full scan on the third day of every month at 03:30 in UI
mode, choose **Every month**, choose **3rd** under **Day Of Month**, enter
`03:30`, and confirm the plain-language schedule preview before saving.


Examples:

Existing weekly behavior remains valid without adding the new variables:

- `CHANGED_SCAN_DAYS=mon,tue,wed,thu,fri`
- `CHANGED_SCAN_TIMES=09:00,13:00,17:00`
- `FULL_SCAN_DAYS=sun`
- `FULL_SCAN_TIMES=03:30`

Monthly on the first day at the configured full-scan time:

- `FULL_SCAN_DAYS=*`
- `FULL_SCAN_MONTHS=*`
- `FULL_SCAN_MONTH_DAYS=1`

Yearly on January 1:

- `FULL_SCAN_DAYS=*`
- `FULL_SCAN_MONTHS=1`
- `FULL_SCAN_MONTH_DAYS=1`

Quarterly on the first day of January, April, July, and October:

- `FULL_SCAN_DAYS=*`
- `FULL_SCAN_MONTHS=1,4,7,10`
- `FULL_SCAN_MONTH_DAYS=1`

For the final day of every month, use `FULL_SCAN_DAYS=*`,
`FULL_SCAN_MONTHS=*`, and `FULL_SCAN_MONTH_DAYS=last`. A numeric date that does
not exist in a selected month is skipped; a combination that can never occur,
such as February 30, is rejected during configuration validation.

Schedules are evaluated in the container timezone from `TZ`.

If a scheduled scan fails, the scheduler retries after `SCAN_FAILURE_RETRY_INTERVAL` until the scan succeeds or a newer scheduled slot becomes due.

A successful full scan also refreshes the changed-files checkpoint, so the scanner does not immediately rerun a redundant changed-files scan in the same cycle.

UI-queued full scans only advance the normal scheduled full/changed checkpoints when they cover all configured scan paths with no extra one-off ignore paths. Scoped UI full scans are treated as extra manual scans and leave the regular schedule checkpoints unchanged.

Changed-file scans treat either a newer content-modified time or a newer metadata-change time as "changed," which helps catch files copied in with preserved old modification times.

If a file disappears after it was added to the scan list but before `clamd` can scan it, the run records it as `vanished`. Isolated normal churn is tolerated, but a count or percentage above the configured thresholds fails the run. Real scan errors, quarantine failures, incomplete enumeration/worker processing, a missing or replaced mount/marker, a dead ClamD, and structured-result or event-write failures keep the previous atomic checkpoints in place.

Detected threat signatures are retained. Each detection is durably spooled before filesystem action, then a quarantine-success or quarantine-failure event is emitted. The same details remain in the structured scan log for the UI and operators; the notifier does not tail that human log. Per-file temporary results use JSON Lines so paths and threat names cannot corrupt a delimiter-based record.

Deprecated environment variables such as `DOWNLOADS_DIR`, `PARALLEL_JOBS`, `CHUNK_SIZE`, `SCAN_INTERVAL`, `CHANGED_SCAN_INTERVAL`, and `FULL_SCAN_INTERVAL` are no longer accepted.

## Multiple scan roots

Mount every host directory you want to scan into the container and list the in-container paths in `SCAN_PATHS` separated by `:`.

Example:

```yaml
environment:
  SCAN_PATHS: /downloads:/archive
volumes:
  - ./downloads:/downloads:rw
  - ./archive:/archive:rw
```

If any configured scan root fails its health check or file enumeration, the current scan cycle is paused and retried after `PATH_UNAVAILABLE_RETRY_INTERVAL`.

For NFS-backed roots, set `SCAN_PATH_MARKER` to the name of a file or directory that must exist in every scan root. That prevents the scanner from quietly treating an empty fallback directory as a healthy mount.

## Excluding paths

Set `EXCLUDE_PATHS` to a colon-separated list of absolute in-container paths you want skipped.

Examples:

- `EXCLUDE_PATHS=/downloads/private:/downloads/tmp`
- `EXCLUDE_PATHS=/downloads/ignore-me.txt:/archive/large-file.iso`

If an entry points to a directory, everything under that directory is skipped. If an entry points to a specific file, only that file is skipped. Directory entries with a trailing `/` are accepted.

## Docker Compose

See `docker-compose.example.yml`. It preserves `/mnt/media:/downloads:rw` while
moving definitions, logs, events, state, and quarantine to dedicated paths below
`/opt/docker/clamav-shared`, outside the media tree. The UI binds only to
`127.0.0.1` by default because it has no authentication. Change the Docker-host
publish address with `UI_PUBLISH_IP` rather than changing the application's
internal `UI_BIND` setting.

When upgrading an existing deployment, keep the existing `/state` contents and
point `SCHEDULED_STATE_HOST_PATH` at that same local directory. No database
migration is required: the existing atomic checkpoints remain authoritative and
the new `scan-progress` directory is created automatically. Before redeploying,
ensure the state directory is owned by the configured `SCANNER_UID` and
`SCANNER_GID` and is on local storage. If the old state path was on NAS storage,
stop the scanner, copy it to the local path, preserve ownership/modes, and then
change the bind mount.

Rollback is also simple: stop the new container and redeploy the previous image
with the same config, definitions, logs, quarantine, and state mounts. Older
images continue from the last successful checkpoint and ignore the new
`scan-progress` directory. Remove a remaining `scan.pause` flag before rollback
if the old image has no pause control. Keep the progress directory until the
rollback window ends; deleting it discards only interrupted per-file progress,
not successful checkpoints or quarantined content.

## Image and update policy

The image uses Alpine `3.24.1` pinned by its multi-architecture OCI digest and ClamAV `1.4.6-r0` pinned as a build argument. Alpine is used because it provides a small, supported runtime with maintained `amd64` and `arm64` packages; the `v3.24` branch is supported through June 2028. Each build applies patched packages from that same stable branch but never jumps to a new Alpine branch. ClamAV `1.4` is the current long-term-support line, and `1.4.6` is the reviewed package revision. See the [Alpine release branches](https://www.alpinelinux.org/releases/), [Alpine 3.24.1 release](https://www.alpinelinux.org/posts/Alpine-3.24.1-released.html), and [ClamAV end-of-life table](https://docs.clamav.net/faq/faq-eol.html).

`latest` is a discovery signal, not a reproducible deployment input. General OS patch revisions are updated only while building and are captured in that image's SBOM and digest; the engine is held for explicit compatibility review. Automatically replacing packages inside a running security scanner would:

- make two builds from the same commit produce different software
- bypass the architecture, reload, quarantine, permission, and regression tests
- make rollback uncertain
- combine code/engine updates with live state changes on a read-only container

Virus definitions are different: they are data designed for frequent automatic updates, so the external updater refreshes them continuously and clamd reloads them. Engine/base-image updates follow a reviewed image rebuild.

Updates are still automated up to the approval boundary:

- Dependabot checks the pinned base image digest and SHA-pinned GitHub Actions weekly
- the weekly dependency audit fails when Alpine's `v3.24` ClamAV candidate differs from `CLAMAV_PACKAGE_VERSION`
- the same audit rebuilds the image and fails on newly disclosed fixed `HIGH` or `CRITICAL` OS vulnerabilities
- every change runs unit tests, a real clamd database-reload/FILDES/quarantine test on `amd64` and `arm64`, and a vulnerability gate
- releases publish provenance and an SBOM

When an audit reports an update, review the upstream release notes, update the explicit version/digest in a pull request, let CI validate it, then deploy the resulting immutable application-image digest.

### Canary rollout

Use the following operational sequence for an engine/base migration:

1. Back up UI config and state, and record the currently deployed image digest.
2. Deploy the candidate digest to one canary with the same mount types and permissions as production.
3. Confirm the healthcheck reports fresh definitions and a responsive scheduler/clamd.
4. Force a definitions update or run the explicit reload command, then verify the database version/age changes without permission errors.
5. Scan a controlled test threat and unusual filename; confirm the signature, source, quarantine path, `quarantine_success=true`, and mode `0600`.
6. Observe at least one changed scan and one full scan, then roll out gradually. Roll back to the recorded digest if errors, memory pressure, or quarantine failures increase.

## Force a full scan

Create the configured flag file, for example:

```sh
touch /opt/docker/clamav-shared/state/clamav-scheduled/force_full_scan.flag
```

The flag is consumed and deleted after a successful forced full scan.

While the scheduler is idle, it polls for the force flag every `FORCE_FULL_POLL_INTERVAL` seconds, so adding the file wakes the loop early instead of waiting until the next scheduled scan time.

## Container registry

GitHub Actions publishes the image to:

`ghcr.io/<repo-owner>/clamav-scheduled:latest`
