# ClamAV Scheduled

A lightweight scheduled ClamAV scanner container for scanning a downloads folder, quarantining infected files, and keeping scan state between runs.

## Features

- Runs `clamd` inside the container
- Uses a persistent socket-based scan client instead of spawning one `clamdscan` process per file
- Time-based full-scan schedule using `FULL_SCAN_DAYS` and `FULL_SCAN_TIMES`
- Time-based changed-files schedule using `CHANGED_SCAN_DAYS` and `CHANGED_SCAN_TIMES`
- Incremental changed-files scans between full scans
- Supports multiple scan roots with `SCAN_PATHS`
- Quarantines infected files
- Force full scan via a flag file
- Dynamic chunk sizing for clearer progress logging
- Separate full-scan and changed-scan concurrency controls
- Richer scan metrics including bytes, infected/error counts, per-root summaries, and slowest files
- Live progress logs show both running-average and since-last-update throughput/data rates
- Treats files that vanish after list-building as non-fatal and reports them separately
- Pauses and retries if any configured scan root becomes unavailable
- Persistent state and ClamAV definitions via bind mounts

## Warning

This container is designed for trusted local/server use. Review paths, permissions, and quarantine behavior before using it on production data.

## Environment Variables

- `TZ` - timezone
- `MAXTHREADS` - clamd thread count
- `SCAN_PATHS` - colon-separated scan roots inside the container; defaults to `/downloads` and every listed path must be mounted and healthy before a scan runs
- `EXCLUDE_PATHS` - optional colon-separated in-container file or directory paths to skip during both full and changed scans
- `FULL_SCAN_PARALLEL_JOBS` - parallel persistent scan workers for full scans
- `CHANGED_SCAN_PARALLEL_JOBS` - parallel persistent scan workers for changed-file scans
- `FULL_PROGRESS_STEPS` - target number of progress updates used to derive the full-scan progress interval
- `CHANGED_PROGRESS_STEPS` - target number of progress updates used to derive the changed-scan progress interval
- `FULL_CHUNK_SIZE` - optional fixed full-scan progress interval override; `0` keeps dynamic sizing
- `CHANGED_CHUNK_SIZE` - optional fixed changed-scan progress interval override; `0` keeps dynamic sizing
- `FULL_SCAN_DAYS` - comma-separated days for scheduled full scans; accepts `mon`-`sun`, full day names, `1`-`7`, or `*`; defaults to `sun`
- `FULL_SCAN_TIMES` - required comma-separated `HH:MM` times for scheduled full scans in the container timezone
- `CHANGED_SCAN_DAYS` - comma-separated days for scheduled changed-file scans; accepts `mon`-`sun`, full day names, `1`-`7`, or `*`; defaults to `*`
- `CHANGED_SCAN_TIMES` - required comma-separated `HH:MM` times for scheduled changed-file scans in the container timezone
- `SCAN_FAILURE_RETRY_INTERVAL` - seconds to wait before retrying a scheduled scan after a non-path-related failure
- `FORCE_FULL_POLL_INTERVAL` - seconds between force-full flag checks while the scheduler is otherwise idle; lower values make forced full scans start sooner
- `PATH_CHECK_TIMEOUT` - seconds allowed for each scan-root health check before treating the path as unavailable
- `PATH_ENUMERATION_TIMEOUT` - seconds allowed for each per-root `find` pass before treating the path as unavailable
- `PATH_UNAVAILABLE_RETRY_INTERVAL` - seconds to wait before retrying when a configured scan root is unavailable
- `SCAN_PATH_MARKER` - optional file or directory name expected inside every scan root; use this to detect missing NFS mounts that fall back to an empty local directory
- `QUARANTINE_DIR` - infected file destination
- `STATE_DIR` - persistent state directory
- `SCANLOG` - log file path
- `FORCE_FULL_FLAG` - full-scan trigger flag file path; defaults to the first path in `SCAN_PATHS`

## Scan schedules

Use `*_SCAN_DAYS` plus `*_SCAN_TIMES` to define when scans should run. `CHANGED_SCAN_TIMES` and `FULL_SCAN_TIMES` are required.

Examples:

- `CHANGED_SCAN_DAYS=mon,tue,wed,thu,fri`
- `CHANGED_SCAN_TIMES=09:00,13:00,17:00`
- `FULL_SCAN_DAYS=sun`
- `FULL_SCAN_TIMES=03:30`

Schedules are evaluated in the container timezone from `TZ`.

If a scheduled scan fails, the scheduler retries after `SCAN_FAILURE_RETRY_INTERVAL` until the scan succeeds or a newer scheduled slot becomes due.

A successful full scan also refreshes the changed-files checkpoint, so the scanner does not immediately rerun a redundant changed-files scan in the same cycle.

Changed-file scans treat either a newer content-modified time or a newer metadata-change time as "changed," which helps catch files copied in with preserved old modification times.

If a file disappears after it was added to the scan list but before `clamd` can scan it, the run records that file as `vanished` instead of failing the entire scan. Real scan errors still fail the run and keep the previous checkpoints in place.

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

See `docker-compose.example.yml`.

## Force a full scan

Create the configured flag file, for example:

```sh
 touch ./downloads/.clamav_force_full_scan.flag
```

The flag is consumed and deleted after a successful forced full scan.

While the scheduler is idle, it polls for the force flag every `FORCE_FULL_POLL_INTERVAL` seconds, so adding the file wakes the loop early instead of waiting until the next scheduled scan time.

## Container registry

GitHub Actions publishes the image to:

`ghcr.io/<repo-owner>/clamav-scheduled:latest`
