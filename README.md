# ClamAV Scheduled

A lightweight scheduled ClamAV scanner container for scanning a downloads folder, quarantining infected files, and keeping scan state between runs.

## Features

- Runs `clamd` inside the container
- Configurable full-scan cadence using `FULL_SCAN_INTERVAL`
- Configurable changed-files scan cadence using `CHANGED_SCAN_INTERVAL`
- Incremental changed-files scans between full scans
- Supports multiple scan roots with `SCAN_PATHS`
- Quarantines infected files
- Force full scan via a flag file
- Dynamic chunk sizing for clearer progress logging
- Separate full-scan and changed-scan concurrency controls
- Pauses and retries if any configured scan root becomes unavailable
- Persistent state and ClamAV definitions via bind mounts

## Warning

This container is designed for trusted local/server use. Review paths, permissions, and quarantine behavior before using it on production data.

## Environment Variables

- `TZ` - timezone
- `MAXTHREADS` - clamd thread count
- `SCAN_PATHS` - colon-separated scan roots inside the container; every listed path must be mounted and healthy before a scan runs
- `FULL_SCAN_PARALLEL_JOBS` - parallel `clamdscan` processes for full scans
- `CHANGED_SCAN_PARALLEL_JOBS` - parallel `clamdscan` processes for changed-file scans
- `FULL_PROGRESS_STEPS` - target number of progress updates used to derive full-scan chunk sizes
- `CHANGED_PROGRESS_STEPS` - target number of progress updates used to derive changed-scan chunk sizes
- `FULL_CHUNK_SIZE` - optional fixed full-scan chunk size override; `0` keeps dynamic chunk sizing
- `CHANGED_CHUNK_SIZE` - optional fixed changed-scan chunk size override; `0` keeps dynamic chunk sizing
- `FULL_SCAN_INTERVAL` - seconds between full scans
- `CHANGED_SCAN_INTERVAL` - seconds between changed-file scans
- `PATH_CHECK_TIMEOUT` - seconds allowed for each scan-root health check before treating the path as unavailable
- `PATH_ENUMERATION_TIMEOUT` - seconds allowed for each per-root `find` pass before treating the path as unavailable
- `PATH_UNAVAILABLE_RETRY_INTERVAL` - seconds to wait before retrying when a configured scan root is unavailable
- `SCAN_PATH_MARKER` - optional file or directory name expected inside every scan root; use this to detect missing NFS mounts that fall back to an empty local directory
- `DOWNLOADS_DIR` - legacy single-root default used when `SCAN_PATHS` is not set
- `QUARANTINE_DIR` - infected file destination
- `STATE_DIR` - persistent state directory
- `SCANLOG` - log file path
- `FORCE_FULL_FLAG` - full-scan trigger flag file path; defaults to the first path in `SCAN_PATHS`

Legacy compatibility:

- `DOWNLOADS_DIR` still provides the default for `SCAN_PATHS`
- `PARALLEL_JOBS` still provides the default for both `FULL_SCAN_PARALLEL_JOBS` and `CHANGED_SCAN_PARALLEL_JOBS`
- `CHUNK_SIZE` still provides the default for both `FULL_CHUNK_SIZE` and `CHANGED_CHUNK_SIZE`
- `SCAN_INTERVAL` still provides the default for `CHANGED_SCAN_INTERVAL`

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

## Docker Compose

See `docker-compose.example.yml`.

## Force a full scan

Create the configured flag file, for example:

```sh
 touch ./downloads/.clamav_force_full_scan.flag
```

The flag is consumed and deleted after a successful forced full scan.

## Container registry

GitHub Actions publishes the image to:

`ghcr.io/<repo-owner>/clamav-scheduled:latest`
