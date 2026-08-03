FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

ARG CLAMAV_PACKAGE_VERSION=1.4.5-r0

RUN apk upgrade --no-cache && apk add --no-cache "clamav=${CLAMAV_PACKAGE_VERSION}" "clamav-daemon=${CLAMAV_PACKAGE_VERSION}" coreutils findutils python3 util-linux tzdata && clamd --version | grep -Eq '^ClamAV 1\.4\.5($|/)' && addgroup -S -g 10001 clamav-scheduled && adduser -S -D -u 10001 -G clamav-scheduled -h /home/clamav-scheduled clamav-scheduled && install -d -o 10001 -g 10001 -m 0750 /config /downloads /events /home/clamav-scheduled /quarantine /state /tmp/clamav-runtime /var/log/clamav && chmod 0755 /var/lib/clamav

COPY scripts/clamav_scheduled.sh /usr/local/bin/clamav_scheduled.sh
COPY scripts/clamd_session_scan.py /usr/local/bin/clamd_session_scan.py
COPY scripts/scan_list_filter.py /usr/local/bin/scan_list_filter.py
COPY scripts/clamav_healthcheck.py /usr/local/bin/clamav_healthcheck.py
COPY scripts/clamav_entrypoint.py /usr/local/bin/clamav_entrypoint.py
COPY scripts/clamav_ui_server.py /usr/local/bin/clamav_ui_server.py
COPY scripts/event_writer.py scripts/scan_root_guard.py scripts/checkpoint_state.py /usr/local/bin/
COPY ui /usr/local/share/clamav-ui

RUN chmod 0555 /usr/local/bin/clamav_scheduled.sh /usr/local/bin/clamd_session_scan.py /usr/local/bin/scan_list_filter.py /usr/local/bin/clamav_healthcheck.py /usr/local/bin/clamav_entrypoint.py /usr/local/bin/clamav_ui_server.py /usr/local/bin/event_writer.py /usr/local/bin/scan_root_guard.py /usr/local/bin/checkpoint_state.py && chmod -R a=rX /usr/local/share/clamav-ui

ENV APP_MODE=headless HOME=/home/clamav-scheduled PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 RUNTIME_DIR=/tmp/clamav-runtime DEFINITIONS_DIR=/var/lib/clamav DEFINITIONS_MAX_AGE_SECONDS=172800 EVENT_DIR=/events

USER 10001:10001

EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=10s --start-period=5m --retries=3 CMD ["python3", "/usr/local/bin/clamav_healthcheck.py"]

CMD ["python3", "/usr/local/bin/clamav_entrypoint.py"]
