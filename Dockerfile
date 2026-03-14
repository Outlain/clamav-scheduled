FROM alpine:3.20

RUN apk add --no-cache \
    clamav \
    clamav-daemon \
    coreutils \
    findutils \
    python3 \
    util-linux \
    tzdata

COPY scripts/clamav_scheduled.sh /usr/local/bin/clamav_scheduled.sh
COPY scripts/clamd_session_scan.py /usr/local/bin/clamd_session_scan.py
RUN chmod +x /usr/local/bin/clamav_scheduled.sh /usr/local/bin/clamd_session_scan.py

CMD ["/bin/sh", "/usr/local/bin/clamav_scheduled.sh"]
