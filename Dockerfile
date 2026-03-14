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
COPY scripts/clamav_entrypoint.py /usr/local/bin/clamav_entrypoint.py
COPY scripts/clamav_ui_server.py /usr/local/bin/clamav_ui_server.py
COPY ui /usr/local/share/clamav-ui
RUN chmod +x \
    /usr/local/bin/clamav_scheduled.sh \
    /usr/local/bin/clamd_session_scan.py \
    /usr/local/bin/clamav_entrypoint.py \
    /usr/local/bin/clamav_ui_server.py

EXPOSE 8080

CMD ["python3", "/usr/local/bin/clamav_entrypoint.py"]
