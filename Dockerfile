FROM alpine:3.20

RUN apk add --no-cache \
    clamav \
    clamav-daemon \
    coreutils \
    findutils \
    util-linux \
    tzdata

COPY scripts/clamav_scheduled.sh /usr/local/bin/clamav_scheduled.sh
RUN chmod +x /usr/local/bin/clamav_scheduled.sh

CMD ["/bin/sh", "/usr/local/bin/clamav_scheduled.sh"]
