# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

FROM grafana/alloy:v1.13.1

USER root

COPY --from=busybox:1.37.0 /bin/busybox /bin/busybox
COPY --chown=root:root config.alloy /etc/alloy/config.alloy

RUN chmod 755 /bin/busybox && \
    chmod 644 /etc/alloy/config.alloy && \
    mkdir -p /var/lib/alloy/data && \
    chown -R 473:473 /var/lib/alloy

USER 473:473
