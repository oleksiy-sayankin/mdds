# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

FROM grafana/alloy:v1.13.1

COPY --from=busybox:1.37.0 /bin/busybox /bin/busybox
COPY config.alloy /etc/alloy/config.alloy
