# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

FROM grafana/loki:2.9.8

USER root

COPY --chown=root:root loki.yaml /etc/loki/loki.yaml

RUN chmod 644 /etc/loki/loki.yaml

USER 10001:10001
