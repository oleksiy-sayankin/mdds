# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

FROM grafana/grafana:12.3.3

USER root

COPY --chown=root:root provisioning /etc/grafana/provisioning
COPY --chown=root:root dashboards /etc/grafana/dashboards

RUN chmod -R go-w /etc/grafana/provisioning /etc/grafana/dashboards && \
    find /etc/grafana/provisioning /etc/grafana/dashboards -type d -exec chmod 755 {} \; && \
    find /etc/grafana/provisioning /etc/grafana/dashboards -type f -exec chmod 644 {} \;

USER 472