# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

FROM alpine:3 AS web-client-extractor

COPY ./mdds-examples/web-clients/mdds-common-web-client/target/mdds-common-web-client.tar.gz \
    /tmp/web-client.tar.gz

RUN mkdir -p /opt/mdds/web-client \
    && tar -xzf /tmp/web-client.tar.gz \
       -C /opt/mdds/web-client


# Extend the MDDS Web Server image with the Common Web Client.
FROM mddsproject/web-server:0.1.0

USER root

COPY --chown=$MDDS_USER:$MDDS_GROUP --from=web-client-extractor /opt/mdds/web-client /opt/mdds/web-client

USER $MDDS_USER

ENV MDDS_WEB_CLIENT_LOCATION=file:/opt/mdds/web-client/
