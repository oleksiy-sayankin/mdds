# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

#
# This base image for all java based images
#
FROM eclipse-temurin:21-jre

ENV MDDS_HOME=/opt/mdds
ENV MDDS_USER=mdds
ENV MDDS_GROUP=mdds
ENV MDDS_USER_ID=1010
ENV MDDS_GROUP_ID=1010
RUN groupadd --gid $MDDS_GROUP_ID $MDDS_USER && \
    useradd --uid $MDDS_USER_ID --gid $MDDS_GROUP_ID -m -s /bin/bash $MDDS_USER
RUN apt-get update && apt-get install -y netcat-traditional
USER $MDDS_USER
WORKDIR $MDDS_HOME
