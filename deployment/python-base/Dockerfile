# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

#
# This base image for all Python based images
#
FROM python:3.12-slim

ENV MDDS_HOME=/opt/mdds
ENV MDDS_USER=mdds
ENV MDDS_GROUP=mdds
ENV MDDS_USER_ID=1010
ENV MDDS_GROUP_ID=1010

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Copy list of Python packages
COPY deployment/python-base/requirements.txt /tmp/requirements.txt

# Copy package version constraints
COPY deployment/python-base/build-constraints.txt /tmp/build-constraints.txt

# Create application user, install system and Python dependencies,
# and install grpc_health_probe binary.
RUN groupadd --gid "$MDDS_GROUP_ID" "$MDDS_GROUP" && \
    useradd --uid "$MDDS_USER_ID" --gid "$MDDS_GROUP_ID" -m -s /bin/bash "$MDDS_USER" && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        libblas-dev \
        liblapack-dev \
        gcc \
        curl \
        build-essential && \
    curl -fsSL \
        https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.41/grpc_health_probe-linux-amd64 \
        -o /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe && \
    pip install --upgrade "pip>=25.3" && \
    pip install --no-cache-dir \
        --build-constraint /tmp/build-constraints.txt \
        -r /tmp/requirements.txt && \
    pip check && \
    rm -rf /var/lib/apt/lists/*

USER $MDDS_USER
WORKDIR $MDDS_HOME
