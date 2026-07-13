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

# Copy locked list of Python packages
COPY deployment/python-base/requirements.lock.txt /tmp/requirements.lock.txt

# Copy package version constraints
COPY deployment/python-base/build-constraints.txt /tmp/build-constraints.txt

# Create application user, install system and Python dependencies,
# and install grpc_health_probe binary.
RUN groupadd --gid "$MDDS_GROUP_ID" "$MDDS_GROUP" && \
    useradd --uid "$MDDS_USER_ID" --gid "$MDDS_GROUP_ID" -m -s /bin/bash "$MDDS_USER" && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        gcc \
        libblas-dev \
        liblapack-dev && \
    curl --fail --silent --show-error --location \
        --proto "=https" \
        --proto-redir "=https" \
        --tlsv1.2 \
        https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.41/grpc_health_probe-linux-amd64 \
        -o /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe && \
    python -m pip install \
        --upgrade \
        --only-binary=:all: \
        "pip==26.1.2" && \
    python -m pip install --no-cache-dir \
        --require-hashes \
        --build-constraint /tmp/build-constraints.txt \
        -r /tmp/requirements.lock.txt && \
    python -m pip check && \
    rm -rf /var/lib/apt/lists/*

USER $MDDS_USER
WORKDIR $MDDS_HOME
