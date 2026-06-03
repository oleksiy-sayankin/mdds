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
RUN groupadd --gid $MDDS_GROUP_ID $MDDS_USER && \
    useradd --uid $MDDS_USER_ID --gid $MDDS_GROUP_ID -m -s /bin/bash $MDDS_USER

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libblas-dev  \
    liblapack-dev \
    gcc \
    build-essential

# Install grpc_health_probe binary
ADD https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.41/grpc_health_probe-linux-amd64 /usr/local/bin/grpc_health_probe
RUN chmod +x /usr/local/bin/grpc_health_probe

# Upgrade pip to latest
RUN pip install --upgrade "pip>=25.3"

# Copy list of Python packages
COPY deployment/python-base/requirements.txt /tmp/requirements.txt

# Copy package version constraints
COPY deployment/python-base/build-constraints.txt /tmp/build-constraints.txt

# Install Python dependencies
RUN pip install --no-cache-dir \
    --build-constraint /tmp/build-constraints.txt \
    -r /tmp/requirements.txt && \
    pip check

USER $MDDS_USER
WORKDIR $MDDS_HOME

