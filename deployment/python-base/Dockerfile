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

# Upgarde pip to latest
RUN pip install --upgrade pip

# Install Python dependencies
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    numpy \
    pytest \
    mpi4py \
    petsc \
    scipy \
    petsc4py \
    aioredis \
    grpcio-tools \
    protobuf \
    grpcio \
    grpcio-health-checking \
    grpcio-reflection

USER $MDDS_USER
WORKDIR $MDDS_HOME

