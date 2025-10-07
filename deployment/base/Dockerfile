# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use minimal Python 3.12 image
FROM python:3.12-slim

ENV APP_HOME=/opt/mdds

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libblas-dev  \
    liblapack-dev \
    gcc \
    build-essential

# Upgarde pip to latest
RUN pip install --upgrade pip

# Create app directory
WORKDIR "$APP_HOME"

# Install Python dependencies
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    numpy \
    pytest \
    mpi4py \
    petsc \
    petsc4py \
    aioredis \
    grpcio-tools \
    protobuf \
    grpcio

