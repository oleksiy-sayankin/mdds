# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use minimal Python 3.12 image
FROM python:3.12-slim

ENV APP_HOME=/opt/mdds
ENV MDDS_GRPC_CORE=mdds_grpc_core

ENV MDDS_EXECUTOR_GRPC_WEBSERVER_HOST=0.0.0.0
ENV MDDS_EXECUTOR_GRPC_WEBSERVER_PORT=40062

ENV MDDS_EXECUTOR_GRPC_SERVER_HOST=0.0.0.0
ENV MDDS_EXECUTOR_GRPC_SERVER_PORT=50051

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
RUN mkdir -p "$APP_HOME/$MDDS_GRPC_CORE"
COPY "$MDDS_GRPC_CORE" "$APP_HOME/$MDDS_GRPC_CORE"
WORKDIR "$APP_HOME/$MDDS_GRPC_CORE"

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

EXPOSE $MDDS_EXECUTOR_GRPC_WEBSERVER_PORT
EXPOSE $MDDS_EXECUTOR_GRPC_SERVER_PORT

CMD ["python", "-m", "run"]
