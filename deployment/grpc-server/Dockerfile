# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/common:0.1.0

ENV APP_HOME=/opt/mdds
ENV MDDS_GRPC_CORE=mdds_grpc_core

ENV MDDS_EXECUTOR_GRPC_SERVER_HOST=0.0.0.0
ENV MDDS_EXECUTOR_GRPC_SERVER_PORT=50051

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Create app directory
RUN mkdir -p "$APP_HOME/$MDDS_GRPC_CORE"
COPY ./"$MDDS_GRPC_CORE" "$APP_HOME/$MDDS_GRPC_CORE"
WORKDIR "$APP_HOME/$MDDS_GRPC_CORE"

EXPOSE $MDDS_EXECUTOR_GRPC_SERVER_PORT

CMD ["python", "-m", "run"]
