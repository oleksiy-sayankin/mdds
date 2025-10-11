# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/common:0.1.0

ENV APP_HOME=/opt/mdds

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Create app directory
RUN mkdir -p $APP_HOME/mdds_grpc_core
COPY ./mdds_grpc_core $APP_HOME/mdds_grpc_core
WORKDIR $APP_HOME/mdds_grpc_core

# Default gRPC server port
EXPOSE 50051

CMD ["python", "-m", "run"]
