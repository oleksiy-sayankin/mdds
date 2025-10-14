# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/python-base:0.1.0
ENV MDDS_EXECUTOR_GRPC_SERVER_PORT=50051
ENV MDDS_EXECUTOR_GRPC_SERVER_HOME=$MDDS_HOME/mdds_grpc_core

# Create app directory
WORKDIR $MDDS_EXECUTOR_GRPC_SERVER_HOME
COPY --chown=$MDDS_USER:$MDDS_GROUP ./mdds_grpc_core $MDDS_EXECUTOR_GRPC_SERVER_HOME

# Default gRPC server port
EXPOSE $MDDS_EXECUTOR_GRPC_SERVER_PORT

CMD ["python", "-m", "run"]
