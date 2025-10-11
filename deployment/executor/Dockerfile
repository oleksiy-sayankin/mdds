# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM eclipse-temurin:21-jre

ENV APP_HOME=/opt/mdds
ENV MDDS_EXECUTOR_WEBAPP_DIR_LOCATION=$APP_HOME/mdds-executor

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Create app directory
RUN mkdir -p $APP_HOME/mdds-executor
COPY ./mdds-executor/target/mdds-executor*jar-with-dependencies.jar $APP_HOME/mdds-executor/mdds-executor.jar

WORKDIR "$APP_HOME"

# Default executor port
EXPOSE 35232

CMD ["java", "-jar", "/opt/mdds/mdds-executor/mdds-executor.jar"]
