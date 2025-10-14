# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/java-base:0.1.0

ENV MDDS_EXECUTOR_HOME=$MDDS_HOME/mdds-executor
ENV MDDS_EXECUTOR_WEBAPP_DIR_LOCATION=$MDDS_HOME/mdds-executor/web-app
ENV MDDS_EXECUTOR_PORT=35232

# Create app directory
RUN mkdir -p $MDDS_EXECUTOR_HOME
RUN mkdir -p $MDDS_EXECUTOR_WEBAPP_DIR_LOCATION
COPY --chown=$MDDS_USER:$MDDS_GROUP ./mdds-executor/target/mdds-executor*jar-with-dependencies.jar $MDDS_EXECUTOR_HOME/mdds-executor.jar

# Default executor port
EXPOSE $MDDS_EXECUTOR_PORT

CMD ["java", "-jar", "/opt/mdds/mdds-executor/mdds-executor.jar"]
