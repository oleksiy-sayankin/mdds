# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM eclipse-temurin:21-jre

ENV APP_HOME=/opt/mdds


ENV MDDS_SERVER_WEBAPP_DIR_LOCATION=$APP_HOME/mdds-server

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Create app directory
RUN mkdir -p $APP_HOME/mdds-server
COPY ./mdds-server/target/mdds-server*jar-with-dependencies.jar $APP_HOME/mdds-server/mdds-server.jar

WORKDIR "$APP_HOME"

# Default web-server port
EXPOSE 8000

CMD ["java", "-jar", "/opt/mdds/mdds-server/mdds-server.jar"]
