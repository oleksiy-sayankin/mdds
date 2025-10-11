# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM eclipse-temurin:21-jre

ENV APP_HOME=/opt/mdds
ENV MDDS_RESULT_CONSUMER_WEBAPP_DIR_LOCATION=$APP_HOME/mdds-result-consumer

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Create app directory
RUN mkdir -p $APP_HOME/mdds-result-consumer
COPY ./mdds-result/target/mdds-result*jar-with-dependencies.jar $APP_HOME/mdds-result-consumer/mdds-result-consumer.jar

WORKDIR "$APP_HOME"

# Default result-consumer port
EXPOSE 8863

CMD ["java", "-jar", "/opt/mdds/mdds-result-consumer/mdds-result-consumer.jar"]
