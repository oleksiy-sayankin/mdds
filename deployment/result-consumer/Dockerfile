# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/java-base:0.1.0

ENV MDDS_RESULT_CONSUMER_HOME=$MDDS_HOME/mdds-result-consumer
ENV MDDS_RESULT_CONSUMER_WEBAPP_DIR_LOCATION=$MDDS_RESULT_CONSUMER_HOME/web-app
ENV MDDS_RESULT_CONSUMER_PORT=8863

# Create app directory
RUN mkdir -p $MDDS_RESULT_CONSUMER_HOME
RUN mkdir -p $MDDS_RESULT_CONSUMER_WEBAPP_DIR_LOCATION

COPY --chown=$MDDS_USER:$MDDS_GROUP ./mdds-result-consumer/target/mdds-result-consumer*jar-with-dependencies.jar $MDDS_RESULT_CONSUMER_HOME/mdds-result-consumer.jar

# Default result-consumer port
EXPOSE $MDDS_RESULT_CONSUMER_PORT

CMD ["java", "-jar", "/opt/mdds/mdds-result-consumer/mdds-result-consumer.jar"]
