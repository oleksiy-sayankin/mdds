# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/java-base:0.1.0

ENV MDDS_SERVER_HOME=$MDDS_HOME/mdds-server
ENV MDDS_SERVER_WEBAPP_DIR_LOCATION=$MDDS_SERVER_HOME/web-app
ENV MDDS_SERVER_PORT=8000

# Create app directory
RUN mkdir -p $MDDS_SERVER_HOME
RUN mkdir -p $MDDS_SERVER_WEBAPP_DIR_LOCATION
COPY --chown=$MDDS_USER:$MDDS_GROUP ./mdds-server/target/mdds-server*jar-with-dependencies.jar $MDDS_SERVER_HOME/mdds-server.jar
COPY --chown=$MDDS_USER:$MDDS_GROUP ./mdds-server/web-app/ $MDDS_SERVER_WEBAPP_DIR_LOCATION/

# Default web-server port
EXPOSE $MDDS_SERVER_PORT

CMD ["java", "-jar", "/opt/mdds/mdds-server/mdds-server.jar"]
