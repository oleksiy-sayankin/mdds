# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/java-base:0.1.0

ENV MDDS_SERVER_HOME=$MDDS_HOME/mdds-web-server
ENV MDDS_SERVER_PORT=8000

# Create app directory
RUN mkdir -p $MDDS_SERVER_HOME
COPY --chown=$MDDS_USER:$MDDS_GROUP ./mdds-web-server/target/mdds-web-server*jar-with-spring-dependencies.jar $MDDS_SERVER_HOME/mdds-web-server.jar

# Default web-server port
EXPOSE $MDDS_SERVER_PORT

CMD ["java", "-jar", "/opt/mdds/mdds-web-server/mdds-web-server.jar"]
