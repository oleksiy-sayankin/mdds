# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Use base image for project
FROM mddsproject/python-base:0.1.0

#
# Install Python Worker Runtime package.
#
# The base image owns third-party Python dependencies.
# The worker runtime wheel owns only MDDS Worker Runtime code and entry point.
#

USER root

COPY mdds-python-worker-runtime/target/dist/*.whl /tmp/

RUN python -m pip install --no-cache-dir --no-deps /tmp/*.whl && \
    python -m pip check && \
    rm -f /tmp/*.whl

USER $MDDS_USER
WORKDIR $MDDS_HOME

ENTRYPOINT ["mdds-worker-runtime"]
