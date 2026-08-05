# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

#
# Base image for Python Atomic Worker Images.
#
FROM python:3.12-slim

ENV MDDS_HOME=/opt/mdds
ENV MDDS_USER=mdds
ENV MDDS_GROUP=mdds
ENV MDDS_USER_ID=1010
ENV MDDS_GROUP_ID=1010

# Copy locked list of Python packages.
COPY mdds-deployment/python-worker-runtime-common/requirements.lock.txt \
    /tmp/requirements.lock.txt

# Copy package version constraints.
COPY mdds-deployment/python-worker-runtime-common/build-constraints.txt \
    /tmp/build-constraints.txt

# Copy the Python Worker Runtime Common package.
COPY mdds-python-worker-runtime-common/target/dist/*.whl /tmp/

# Create the non-root runtime user, prepare the canonical filesystem
# layout, install dependencies, and expose the contract entry point.
RUN groupadd --gid "$MDDS_GROUP_ID" "$MDDS_GROUP" && \
    useradd \
        --uid "$MDDS_USER_ID" \
        --gid "$MDDS_GROUP_ID" \
        -m \
        -s /bin/bash \
        "$MDDS_USER" && \
    install -d -m 0755 \
        "$MDDS_HOME/bin" \
        "$MDDS_HOME/config" \
        "$MDDS_HOME/inputs" && \
    install -d \
        -m 0755 \
        -o "$MDDS_USER" \
        -g "$MDDS_GROUP" \
        "$MDDS_HOME/outputs" \
        "$MDDS_HOME/result" \
        "$MDDS_HOME/tmp" && \
    python -m pip install \
        --upgrade \
        --only-binary=:all: \
        "pip==26.1.2" && \
    python -m pip install \
        --no-cache-dir \
        --require-hashes \
        --build-constraint /tmp/build-constraints.txt \
        -r /tmp/requirements.lock.txt && \
    python -m pip check && \
    python -m pip install \
        --no-cache-dir \
        --no-deps \
        /tmp/*.whl && \
    python -m pip check && \
    ln -s \
        /usr/local/bin/mdds-worker \
        "$MDDS_HOME/bin/mdds-worker" && \
    test -x "$MDDS_HOME/bin/mdds-worker" && \
    rm -f \
        /tmp/*.whl \
        /tmp/requirements.lock.txt \
        /tmp/build-constraints.txt

USER $MDDS_USER
WORKDIR $MDDS_HOME