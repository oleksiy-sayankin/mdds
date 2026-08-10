# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

#
# Base image for Python Atomic Worker Images.
#
FROM mddsproject/python-worker-runtime-common:0.1.0

# The base image runs as mdds. Temporarily switch to root at build time
# to install packages and remove root-owned build artifacts.
USER root

# Copy locked list of Python packages.
COPY mdds-deployment/python-worker-solving-slae-numpy/requirements.lock.txt \
    /tmp/requirements.lock.txt

# Copy package version constraints.
COPY mdds-deployment/python-worker-solving-slae-numpy/build-constraints.txt \
    /tmp/build-constraints.txt

# Copy the Python Worker Solving SLAE NumPy package.
COPY mdds-examples/workers/mdds-python-worker-solving-slae-numpy/target/dist/*.whl /tmp/

# Install dependencies.
RUN python -m pip install \
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
    rm -f \
        /tmp/*.whl \
        /tmp/requirements.lock.txt \
        /tmp/build-constraints.txt

# Run the Worker Runtime without root privileges.

USER $MDDS_USER
WORKDIR $MDDS_HOME

ENV MDDS_WORKER_NAME="solving-slae-python"
ENV MDDS_WORKER_VERSION="0.1.0"
ENV MDDS_WORKER_HANDLER="mdds_python_worker_solving_slae_numpy.handler:SlaeWorkerHandler"
