# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

FROM mddsproject/python-worker-runtime:0.1.0

USER root

COPY mdds-examples/workers/mdds-python-worker-solving-slae/target/dist/*.whl /tmp/

RUN python -m pip install --no-cache-dir --no-deps /tmp/*.whl && \
    python -m pip check && \
    rm -f /tmp/*.whl

USER $MDDS_USER
WORKDIR $MDDS_HOME

ENV MDDS_WORKER_JOB_TYPE="solving_slae"
ENV MDDS_WORKER_HANDLER="mdds_python_worker_solving_slae.handler:SlaeJobHandler"
