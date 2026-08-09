# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Synthetic worker handlers used by integration tests."""

from __future__ import annotations

import time

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.handler import WorkerHandler


class HangingWorkerHandler(WorkerHandler):
    """Synthetic WorkerHandler used by Docker e2e SIGTERM tests."""

    def execute(self, context: WorkerExecutionContext) -> None:
        """Keep the supervised worker process alive until Worker Runtime stops it."""
        del context

        while True:
            time.sleep(0.1)


class ErrorWorkerHandler(WorkerHandler):
    """Synthetic WorkerHandler used by Docker e2e error tests."""

    def execute(self, context: WorkerExecutionContext) -> None:
        """Simulate a deterministic job execution failure."""
        del context

        raise ZeroDivisionError("Synthetic division by zero execution failure.")
