# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Synthetic SLAE worker handlers used by integration tests."""

from __future__ import annotations

import time

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler


class HangingJobHandler(JobHandler):
    """Synthetic JobHandler used by Docker e2e cancellation tests."""

    def execute(self, context: JobExecutionContext) -> None:
        """Keep the supervised job process alive until Worker Runtime cancels it."""
        del context

        while True:
            time.sleep(0.1)
