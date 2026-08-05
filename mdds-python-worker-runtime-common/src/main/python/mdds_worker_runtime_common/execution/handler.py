# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from abc import ABC, abstractmethod

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext


class WorkerHandler(ABC):
    """Base class for all concrete MDDS worker handlers."""

    @abstractmethod
    def execute(self, context: WorkerExecutionContext) -> None:
        """Execute worker-specific logic and produce declared output artifacts."""
