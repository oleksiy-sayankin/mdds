# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from abc import ABC, abstractmethod

from mdds_worker_runtime.execution.context import JobExecutionContext


class JobHandler(ABC):
    """Base class for all concrete MDDS job handlers."""

    @abstractmethod
    def validate(self, context: JobExecutionContext) -> None:
        """Validate job-specific inputs and parameters before execution."""

    @abstractmethod
    def execute(self, context: JobExecutionContext) -> None:
        """Execute job-specific logic and produce declared output artifacts."""
