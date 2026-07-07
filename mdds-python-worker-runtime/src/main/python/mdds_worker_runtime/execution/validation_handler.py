# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler


class ValidationFailed(RuntimeError):
    """Raised when worker-side semantic validation fails."""


class ValidationHandler:
    """Runs worker-side semantic validation before supervised execution starts.

    This component intentionally does not publish lifecycle statuses, acknowledge
    submitted messages, clean up local workspaces, or decide terminal job state.
    It only invokes the concrete job handler validation method and lets the
    caller classify the outcome through the job state transition coordinator.
    """

    def validate(
        self,
        *,
        handler: JobHandler,
        context: JobExecutionContext,
    ) -> None:
        """Validate submitted job inputs and parameters using the job handler.

        A successful validation returns normally. Expected domain validation
        problems must be reported by the concrete handler as ``ValidationFailed``.
        Unexpected exceptions are propagated unchanged so the caller can convert
        them into terminal ``ERROR`` through a coordinator-owned transition.
        """
        if handler is None:
            raise ValueError("handler cannot be null.")
        if context is None:
            raise ValueError("context cannot be null.")

        handler.validate(context)
