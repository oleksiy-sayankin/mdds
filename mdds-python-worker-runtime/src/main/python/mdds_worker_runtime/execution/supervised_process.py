# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from multiprocessing.connection import Connection
from pathlib import Path

from mdds_worker_runtime.execution.context_snapshot import (
    JobExecutionContextSnapshotStore,
)
from mdds_worker_runtime.execution.handler_loader import JobHandlerLoader


class SupervisedExecutionStatus(Enum):
    """Internal child-process execution outcome."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class SupervisedExecutionResult:
    """Result sent from supervised child process to parent runtime process."""

    job_id: str
    status: SupervisedExecutionStatus
    message: str | None = None
    error_type: str | None = None

    @staticmethod
    def succeeded(job_id: str) -> "SupervisedExecutionResult":
        return SupervisedExecutionResult(
            job_id=job_id,
            status=SupervisedExecutionStatus.SUCCEEDED,
            message="Execution succeeded.",
        )

    @staticmethod
    def failed(job_id: str, error: BaseException) -> "SupervisedExecutionResult":
        return SupervisedExecutionResult(
            job_id=job_id,
            status=SupervisedExecutionStatus.FAILED,
            message=str(error),
            error_type=type(error).__name__,
        )


def run_job_in_child_process(
    job_id: str,
    handler_import_path: str,
    context_snapshot_path: Path,
    child_connection: Connection,
) -> None:
    """Run JobHandler.execute(context) in a supervised child process.

    The child process must not publish lifecycle statuses, acknowledge queue
    messages, upload artifacts, or access runtime internals. It only executes
    job-specific logic and sends an internal execution result to the parent.
    """

    try:
        context = JobExecutionContextSnapshotStore().load(context_snapshot_path)
        handler = JobHandlerLoader(handler_import_path).load()

        handler.execute(context)

        child_connection.send(SupervisedExecutionResult.succeeded(job_id))
    except Exception as exc:
        child_connection.send(SupervisedExecutionResult.failed(job_id, exc))
    finally:
        child_connection.close()
