# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
import multiprocessing as mp
from datetime import datetime, timezone
from multiprocessing.context import BaseContext
from pathlib import Path

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.context_snapshot import (
    JobExecutionContextSnapshotStore,
)
from mdds_worker_runtime.execution.models import ExecutionRecord
from mdds_worker_runtime.execution.supervised_process import run_job_in_child_process
from mdds_worker_runtime.queue.queue_client import Acknowledger

CONTEXT_SNAPSHOT_FILE_NAME = "context.snapshot.json"


class ExecutionSupervisorStartError(RuntimeError):
    """Raised when supervised execution cannot be started."""


@dataclass(frozen=True)
class SupervisedExecutionRequest:
    """Request to start supervised execution for one validated job."""

    context: JobExecutionContext
    worker_id: str
    manifest_object_key: str
    manifest: JobManifest
    submitted_ack: Acknowledger


class ExecutionSupervisor:
    """Starts and tracks supervised job execution processes."""

    def __init__(
        self,
        jobs_root: Path,
        handler_import_path: str,
        snapshot_store: JobExecutionContextSnapshotStore | None = None,
        process_context: BaseContext | None = None,
    ) -> None:
        if jobs_root is None:
            raise ValueError("jobs_root cannot be null.")
        if handler_import_path is None or handler_import_path.strip() == "":
            raise ValueError("handler_import_path cannot be null or blank.")

        self._jobs_root = jobs_root
        self._handler_import_path = handler_import_path.strip()
        self._snapshot_store = snapshot_store or JobExecutionContextSnapshotStore()
        self._process_context = process_context or mp.get_context("spawn")

    def start(self, request: SupervisedExecutionRequest) -> ExecutionRecord:
        if request is None:
            raise ValueError("request cannot be null.")
        if request.context is None:
            raise ValueError("request context cannot be null.")
        if request.worker_id is None or request.worker_id.strip() == "":
            raise ValueError("request worker_id cannot be null or blank.")
        if (
            request.manifest_object_key is None
            or request.manifest_object_key.strip() == ""
        ):
            raise ValueError("request manifest_object_key cannot be null or blank.")
        if request.manifest is None:
            raise ValueError("request manifest cannot be null.")
        if request.submitted_ack is None:
            raise ValueError("request submitted_ack cannot be null.")

        context = request.context
        context_snapshot_path = self._context_snapshot_path(context)
        self._snapshot_store.save(context, context_snapshot_path)

        parent_connection, child_connection = self._process_context.Pipe(
            duplex=False,
        )

        try:
            process = self._process_context.Process(
                target=run_job_in_child_process,
                args=(
                    context.job_id,
                    self._handler_import_path,
                    context_snapshot_path,
                    child_connection,
                ),
                name=f"mdds-job-{context.job_id}",
            )

            process.start()
        except Exception as exc:
            parent_connection.close()
            child_connection.close()
            raise ExecutionSupervisorStartError(
                f"Cannot start supervised execution process for job {context.job_id}."
            ) from exc

        child_connection.close()
        started_at = datetime.now(timezone.utc)
        return ExecutionRecord(
            job_id=context.job_id,
            user_id=context.user_id,
            job_type=context.job_type,
            worker_id=request.worker_id,
            manifest_object_key=request.manifest_object_key,
            manifest=request.manifest,
            context=request.context,
            process=process,
            parent_connection=parent_connection,
            submitted_ack=request.submitted_ack,
            started_at=started_at,
        )

    def _context_snapshot_path(self, context: JobExecutionContext) -> Path:
        return (
            self._jobs_root
            / str(context.user_id)
            / context.job_id
            / CONTEXT_SNAPSHOT_FILE_NAME
        )
