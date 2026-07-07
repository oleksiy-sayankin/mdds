# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import logging
import threading

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    ProcessRecord,
)

logger = logging.getLogger(__name__)


class DuplicateExecutionRecordError(RuntimeError):
    """Raised when an execution record for the same job already exists."""


class ExecutionRecordNotFoundError(RuntimeError):
    """Raised when an execution record cannot be found."""


class ExecutionRegistry:
    """
    Thread-safe execution registry for locally running worker jobs.

    The registry is an in-memory index of job_id -> ExecutionRecord.
    It does not publish statuses, does not acknowledge queue messages, does not
    terminate processes, and does not decide which records are timeout or cleanup
    candidates. Those decisions belong to the corresponding watchers.

    Typical successful lifecycle of one execution record:

    1. The submitted job consumer validates the manifest and job-specific inputs.

    2. The worker publishes IN_PROGRESS to the status queue.
       This is a public worker lifecycle status, not a registry state.

    3. The execution supervisor starts the child process.

    4. The execution supervisor creates ExecutionRecord and registers it by calling
       registry.add(record). The registry derives the dictionary key from
       record.job_id, so callers cannot desynchronize the key and the record.

    5. The execution watcher periodically reads registry.snapshot() and checks
       process/pipe state.

    6. When the execution watcher receives a successful process result, it persists
       all declared output artifacts to object storage according to the manifest.
       For DONE, this is part of the completion commit: DONE must not be published
       before output artifacts are durable and available in object storage.

    7. After successful output persistence, the execution watcher calls
       registry.try_claim_terminal(job_id, WorkerJobStatus.DONE, message, finished_at).
       This atomically implements the "first terminal status wins" rule.

    8. The execution watcher publishes DONE to the status queue.

    9. After successful DONE publication, it calls
       registry.mark_terminal_published(job_id).

    10. After acknowledging the original submitted queue message, it calls
        registry.mark_acknowledged(job_id).

    11. After all terminal-side effects are completed, it calls
        registry.mark_cleanup_ready(job_id).

    12. The cleanup watcher reads registry.snapshot(), selects records whose terminal
        status was claimed, terminal status was published, acknowledgement was done,
        and cleanup_ready is true, then closes resources and removes the record with
        registry.remove_if_same(record).

    The same terminal lifecycle is used for ERROR, CANCELLED,
    but this module does not decide when those statuses should happen. It only
    provides thread-safe state transitions and guards their ordering.
    """

    def __init__(self) -> None:
        self._records: dict[str, ExecutionRecord] = {}
        self._lock = threading.RLock()

    def add(self, record: ExecutionRecord) -> None:
        """Add a new execution record.

        Raises DuplicateExecutionRecordError if the job is already registered.
        """
        self._validate_record(record)

        with self._lock:
            if record.job_id in self._records:
                raise DuplicateExecutionRecordError(
                    f"Execution record for job '{record.job_id}' already exists."
                )

            self._records[record.job_id] = record

        logger.info(
            "Execution record registered.",
            extra={
                "component": "execution_registry",
                "event": "execution_record_registered",
                "jobId": record.job_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

    def attach_context(
        self,
        *,
        job_id: str,
        context: JobExecutionContext,
    ) -> ExecutionRecord:
        if context is None:
            raise ValueError("context cannot be null.")

        with self._lock:
            record = self.require(job_id)

            if record.context is not None:
                raise ValueError(f"Context for job '{job_id}' is already attached.")

            record.context = context
            return record

    def attach_process_record(
        self,
        *,
        job_id: str,
        process_record: ProcessRecord,
    ) -> ExecutionRecord:
        if process_record is None:
            raise ValueError("process_record cannot be null.")

        with self._lock:
            record = self.require(job_id)

            if record.context is None:
                raise ValueError(
                    f"Cannot attach process before context for job '{job_id}'."
                )

            if record.process_record is not None:
                raise ValueError(f"Process for job '{job_id}' is already attached.")

            record.process_record = process_record
            return record

    def get(self, job_id: str) -> ExecutionRecord | None:
        """Return execution record by job id, or None."""
        self._validate_job_id(job_id)

        with self._lock:
            return self._records.get(job_id)

    def require(self, job_id: str) -> ExecutionRecord:
        """Return execution record or raise error if absent."""
        record = self.get(job_id)
        if record is None:
            raise ExecutionRecordNotFoundError(
                f"Execution record for job '{job_id}' was not found."
            )
        return record

    def snapshot(self) -> list[ExecutionRecord]:
        """Return a stable snapshot of current records.

        Watchers must iterate over snapshots instead of iterating over the
        dictionary directly.
        """
        with self._lock:
            return list(self._records.values())

    def remove(self, job_id: str) -> ExecutionRecord | None:
        """Remove execution record by job id."""
        self._validate_job_id(job_id)

        with self._lock:
            record = self._records.pop(job_id, None)

        if record is not None:
            logger.info(
                "Execution record removed.",
                extra={
                    "component": "execution_registry",
                    "event": "execution_record_removed",
                    "jobId": record.job_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )

        return record

    def remove_if_same(self, record: ExecutionRecord) -> bool:
        """Remove record only if the same object is still registered.

        This protects cleanup from accidentally removing a newer record
        registered for the same job id after a race/retry.
        """
        self._validate_record(record)

        with self._lock:
            current = self._records.get(record.job_id)
            if current is not record:
                return False

            del self._records[record.job_id]

        logger.info(
            "Execution record removed.",
            extra={
                "component": "execution_registry",
                "event": "execution_record_removed",
                "jobId": record.job_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )
        return True

    def size(self) -> int:
        """Return number of registered executions."""
        with self._lock:
            return len(self._records)

    def clear(self) -> list[ExecutionRecord]:
        """Remove all records and return removed records.

        Caller is responsible for terminating processes and closing resources.
        """
        with self._lock:
            records = list(self._records.values())
            self._records.clear()

        return records

    @staticmethod
    def _validate_record(record: ExecutionRecord) -> None:
        if record is None:
            raise ValueError("record cannot be null.")
        ExecutionRegistry._validate_job_id(record.job_id)

    @staticmethod
    def _validate_job_id(job_id: str) -> None:
        if job_id is None or job_id.strip() == "":
            raise ValueError("job_id cannot be null or blank.")
