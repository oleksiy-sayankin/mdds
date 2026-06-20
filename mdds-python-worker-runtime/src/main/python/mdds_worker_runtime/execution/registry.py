# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import logging
import threading
from datetime import datetime

from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    WorkerJobStatus,
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

    The same terminal lifecycle is used for ERROR, CANCELLED, and VALIDATION_FAILED,
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

    def try_claim_terminal(
        self,
        job_id: str,
        terminal_status: WorkerJobStatus,
        message: str | None,
        finished_at: datetime,
    ) -> ExecutionRecord | None:
        """Try to claim terminal status for a job.

        Returns the record if this caller won the terminal race.
        Returns None if terminal status was already claimed or record is absent.
        """
        if terminal_status not in {
            WorkerJobStatus.DONE,
            WorkerJobStatus.ERROR,
            WorkerJobStatus.CANCELLED,
            WorkerJobStatus.VALIDATION_FAILED,
        }:
            raise ValueError(f"Status '{terminal_status}' is not terminal.")

        record = self.get(job_id)
        if record is None:
            return None

        with record.lock:
            if record.terminal_status_claimed:
                return None

            record.terminal_status_claimed = True
            record.terminal_status = terminal_status
            record.terminal_message = message
            record.finished_at = finished_at

        logger.info(
            "Terminal status claimed.",
            extra={
                "component": "execution_registry",
                "event": "terminal_status_claimed",
                "jobId": record.job_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "status": terminal_status.value,
            },
        )

        return record

    def mark_terminal_published(self, job_id: str) -> None:
        """Mark that the selected terminal status was published.

        This method must be called only after try_claim_terminal(...) succeeds and
        the caller successfully publishes the terminal status to the status queue.

        For WorkerJobStatus.DONE, the caller must first persist all declared output
        artifacts to object storage according to the job manifest. DONE must not be
        published until the output artifacts are durable and available for download.

        It intentionally does not publish statuses and does not write artifacts by
        itself. Output persistence belongs to the execution watcher / output
        committer. Status publication belongs to the status publisher.

        This flag is required before acknowledging the original submitted queue
        message. The ordering protects the system from losing the final job state:
        a job message must not be acknowledged before its terminal status is
        successfully published. For DONE, it also protects users from seeing DONE
        before output artifacts are available in object storage.
        """
        record = self.require(job_id)

        with record.lock:
            if not record.terminal_status_claimed:
                raise RuntimeError(
                    f"Cannot mark terminal status as published for job '{job_id}': "
                    "terminal status was not claimed."
                )

            record.terminal_status_published = True

    def mark_acknowledged(self, job_id: str) -> None:
        """Mark that the original submitted queue message was acknowledged.

        This method must be called only after terminal status publication succeeds
        and after the caller acknowledges the original submitted queue message.

        The registry does not call ack() itself. The acknowledgement handle is stored
        in ExecutionRecord, but the caller controls the exact queue-side operation.

        The ordering is important: acknowledgement before terminal status publication
        could make the job disappear from the queue while the orchestrator never
        receives DONE, ERROR, CANCELLED, or VALIDATION_FAILED.
        """
        record = self.require(job_id)

        with record.lock:
            if not record.terminal_status_published:
                raise RuntimeError(
                    f"Cannot mark job '{job_id}' as acknowledged before terminal status is published."
                )

            record.acknowledgement_done = True

    def mark_cleanup_ready(self, job_id: str) -> None:
        """Mark execution record as ready for cleanup.

        This method must be called after all terminal side effects are complete:
        terminal status was published and the original submitted queue message was
        acknowledged.

        The method does not remove the record and does not close process resources.
        Cleanup watcher owns that responsibility. The watcher should read
        registry.snapshot(), select records that satisfy cleanup conditions, close
        process/pipe resources, and remove the record using remove_if_same(record).
        """
        record = self.require(job_id)

        with record.lock:
            if not record.terminal_status_published:
                raise RuntimeError(
                    f"Cannot mark job '{job_id}' as cleanup-ready before terminal status is published."
                )

            if not record.acknowledgement_done:
                raise RuntimeError(
                    f"Cannot mark job '{job_id}' as cleanup-ready before acknowledgement."
                )

            record.cleanup_ready = True

    @staticmethod
    def _validate_record(record: ExecutionRecord) -> None:
        if record is None:
            raise ValueError("record cannot be null.")
        ExecutionRegistry._validate_job_id(record.job_id)

    @staticmethod
    def _validate_job_id(job_id: str) -> None:
        if job_id is None or job_id.strip() == "":
            raise ValueError("job_id cannot be null or blank.")
