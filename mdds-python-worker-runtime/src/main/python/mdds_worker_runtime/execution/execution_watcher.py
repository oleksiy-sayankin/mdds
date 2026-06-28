# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
import logging
import math
import threading

from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.output_artifact_uploader import (
    OutputArtifactUploader,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.supervised_process import (
    SupervisedExecutionResult,
    SupervisedExecutionStatus,
)

logger = logging.getLogger(__name__)

DONE_PROGRESS = 100
ERROR_PROGRESS = 100
MIN_IN_PROGRESS_PROGRESS = 1
MAX_IN_PROGRESS_PROGRESS = 99

DEFAULT_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_PROGRESS_INTERVAL_SECONDS = 5.0


class ExecutionWatcher:
    """Observes supervised executions and finalizes worker lifecycle state.

    The watcher converts completed supervised process results into terminal
    Worker Runtime lifecycle outcomes.

    While a supervised process is still running, the watcher periodically
    publishes time-based IN_PROGRESS updates.

    It does not start jobs, does not perform validation, does not remove
    execution records, does not implement cancellation, and does not enforce
    timeouts.
    """

    def __init__(
        self,
        execution_registry: ExecutionRegistry,
        output_artifact_uploader: OutputArtifactUploader,
        status_publisher: StatusPublisher,
        worker_id: str,
        job_timeout_seconds: float,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
        progress_interval_seconds: float = DEFAULT_PROGRESS_INTERVAL_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if output_artifact_uploader is None:
            raise ValueError("output_artifact_uploader cannot be null.")
        if status_publisher is None:
            raise ValueError("status_publisher cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")
        if job_timeout_seconds <= 0:
            raise ValueError("job_timeout_seconds must be greater than zero.")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than zero.")
        if progress_interval_seconds <= 0:
            raise ValueError("progress_interval_seconds must be greater than zero.")
        if clock is None:
            raise ValueError("clock cannot be null.")

        self._execution_registry = execution_registry
        self._output_artifact_uploader = output_artifact_uploader
        self._status_publisher = status_publisher
        self._worker_id = worker_id.strip()
        self._job_timeout_seconds = job_timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._progress_interval_seconds = progress_interval_seconds
        self._clock = clock

        self._last_progress_published_at_by_job_id: dict[str, datetime] = {}

        self._stop_requested = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start background execution watcher thread."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop_requested.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="mdds-execution-watcher",
                daemon=True,
            )
            self._thread.start()

        logger.info(
            "Execution watcher started.",
            extra={
                "component": "execution_watcher",
                "event": "execution_watcher_started",
                "workerId": self._worker_id,
            },
        )

    def stop(self, timeout_seconds: float = 10.0) -> None:
        """Request watcher shutdown and wait for its thread to stop."""
        self._stop_requested.set()

        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout_seconds)

        logger.info(
            "Execution watcher stopped.",
            extra={
                "component": "execution_watcher",
                "event": "execution_watcher_stopped",
                "workerId": self._worker_id,
            },
        )

    def poll_once(self) -> None:
        """Process one snapshot of active execution records.

        This method is intentionally public to make watcher behavior deterministic
        in unit tests.
        """
        now = self._clock()

        for record in self._execution_registry.snapshot():
            self._process_record(record, now)

    def _run_loop(self) -> None:
        while not self._stop_requested.is_set():
            try:
                self.poll_once()
            except Exception:
                logger.exception(
                    "Execution watcher polling iteration failed.",
                    extra={
                        "component": "execution_watcher",
                        "event": "execution_watcher_poll_failed",
                        "workerId": self._worker_id,
                    },
                )

            self._stop_requested.wait(self._poll_interval_seconds)

    def _process_record(self, record: ExecutionRecord, now: datetime) -> None:
        if record is None:
            return

        if self._is_terminal_claimed(record):
            logger.debug(
                "Execution record is already terminal-claimed; skipping.",
                extra={
                    "component": "execution_watcher",
                    "event": "execution_record_already_terminal_claimed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )
            return

        result = self._try_read_execution_result(record)
        if result is None:
            self._publish_in_progress_if_due(record, now)
            return

        self._close_supervised_execution_resources(record)

        if result.status == SupervisedExecutionStatus.SUCCEEDED:
            self._handle_succeeded(record, result, now)
            return

        if result.status == SupervisedExecutionStatus.FAILED:
            self._handle_failed(record, result, now)
            return

        self._handle_unknown_result_status(record, result, now)

    @staticmethod
    def _is_terminal_claimed(record: ExecutionRecord) -> bool:
        with record.lock:
            return record.terminal_status_claimed

    @staticmethod
    def _try_read_execution_result(
        record: ExecutionRecord,
    ) -> SupervisedExecutionResult | None:
        try:
            if not record.parent_connection.poll():
                return None

            result = record.parent_connection.recv()
        except EOFError as exc:
            return SupervisedExecutionResult(
                job_id=record.job_id,
                status=SupervisedExecutionStatus.FAILED,
                message="Supervised process closed its result pipe without sending a result.",
                error_type=type(exc).__name__,
            )
        except OSError as exc:
            return SupervisedExecutionResult(
                job_id=record.job_id,
                status=SupervisedExecutionStatus.FAILED,
                message=f"Cannot read supervised execution result: {exc}",
                error_type=type(exc).__name__,
            )

        if not isinstance(result, SupervisedExecutionResult):
            return SupervisedExecutionResult(
                job_id=record.job_id,
                status=SupervisedExecutionStatus.FAILED,
                message=(
                    "Supervised process returned unexpected result type: "
                    f"{type(result).__name__}"
                ),
                error_type="UnexpectedSupervisedExecutionResult",
            )

        if result.job_id != record.job_id:
            return SupervisedExecutionResult(
                job_id=record.job_id,
                status=SupervisedExecutionStatus.FAILED,
                message=(
                    "Supervised process returned result for unexpected jobId: "
                    f"expected='{record.job_id}', actual='{result.job_id}'."
                ),
                error_type="UnexpectedSupervisedExecutionJobId",
            )

        return result

    @staticmethod
    def _close_supervised_execution_resources(record: ExecutionRecord) -> None:
        try:
            record.parent_connection.close()
        except Exception:
            logger.exception(
                "Failed to close supervised execution parent connection.",
                extra={
                    "component": "execution_watcher",
                    "event": "supervised_execution_parent_connection_close_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )

        try:
            record.process.join(timeout=0)
        except Exception:
            logger.exception(
                "Failed to join supervised execution process.",
                extra={
                    "component": "execution_watcher",
                    "event": "supervised_execution_process_join_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )

    def _publish_in_progress_if_due(
        self,
        record: ExecutionRecord,
        now: datetime,
    ) -> None:
        if not self._is_progress_interval_elapsed(record, now):
            return

        progress = self._calculate_in_progress_value(record, now)
        message = f"Job execution is still in progress: {progress}%."

        try:
            self._status_publisher.publish_in_progress(
                user_id=record.user_id,
                job_id=record.job_id,
                job_type=record.job_type,
                worker_id=self._worker_id,
                progress=progress,
                message=message,
            )
        except Exception:
            logger.exception(
                "Failed to publish IN_PROGRESS status update.",
                extra={
                    "component": "execution_watcher",
                    "event": "in_progress_status_publication_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                    "progress": progress,
                },
            )
            return

        self._last_progress_published_at_by_job_id[record.job_id] = now

        logger.info(
            "Periodic IN_PROGRESS status update published.",
            extra={
                "component": "execution_watcher",
                "event": "periodic_in_progress_status_published",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "progress": progress,
            },
        )

    def _is_progress_interval_elapsed(
        self,
        record: ExecutionRecord,
        now: datetime,
    ) -> bool:
        last_published_at = self._last_progress_published_at_by_job_id.get(
            record.job_id
        )
        reference_time = last_published_at or record.started_at

        elapsed_seconds = (now - reference_time).total_seconds()
        return elapsed_seconds >= self._progress_interval_seconds

    def _calculate_in_progress_value(
        self,
        record: ExecutionRecord,
        now: datetime,
    ) -> int:
        elapsed_seconds = max(0.0, (now - record.started_at).total_seconds())
        raw_progress = math.floor(
            elapsed_seconds / self._job_timeout_seconds * DONE_PROGRESS
        )

        return min(
            MAX_IN_PROGRESS_PROGRESS,
            max(MIN_IN_PROGRESS_PROGRESS, raw_progress),
        )

    def _handle_succeeded(
        self,
        record: ExecutionRecord,
        result: SupervisedExecutionResult,
        now: datetime,
    ) -> None:
        try:
            self._output_artifact_uploader.upload(record.context)
        except Exception as exc:
            self._handle_output_upload_failed(record, exc, now)
            return

        message = result.message or "Execution succeeded."

        claimed_record = self._execution_registry.try_claim_terminal(
            job_id=record.job_id,
            terminal_status=WorkerJobStatus.DONE,
            message=message,
            finished_at=now,
        )

        if claimed_record is None:
            self._log_terminal_already_claimed(record, WorkerJobStatus.DONE)
            return

        self._status_publisher.publish_done(
            user_id=claimed_record.user_id,
            job_id=claimed_record.job_id,
            job_type=claimed_record.job_type,
            worker_id=self._worker_id,
            message=message,
        )
        self._execution_registry.mark_terminal_published(claimed_record.job_id)

        self._acknowledge_and_mark_cleanup_ready(claimed_record)
        self._forget_progress_state(claimed_record.job_id)

        logger.info(
            "Supervised execution completed successfully.",
            extra={
                "component": "execution_watcher",
                "event": "supervised_execution_succeeded",
                "jobId": claimed_record.job_id,
                "userId": claimed_record.user_id,
                "jobType": claimed_record.job_type,
                "workerId": claimed_record.worker_id,
                "status": WorkerJobStatus.DONE.value,
                "progress": DONE_PROGRESS,
            },
        )

    def _handle_failed(
        self,
        record: ExecutionRecord,
        result: SupervisedExecutionResult,
        now: datetime,
    ) -> None:
        error_message = self._format_execution_error(result)

        self._finalize_as_error(
            record=record,
            message=error_message,
            event="supervised_execution_failed",
            finished_at=now,
        )

    def _handle_output_upload_failed(
        self,
        record: ExecutionRecord,
        exc: Exception,
        now: datetime,
    ) -> None:
        message = (
            "Output artifact upload failed after supervised execution succeeded: "
            f"{exc}"
        )

        self._finalize_as_error(
            record=record,
            message=message,
            event="output_artifact_upload_failed",
            finished_at=now,
        )

    def _handle_unknown_result_status(
        self,
        record: ExecutionRecord,
        result: SupervisedExecutionResult,
        now: datetime,
    ) -> None:
        message = f"Unknown supervised execution status: {result.status}"

        self._finalize_as_error(
            record=record,
            message=message,
            event="unknown_supervised_execution_status",
            finished_at=now,
        )

    def _finalize_as_error(
        self,
        record: ExecutionRecord,
        message: str,
        event: str,
        finished_at: datetime,
    ) -> None:
        claimed_record = self._execution_registry.try_claim_terminal(
            job_id=record.job_id,
            terminal_status=WorkerJobStatus.ERROR,
            message=message,
            finished_at=finished_at,
        )

        if claimed_record is None:
            self._log_terminal_already_claimed(record, WorkerJobStatus.ERROR)
            return

        self._status_publisher.publish_error(
            user_id=claimed_record.user_id,
            job_id=claimed_record.job_id,
            job_type=claimed_record.job_type,
            worker_id=self._worker_id,
            message=message,
        )
        self._execution_registry.mark_terminal_published(claimed_record.job_id)

        self._acknowledge_and_mark_cleanup_ready(claimed_record)
        self._forget_progress_state(claimed_record.job_id)

        logger.info(
            "Supervised execution finalized as ERROR.",
            extra={
                "component": "execution_watcher",
                "event": event,
                "jobId": claimed_record.job_id,
                "userId": claimed_record.user_id,
                "jobType": claimed_record.job_type,
                "workerId": claimed_record.worker_id,
                "status": WorkerJobStatus.ERROR.value,
                "progress": ERROR_PROGRESS,
                "errorCode": event,
            },
        )

    def _acknowledge_and_mark_cleanup_ready(self, record: ExecutionRecord) -> None:
        record.submitted_ack.ack()
        self._execution_registry.mark_acknowledged(record.job_id)
        self._execution_registry.mark_cleanup_ready(record.job_id)

        logger.info(
            "Submitted job message acknowledged and execution record marked cleanup-ready.",
            extra={
                "component": "execution_watcher",
                "event": "submitted_job_acknowledged_cleanup_ready",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

    def _forget_progress_state(self, job_id: str) -> None:
        self._last_progress_published_at_by_job_id.pop(job_id, None)

    @staticmethod
    def _format_execution_error(result: SupervisedExecutionResult) -> str:
        message = result.message or "Supervised execution failed."
        if result.error_type is None or result.error_type.strip() == "":
            return message

        return f"{result.error_type}: {message}"

    @staticmethod
    def _log_terminal_already_claimed(
        record: ExecutionRecord,
        attempted_status: WorkerJobStatus,
    ) -> None:
        logger.info(
            "Terminal status was already claimed; execution watcher skips terminal publication.",
            extra={
                "component": "execution_watcher",
                "event": "terminal_status_already_claimed",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "status": attempted_status.value,
            },
        )
