# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
import logging
import threading

from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_WATCHER_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_TERMINATED_PROCESS_JOIN_TIMEOUT_SECONDS = 1.0


class TimeoutWatcher:
    """Terminates executions that exceed configured runtime timeout.

    TimeoutWatcher is a Worker Runtime lifecycle component.

    It enforces the configured maximum execution time for supervised job
    processes. Timeout is treated as runtime failure and is finalized as ERROR.

    It does not delete local job workspaces and does not remove records from
    ExecutionRegistry. CleanupWatcher owns local workspace deletion and registry
    record removal after terminal publication and acknowledgement are complete.
    """

    def __init__(
        self,
        execution_registry: ExecutionRegistry,
        status_publisher: StatusPublisher,
        worker_id: str,
        job_timeout_seconds: float,
        poll_interval_seconds: float = DEFAULT_TIMEOUT_WATCHER_POLL_INTERVAL_SECONDS,
        terminated_process_join_timeout_seconds: float = (
            DEFAULT_TERMINATED_PROCESS_JOIN_TIMEOUT_SECONDS
        ),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if status_publisher is None:
            raise ValueError("status_publisher cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")
        if job_timeout_seconds <= 0:
            raise ValueError("job_timeout_seconds must be greater than zero.")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than zero.")
        if terminated_process_join_timeout_seconds < 0:
            raise ValueError(
                "terminated_process_join_timeout_seconds must not be negative."
            )
        if clock is None:
            raise ValueError("clock cannot be null.")

        self._execution_registry = execution_registry
        self._status_publisher = status_publisher
        self._worker_id = worker_id.strip()
        self._job_timeout_seconds = job_timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._terminated_process_join_timeout_seconds = (
            terminated_process_join_timeout_seconds
        )
        self._clock = clock

        self._stop_requested = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start background timeout watcher thread."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop_requested.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="mdds-timeout-watcher",
                daemon=True,
            )
            self._thread.start()

        logger.info(
            "Timeout watcher started.",
            extra={
                "component": "timeout_watcher",
                "event": "timeout_watcher_started",
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
            "Timeout watcher stopped.",
            extra={
                "component": "timeout_watcher",
                "event": "timeout_watcher_stopped",
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

    def _process_record(self, record: ExecutionRecord | None, now: datetime) -> None:
        if record is None:
            return

        if not self._is_timeout_candidate(record, now):
            return

        try:
            self._finalize_timed_out_execution(record, now)
        except Exception:
            logger.exception(
                "Timed-out execution finalization failed; record will remain in registry.",
                extra={
                    "component": "timeout_watcher",
                    "event": "timed_out_execution_finalization_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )

    def _is_timeout_candidate(self, record: ExecutionRecord, now: datetime) -> bool:
        with record.lock:
            if record.terminal_status_claimed:
                logger.debug(
                    "Execution record is already terminal-claimed; timeout watcher skips it.",
                    extra={
                        "component": "timeout_watcher",
                        "event": "execution_record_already_terminal_claimed",
                        "jobId": record.job_id,
                        "userId": record.user_id,
                        "jobType": record.job_type,
                        "workerId": record.worker_id,
                    },
                )
                return False

            started_at = record.started_at

        elapsed_seconds = (now - started_at).total_seconds()
        if elapsed_seconds < self._job_timeout_seconds:
            return False

        if not self._is_supervised_process_alive(record):
            logger.debug(
                "Execution exceeded timeout but supervised process is not alive; execution watcher should finalize it.",
                extra={
                    "component": "timeout_watcher",
                    "event": "timed_out_process_not_alive",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )
            return False

        return True

    @staticmethod
    def _is_supervised_process_alive(record: ExecutionRecord) -> bool:
        try:
            return bool(record.process.is_alive())
        except Exception:
            logger.exception(
                "Failed to inspect supervised process liveness.",
                extra={
                    "component": "timeout_watcher",
                    "event": "supervised_process_liveness_check_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )
            return False

    def _finalize_timed_out_execution(
        self,
        record: ExecutionRecord,
        now: datetime,
    ) -> None:
        message = self._timeout_message(record)

        claimed_record = self._execution_registry.try_claim_terminal(
            job_id=record.job_id,
            terminal_status=WorkerJobStatus.ERROR,
            message=message,
            finished_at=now,
        )

        if claimed_record is None:
            self._log_terminal_already_claimed(record)
            return

        self._terminate_supervised_process(claimed_record)
        self._close_supervised_execution_resources(claimed_record)

        self._status_publisher.publish_error(
            user_id=claimed_record.user_id,
            job_id=claimed_record.job_id,
            job_type=claimed_record.job_type,
            worker_id=self._worker_id,
            message=message,
        )
        self._execution_registry.mark_terminal_published(claimed_record.job_id)

        self._acknowledge_and_mark_cleanup_ready(claimed_record)

        logger.info(
            "Timed-out execution finalized as ERROR.",
            extra={
                "component": "timeout_watcher",
                "event": "timed_out_execution_finalized_as_error",
                "jobId": claimed_record.job_id,
                "userId": claimed_record.user_id,
                "jobType": claimed_record.job_type,
                "workerId": claimed_record.worker_id,
                "status": WorkerJobStatus.ERROR.value,
                "errorCode": "execution_timeout",
            },
        )

    def _terminate_supervised_process(self, record: ExecutionRecord) -> None:
        logger.warning(
            "Execution timeout detected; terminating supervised process.",
            extra={
                "component": "timeout_watcher",
                "event": "execution_timeout_detected",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

        try:
            if record.process.is_alive():
                record.process.terminate()

            record.process.join(timeout=self._terminated_process_join_timeout_seconds)

            if record.process.is_alive():
                raise RuntimeError(
                    "Timed-out supervised process is still alive after terminate "
                    f"and join: jobId='{record.job_id}'."
                )
        except Exception:
            logger.exception(
                "Failed to terminate timed-out supervised process.",
                extra={
                    "component": "timeout_watcher",
                    "event": "timed_out_process_termination_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )
            raise

        logger.info(
            "Timed-out supervised process terminated.",
            extra={
                "component": "timeout_watcher",
                "event": "timed_out_process_terminated",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

    @staticmethod
    def _close_supervised_execution_resources(record: ExecutionRecord) -> None:
        try:
            record.parent_connection.close()
        except Exception:
            logger.exception(
                "Failed to close timed-out execution parent connection.",
                extra={
                    "component": "timeout_watcher",
                    "event": "timed_out_parent_connection_close_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )

    def _acknowledge_and_mark_cleanup_ready(self, record: ExecutionRecord) -> None:
        record.submitted_ack.ack()
        self._execution_registry.mark_acknowledged(record.job_id)
        self._execution_registry.mark_cleanup_ready(record.job_id)

        logger.info(
            "Timed-out submitted job message acknowledged and execution record marked cleanup-ready.",
            extra={
                "component": "timeout_watcher",
                "event": "timed_out_job_acknowledged_cleanup_ready",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

    def _timeout_message(self, record: ExecutionRecord) -> str:
        return (
            "Job execution exceeded runtime timeout and was terminated: "
            f"jobId='{record.job_id}', timeoutSeconds={self._job_timeout_seconds:g}."
        )

    @staticmethod
    def _log_terminal_already_claimed(record: ExecutionRecord) -> None:
        logger.info(
            "Terminal status was already claimed; timeout watcher skips timeout finalization.",
            extra={
                "component": "timeout_watcher",
                "event": "terminal_status_already_claimed",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "status": WorkerJobStatus.ERROR.value,
            },
        )

    def _run_loop(self) -> None:
        while not self._stop_requested.is_set():
            try:
                self.poll_once()
            except Exception:
                logger.exception(
                    "Timeout watcher polling iteration failed.",
                    extra={
                        "component": "timeout_watcher",
                        "event": "timeout_watcher_poll_failed",
                        "workerId": self._worker_id,
                    },
                )

            self._stop_requested.wait(self._poll_interval_seconds)
