# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
import logging

from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher

logger = logging.getLogger(__name__)

DEFAULT_CANCELLED_PROCESS_JOIN_TIMEOUT_SECONDS = 1.0


class CancellationRequestHandler:
    """Executes local Worker Runtime cancellation requests.

    The cancellation message consumer is responsible for transport concerns:
    receiving a cancellation message, validating its payload, delegating the
    accepted request, and acknowledging the cancellation message itself.

    This handler owns runtime lifecycle cancellation:

    - find the local execution record;
    - win the terminal lifecycle race as CANCELLED;
    - stop the supervised process;
    - publish CANCELLED;
    - acknowledge the original submitted job message;
    - mark the execution record cleanup-ready.

    It does not consume RabbitMQ cancellation messages directly, does not remove
    records from ExecutionRegistry, and does not delete local job workspaces.
    CleanupWatcher owns record removal and workspace deletion.
    """

    def __init__(
        self,
        execution_registry: ExecutionRegistry,
        status_publisher: StatusPublisher,
        worker_id: str,
        cancelled_process_join_timeout_seconds: float = (
            DEFAULT_CANCELLED_PROCESS_JOIN_TIMEOUT_SECONDS
        ),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if status_publisher is None:
            raise ValueError("status_publisher cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")
        if cancelled_process_join_timeout_seconds < 0:
            raise ValueError(
                "cancelled_process_join_timeout_seconds must not be negative."
            )
        if clock is None:
            raise ValueError("clock cannot be null.")

        self._execution_registry = execution_registry
        self._status_publisher = status_publisher
        self._worker_id = worker_id.strip()
        self._cancelled_process_join_timeout_seconds = (
            cancelled_process_join_timeout_seconds
        )
        self._clock = clock

    def request_cancellation(self, job_id: str) -> None:
        """Cancel a locally running job if this runtime still owns it.

        Missing records and already-terminal records are ignored because
        cancellation is best-effort. Finalization failures are allowed to
        propagate so the caller can apply its generic message handling policy.
        """
        if job_id is None or job_id.strip() == "":
            raise ValueError("job_id cannot be null or blank.")

        resolved_job_id = job_id.strip()

        logger.info(
            "Cancellation request accepted for local processing.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancellation_request_accepted",
                "jobId": resolved_job_id,
                "workerId": self._worker_id,
            },
        )

        record = self._execution_registry.get(resolved_job_id)
        if record is None:
            self._log_missing_record(resolved_job_id)
            return

        if self._is_terminal_claimed(record):
            self._log_already_terminal(record)
            return

        message = self._cancelled_message(record)

        claimed_record = self._execution_registry.try_claim_terminal(
            job_id=record.job_id,
            terminal_status=WorkerJobStatus.CANCELLED,
            message=message,
            finished_at=self._clock(),
        )

        if claimed_record is None:
            self._log_terminal_claim_failed(record)
            return

        try:
            self._finalize_cancelled_execution(claimed_record, message)
        except Exception:
            logger.exception(
                "Cancellation finalization failed; record will remain in registry.",
                extra={
                    "component": "cancellation_request_handler",
                    "event": "cancellation_finalization_failed",
                    "jobId": claimed_record.job_id,
                    "userId": claimed_record.user_id,
                    "jobType": claimed_record.job_type,
                    "workerId": claimed_record.worker_id,
                    "status": WorkerJobStatus.CANCELLED.value,
                },
            )
            raise

    @staticmethod
    def _is_terminal_claimed(record: ExecutionRecord) -> bool:
        with record.lock:
            return record.terminal_status_claimed

    def _finalize_cancelled_execution(
        self,
        record: ExecutionRecord,
        message: str,
    ) -> None:
        self._stop_supervised_process(record)
        self._close_supervised_execution_resources(record)

        self._status_publisher.publish_cancelled(
            user_id=record.user_id,
            job_id=record.job_id,
            job_type=record.job_type,
            worker_id=self._worker_id,
            message=message,
        )
        self._execution_registry.mark_terminal_published(record.job_id)

        self._acknowledge_and_mark_cleanup_ready(record)

        logger.info(
            "Cancellation finalized as CANCELLED.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancellation_finalized_as_cancelled",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "status": WorkerJobStatus.CANCELLED.value,
            },
        )

    def _stop_supervised_process(self, record: ExecutionRecord) -> None:
        logger.info(
            "Stopping supervised process for cancellation.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancelled_process_stop_started",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

        try:
            process = record.process

            if process.is_alive():
                logger.info(
                    "Terminating supervised process for cancellation.",
                    extra={
                        "component": "cancellation_request_handler",
                        "event": "cancelled_process_terminate_started",
                        "jobId": record.job_id,
                        "userId": record.user_id,
                        "jobType": record.job_type,
                        "workerId": record.worker_id,
                    },
                )
                process.terminate()

            process.join(timeout=self._cancelled_process_join_timeout_seconds)

            if process.is_alive():
                self._kill_supervised_process(record)

            if process.is_alive():
                raise RuntimeError(
                    "Cancelled supervised process is still alive after terminate, "
                    f"kill, and join: jobId='{record.job_id}'."
                )
        except Exception:
            logger.exception(
                "Failed to stop supervised process for cancellation.",
                extra={
                    "component": "cancellation_request_handler",
                    "event": "cancelled_process_stop_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                },
            )
            raise

        logger.info(
            "Supervised process stopped for cancellation.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancelled_process_stopped",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

    def _kill_supervised_process(self, record: ExecutionRecord) -> None:
        process = record.process

        if not hasattr(process, "kill"):
            raise RuntimeError(
                "Cancelled supervised process is still alive after terminate "
                f"and does not support kill: jobId='{record.job_id}'."
            )

        logger.warning(
            "Supervised process did not stop after terminate; killing it.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancelled_process_kill_started",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

        process.kill()
        process.join(timeout=self._cancelled_process_join_timeout_seconds)

    @staticmethod
    def _close_supervised_execution_resources(record: ExecutionRecord) -> None:
        try:
            record.parent_connection.close()
        except Exception:
            logger.exception(
                "Failed to close cancelled execution parent connection.",
                extra={
                    "component": "cancellation_request_handler",
                    "event": "cancelled_parent_connection_close_failed",
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
            "Cancelled submitted job message acknowledged and execution record marked cleanup-ready.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancelled_job_acknowledged_cleanup_ready",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
            },
        )

    @staticmethod
    def _cancelled_message(record: ExecutionRecord) -> str:
        return f"Job cancellation requested and applied: jobId='{record.job_id}'."

    def _log_missing_record(self, job_id: str) -> None:
        logger.info(
            "Cancellation request ignored because local execution record was not found.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancellation_record_not_found",
                "jobId": job_id,
                "workerId": self._worker_id,
            },
        )

    @staticmethod
    def _log_already_terminal(record: ExecutionRecord) -> None:
        with record.lock:
            terminal_status = (
                record.terminal_status.value if record.terminal_status else None
            )

        logger.info(
            "Cancellation request ignored because execution record is already terminal-claimed.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancellation_record_already_terminal_claimed",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "status": terminal_status,
            },
        )

    @staticmethod
    def _log_terminal_claim_failed(record: ExecutionRecord) -> None:
        logger.info(
            "Cancellation request lost terminal race; no cancellation finalization will be performed.",
            extra={
                "component": "cancellation_request_handler",
                "event": "cancellation_terminal_claim_failed",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "status": WorkerJobStatus.CANCELLED.value,
            },
        )
