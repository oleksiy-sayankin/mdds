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


class CancellationRequestNotAccepted(RuntimeError):
    """Raised when this Worker cannot accept a cancellation request."""


class CancellationRequestHandler:
    """Executes local Worker Runtime cancellation requests.

    The cancellation message consumer owns transport concerns: reading the
    cancellation message, validating its payload, requesting the
    ``IN_PROGRESS`` -> ``CANCELLED`` worker-local terminal transition, and acknowledging
    the cancellation message itself.

    This handler owns the local cancellation execution after the cancellation
    request has been accepted for this Worker:

    - find the local execution record;
    - stop the supervised process;
    - publish terminal ``CANCELLED``;

    This handler does not commit ``CANCELLED``.
    It only performs cancellation side effects used by CancelConsumer's terminal transition operation.
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

    def finalize_cancellation(self, job_id: str) -> None:
        """
        Commit terminal ``CANCELLED``.
        """
        resolved_job_id = self._validate_and_normalize_job_id(job_id)

        record = self._execution_registry.get(resolved_job_id)
        if record is None:
            logger.error(
                "Cancellation request ignored because local execution record was not found.",
                extra={
                    "component": "cancellation_request_handler",
                    "event": "cancellation_record_not_found",
                    "jobId": job_id,
                    "workerId": self._worker_id,
                },
            )
            raise CancellationRequestNotAccepted(
                "Cancellation request cannot be accepted by Worker."
            )

        message = self._cancelled_message(record)
        self._stop_supervised_process(record)
        self._close_supervised_execution_resources(record)

        self._status_publisher.publish_cancelled(
            workspace=record.workspace,
            message=message,
        )

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

    @staticmethod
    def _cancelled_message(record: ExecutionRecord) -> str:
        return f"Job cancellation requested and applied: jobId='{record.job_id}'."

    @staticmethod
    def _validate_and_normalize_job_id(job_id: str) -> str:
        if job_id is None or job_id.strip() == "":
            raise ValueError("job_id cannot be null or blank.")

        return job_id.strip()
