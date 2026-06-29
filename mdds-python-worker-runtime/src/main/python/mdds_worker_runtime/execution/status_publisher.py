# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import logging

from mdds_worker_runtime.dto.messages import JobStatusUpdateDTO
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.queue.queue_client import QueueClient, QueueMessage

logger = logging.getLogger(__name__)


class StatusPublisher:
    """Publishes worker status update messages to the worker status queue."""

    def __init__(
        self,
        worker_status_queue_name: str,
        queue_client: QueueClient | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if queue_client is None:
            raise ValueError("queue_client cannot be null.")
        if worker_status_queue_name is None or worker_status_queue_name.strip() == "":
            raise ValueError("worker_status_queue_name cannot be null or blank.")

        self._worker_status_queue_name = worker_status_queue_name.strip()
        self._queue_client = queue_client
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def publish_in_progress(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        progress: int,
        message: str,
    ) -> None:
        self._publish(
            user_id=user_id,
            job_id=job_id,
            job_type=job_type,
            worker_id=worker_id,
            status=WorkerJobStatus.IN_PROGRESS,
            progress=progress,
            message=message,
        )

    def publish_done(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str = "Job completed successfully.",
    ) -> None:
        self._publish(
            user_id=user_id,
            job_id=job_id,
            job_type=job_type,
            worker_id=worker_id,
            status=WorkerJobStatus.DONE,
            progress=100,
            message=message,
        )

    def publish_error(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str,
    ) -> None:
        self._publish(
            user_id=user_id,
            job_id=job_id,
            job_type=job_type,
            worker_id=worker_id,
            status=WorkerJobStatus.ERROR,
            progress=100,
            message=message,
        )

    def publish_cancelled(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str,
    ) -> None:
        self._publish(
            user_id=user_id,
            job_id=job_id,
            job_type=job_type,
            worker_id=worker_id,
            status=WorkerJobStatus.CANCELLED,
            progress=100,
            message=message,
        )

    def publish_validation_failed(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str,
    ) -> None:
        self._publish(
            user_id=user_id,
            job_id=job_id,
            job_type=job_type,
            worker_id=worker_id,
            status=WorkerJobStatus.VALIDATION_FAILED,
            progress=0,
            message=message,
        )

    def _publish(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        status: WorkerJobStatus,
        progress: int,
        message: str,
    ) -> None:
        _validate_required_fields(
            job_id=job_id,
            job_type=job_type,
            worker_id=worker_id,
            progress=progress,
            message=message,
        )
        status_update_dto = JobStatusUpdateDTO(
            jobId=job_id.strip(),
            workerId=worker_id.strip(),
            status=status.value,
            progress=progress,
            message=message.strip(),
            eventTime=_format_event_time(self._clock()),
        )

        self._queue_client.publish(
            self._worker_status_queue_name,
            QueueMessage(payload=status_update_dto),
        )

        logger.info(
            "Status update publishing completed.",
            extra={
                "component": "status_publisher",
                "event": "status_update_publishing_completed",
                "jobId": job_id,
                "userId": user_id,
                "jobType": job_type,
                "status": status.value,
                "progress": progress,
                "statusQueueName": self._worker_status_queue_name,
            },
        )


def _format_event_time(event_time: datetime) -> str:
    if event_time.tzinfo is None or event_time.utcoffset() is None:
        raise ValueError("event_time must be timezone-aware.")

    return event_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _validate_required_fields(
    job_id: str,
    job_type: str,
    worker_id: str,
    progress: int,
    message: str,
) -> None:
    if job_id is None or job_id.strip() == "":
        raise ValueError("job_id cannot be null or blank.")
    if job_type is None or job_type.strip() == "":
        raise ValueError("job_type cannot be null or blank.")
    if worker_id is None or worker_id.strip() == "":
        raise ValueError("worker_id cannot be null or blank.")
    if progress < 0 or progress > 100:
        raise ValueError("progress must be between 0 and 100.")
    if message is None or message.strip() == "":
        raise ValueError("message cannot be null or blank.")
