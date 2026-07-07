# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import logging

from mdds_worker_runtime.dto.messages import JobStatusUpdateDTO
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.queue.queue_client import QueueClient, QueueMessage

logger = logging.getLogger(__name__)


class StatusPublisher:
    """Publishes worker status update messages to the worker status queue.

    This class is only a transport-level helper. It does not decide lifecycle
    transitions, terminal ownership, message acknowledgement, or retry policy.
    Those decisions belong to the Worker Runtime job state transition
    coordinator and its callers.
    """

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

    def publish_inputs_prepared(
        self,
        workspace: JobWorkspace,
        message: str = "Worker prepared job inputs.",
    ) -> None:
        """Publish public INPUTS_PREPARED status for a prepared job."""
        self._publish(
            workspace=workspace,
            status=WorkerJobStatus.INPUTS_PREPARED,
            progress=0,
            message=message,
        )

    def publish_validated(
        self,
        workspace: JobWorkspace,
        message: str = "Worker-side semantic validation completed.",
    ) -> None:
        """Publish public VALIDATED status after semantic validation succeeds."""
        self._publish(
            workspace=workspace,
            status=WorkerJobStatus.VALIDATED,
            progress=0,
            message=message,
        )

    def publish_in_progress(
        self,
        workspace: JobWorkspace,
        progress: int,
        message: str,
    ) -> None:
        self._publish(
            workspace=workspace,
            status=WorkerJobStatus.IN_PROGRESS,
            progress=progress,
            message=message,
        )

    def publish_done(
        self,
        workspace: JobWorkspace,
        message: str = "Job completed successfully.",
    ) -> None:
        self._publish(
            workspace=workspace,
            status=WorkerJobStatus.DONE,
            progress=100,
            message=message,
        )

    def publish_error(
        self,
        workspace: JobWorkspace,
        message: str,
    ) -> None:
        self._publish(
            workspace=workspace,
            status=WorkerJobStatus.ERROR,
            progress=100,
            message=message,
        )

    def publish_cancelled(
        self,
        workspace: JobWorkspace,
        message: str,
    ) -> None:
        self._publish(
            workspace=workspace,
            status=WorkerJobStatus.CANCELLED,
            progress=100,
            message=message,
        )

    def _publish(
        self,
        workspace: JobWorkspace,
        status: WorkerJobStatus,
        progress: int,
        message: str,
    ) -> None:
        _validate_required_fields(
            workspace=workspace,
            progress=progress,
            message=message,
        )
        status_update_dto = JobStatusUpdateDTO(
            jobId=workspace.job_id.strip(),
            workerId=workspace.worker_id.strip(),
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
                "jobId": workspace.job_id,
                "userId": workspace.user_id,
                "jobType": workspace.job_type,
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
    workspace: JobWorkspace,
    progress: int,
    message: str,
) -> None:
    job_id = workspace.job_id
    job_type = workspace.job_type
    worker_id = workspace.worker_id
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
