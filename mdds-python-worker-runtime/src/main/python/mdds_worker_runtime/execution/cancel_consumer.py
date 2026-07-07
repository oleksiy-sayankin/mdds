# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import logging

from mdds_worker_runtime.dto.messages import CancelJobDTO
from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)
from mdds_worker_runtime.queue.queue_client import (
    Acknowledger,
    MessageHandler,
    QueueMessage,
)

logger = logging.getLogger(__name__)


class CancelConsumer(MessageHandler[CancelJobDTO]):
    """Consumes worker-specific job cancellation messages.

    CancelConsumer owns the cancellation-message state-machine flow:

    - request the worker-local ``IN_PROGRESS`` -> ``CANCELLED`` transition;
    - acknowledge the cancellation message after the transition is committed or
      determined stale for this Worker.

    The Worker must not publish public ``CANCEL_REQUESTED`` status because that
    public state is controlled by the Web Server. The Worker tries to finalize it as
    terminal ``CANCELLED``.
    """

    def __init__(
        self,
        job_state_transition_coordinator: JobStateTransitionCoordinator,
        cancellation_request_handler: CancellationRequestHandler,
        worker_id: str,
    ) -> None:
        if job_state_transition_coordinator is None:
            raise ValueError("job_state_transition_coordinator cannot be null.")
        if cancellation_request_handler is None:
            raise ValueError("cancellation_request_handler cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")

        self._job_state_transition_coordinator = job_state_transition_coordinator
        self._cancellation_request_handler = cancellation_request_handler
        self._worker_id = worker_id.strip()

    def handle(
        self,
        message: QueueMessage[CancelJobDTO],
        ack: Acknowledger,
    ) -> None:
        """Handle one worker-specific cancellation message.

        The Worker attempts the local terminal cancellation transition first. If the
        transition commits or is stale, the cancellation message is acknowledged. If
        the transition operation fails, the exception is allowed to propagate so the
        queue client can apply its generic message failure policy.
        """
        if ack is None:
            raise ValueError("ack cannot be null.")

        job_id = self._extract_job_id(message)

        logger.info(
            "Cancellation message received.",
            extra={
                "component": "cancel_consumer",
                "event": "cancellation_message_received",
                "jobId": job_id,
                "workerId": self._worker_id,
            },
        )

        result = self._job_state_transition_coordinator.transition(
            job_id=job_id,
            target_state=WorkerJobStatus.CANCELLED,
            operation=lambda: self._cancellation_request_handler.finalize_cancellation(
                job_id=job_id
            ),
        )
        if result.failed:
            raise result.error

        ack.ack()

        logger.info(
            "Cancellation message done and acknowledged.",
            extra={
                "component": "cancel_consumer",
                "event": "cancellation_message_done",
                "jobId": job_id,
                "workerId": self._worker_id,
            },
        )

    @staticmethod
    def _extract_job_id(message: QueueMessage[CancelJobDTO]) -> str:
        if message is None:
            raise ValueError("message cannot be null.")

        payload = message.payload
        if payload is None:
            raise ValueError("cancel message payload cannot be null.")

        job_id = payload.job_id
        if job_id is None or job_id.strip() == "":
            raise ValueError("cancel jobId cannot be null or blank.")

        return job_id.strip()
