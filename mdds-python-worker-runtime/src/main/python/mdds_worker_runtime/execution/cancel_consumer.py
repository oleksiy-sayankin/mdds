# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import logging

from mdds_worker_runtime.dto.messages import CancelJobDTO
from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.queue.queue_client import (
    Acknowledger,
    MessageHandler,
    QueueMessage,
)

logger = logging.getLogger(__name__)


class CancelConsumer(MessageHandler[CancelJobDTO]):
    """Consumes worker-specific job cancellation messages.

    CancelConsumer belongs to the Worker Runtime control plane.

    Its responsibility is intentionally narrow:

    - read cancellation messages from the worker cancellation queue;
    - validate that jobId is present;
    - pass the accepted request to an internal cancellation handler;
    - acknowledge the cancellation message after local acceptance.

    It does not terminate supervised processes, does not publish CANCELLED,
    does not acknowledge the original submitted job message, and does not mark
    execution records cleanup-ready. Those actions belong to the cancellation
    execution flow.
    """

    def __init__(
        self,
        cancellation_request_handler: CancellationRequestHandler,
        worker_id: str,
    ) -> None:
        if cancellation_request_handler is None:
            raise ValueError("cancellation_request_handler cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")

        self._cancellation_request_handler = cancellation_request_handler
        self._worker_id = worker_id.strip()

    def handle(
        self,
        message: QueueMessage[CancelJobDTO],
        ack: Acknowledger,
    ) -> None:
        """Handle one cancellation message.

        The cancellation message is acknowledged only after the runtime accepts
        it for local processing.

        If validation or delegation fails, the exception is intentionally allowed
        to propagate to the queue client. The queue client owns the generic
        handler-error nack policy.
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

        self._cancellation_request_handler.request_cancellation(job_id)
        ack.ack()

        logger.info(
            "Cancellation message accepted and acknowledged.",
            extra={
                "component": "cancel_consumer",
                "event": "cancellation_message_accepted",
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
