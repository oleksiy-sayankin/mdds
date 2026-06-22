# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from unittest.mock import MagicMock

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.queue.queue_client import QueueMessage


def test_job_consumer_placeholder_accepts_job_message_dto() -> None:
    consumer = JobConsumer()
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        ),
    )
    acknowledger = MagicMock()

    consumer.handle(message, acknowledger)

    acknowledger.ack.assert_not_called()
    acknowledger.nack.assert_not_called()
