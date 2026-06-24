# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.queue.queue_client import QueueMessage


def test_job_consumer_loads_manifest_by_object_key_and_does_not_ack_yet() -> None:
    manifest_loader = MagicMock()
    consumer = JobConsumer(manifest_loader)

    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        ),
    )
    acknowledger = MagicMock()

    consumer.handle(message, acknowledger)

    manifest_loader.load.assert_called_once_with("jobs/42/job-1/manifest.json")
    acknowledger.ack.assert_not_called()
    acknowledger.nack.assert_not_called()


def test_job_consumer_rejects_null_manifest_loader() -> None:
    with pytest.raises(ValueError, match="manifest_loader cannot be null"):
        JobConsumer(None)
