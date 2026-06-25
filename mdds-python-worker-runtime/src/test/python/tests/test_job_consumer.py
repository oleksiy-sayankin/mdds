# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.queue.queue_client import QueueMessage


def test_job_consumer_loads_manifest_prepares_inputs_and_does_not_ack_yet() -> None:
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    input_artifact_preparer = MagicMock()
    job_execution_context_factory = MagicMock()
    consumer = JobConsumer(
        manifest_loader, input_artifact_preparer, job_execution_context_factory
    )

    ack = MagicMock()
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    manifest_loader.load.assert_called_once_with("jobs/42/job-1/manifest.json")
    input_artifact_preparer.prepare.assert_called_once_with(
        42,
        "job-1",
        manifest.inputs,
    )
    job_execution_context_factory.create.assert_called_once_with(
        manifest,
        input_artifact_preparer.prepare.return_value,
    )
    ack.ack.assert_not_called()
    ack.nack.assert_not_called()


def test_job_consumer_rejects_null_manifest_loader() -> None:
    with pytest.raises(ValueError, match="manifest_loader cannot be null"):
        JobConsumer(None, None, MagicMock())


def test_job_consumer_rejects_null_input_artifact_preparer() -> None:
    with pytest.raises(ValueError, match="input_artifact_preparer cannot be null"):
        JobConsumer(MagicMock(), None, MagicMock())


def test_job_consumer_rejects_null_context_factory() -> None:
    with pytest.raises(ValueError, match="context_factory cannot be null"):
        JobConsumer(MagicMock(), MagicMock(), None)
