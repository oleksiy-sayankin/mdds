# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.queue.queue_client import QueueMessage


def test_job_consumer_loads_manifest_prepares_inputs_creates_context_and_validates() -> (
    None
):
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    input_artifact_preparer = MagicMock()
    job_execution_context_factory = MagicMock()
    job_handler_loader = MagicMock()

    consumer = JobConsumer(
        manifest_loader,
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
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
    job_handler_loader.load.assert_called_once_with()
    job_handler_loader.load.return_value.validate.assert_called_once_with(
        job_execution_context_factory.create.return_value,
    )
    ack.ack.assert_not_called()
    ack.nack.assert_not_called()


def test_job_consumer_propagates_validation_failure_and_does_not_ack_yet() -> None:
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    input_artifact_preparer = MagicMock()
    job_execution_context_factory = MagicMock()

    job_handler = MagicMock()
    job_handler.validate.side_effect = RuntimeError("validation failed")

    job_handler_loader = MagicMock()
    job_handler_loader.load.return_value = job_handler

    consumer = JobConsumer(
        manifest_loader,
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
    )

    ack = MagicMock()
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    with pytest.raises(RuntimeError, match="validation failed"):
        consumer.handle(message, ack)

    job_handler_loader.load.assert_called_once_with()
    job_handler.validate.assert_called_once_with(
        job_execution_context_factory.create.return_value,
    )
    ack.ack.assert_not_called()
    ack.nack.assert_not_called()


def test_job_consumer_rejects_null_manifest_loader() -> None:
    with pytest.raises(ValueError, match="manifest_loader cannot be null"):
        JobConsumer(None, None, MagicMock(), MagicMock())


def test_job_consumer_rejects_null_input_artifact_preparer() -> None:
    with pytest.raises(ValueError, match="input_artifact_preparer cannot be null"):
        JobConsumer(MagicMock(), None, MagicMock(), MagicMock())


def test_job_consumer_rejects_null_context_factory() -> None:
    with pytest.raises(ValueError, match="context_factory cannot be null"):
        JobConsumer(MagicMock(), MagicMock(), None, MagicMock())


def test_job_consumer_rejects_null_job_handler_loader() -> None:
    with pytest.raises(ValueError, match="job_handler_loader cannot be null"):
        JobConsumer(MagicMock(), MagicMock(), MagicMock(), None)
