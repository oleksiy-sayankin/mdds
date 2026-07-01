# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.execution.job_preparation_handler import (
    JobPreparationHandler,
    PreparedJob,
)
from mdds_worker_runtime.queue.queue_client import QueueMessage


def test_job_consumer_loads_manifest_prepares_context_delegates_validation_and_starts_execution() -> (
    None
):
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    prepared_job_inputs = MagicMock()

    input_artifact_preparer = MagicMock()
    input_artifact_preparer.prepare.return_value = prepared_job_inputs

    context = MagicMock()

    job_execution_context_factory = MagicMock()
    job_execution_context_factory.create.return_value = context

    job_handler = MagicMock()

    job_handler_loader = MagicMock()
    job_handler_loader.load.return_value = job_handler

    validation_handler = MagicMock()
    validation_handler.validate_or_handle_failure.return_value = True

    execution_record = MagicMock()

    execution_supervisor = MagicMock()
    execution_supervisor.start.return_value = execution_record

    execution_registry = MagicMock()
    status_publisher = MagicMock()
    workspace_cleaner = MagicMock()
    preparation_handler = JobPreparationHandler(
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
        status_publisher,
        workspace_cleaner,
        "test-worker-id",
    )

    consumer = JobConsumer(
        manifest_loader,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
        "test-worker-id",
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
        prepared_job_inputs,
    )
    job_handler_loader.load.assert_called_once_with()

    validation_handler.validate_or_handle_failure.assert_called_once_with(
        handler=job_handler,
        context=context,
        manifest=manifest,
        submitted_ack=ack,
    )

    job_handler.validate.assert_not_called()

    status_publisher.publish_in_progress.assert_called_once_with(
        42,
        "job-1",
        "SOLVING_SLAE",
        "test-worker-id",
        0,
        "Start job execution",
    )

    execution_supervisor.start.assert_called_once()
    supervised_execution_request = execution_supervisor.start.call_args.args[0]

    assert supervised_execution_request.context is context
    assert supervised_execution_request.worker_id == "test-worker-id"
    assert (
        supervised_execution_request.manifest_object_key
        == "jobs/42/job-1/manifest.json"
    )
    assert supervised_execution_request.manifest is manifest
    assert supervised_execution_request.submitted_ack is ack

    execution_registry.add.assert_called_once_with(execution_record)

    ack.ack.assert_not_called()
    ack.nack.assert_not_called()


def test_job_consumer_rejects_null_manifest_loader() -> None:
    with pytest.raises(ValueError, match="manifest_loader cannot be null"):
        JobConsumer(
            None,
            None,
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            "test-worker-id",
        )


def test_job_consumer_rejects_null_validation_handler() -> None:
    with pytest.raises(ValueError, match="validation_handler cannot be null"):
        JobConsumer(
            MagicMock(),
            MagicMock(),
            None,
            MagicMock(),
            MagicMock(),
            MagicMock(),
            "test-worker-id",
        )


def test_job_consumer_rejects_null_execution_registry() -> None:
    with pytest.raises(ValueError, match="execution_registry cannot be null"):
        JobConsumer(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            None,
            MagicMock(),
            "test-worker-id",
        )


def test_job_consumer_rejects_null_status_publisher() -> None:
    with pytest.raises(ValueError, match="status_publisher cannot be null"):
        JobConsumer(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            None,
            "test-worker-id",
        )


def test_job_consumer_delegates_validation_and_continues_happy_path() -> None:
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    prepared_job_inputs = MagicMock()

    input_artifact_preparer = MagicMock()
    input_artifact_preparer.prepare.return_value = prepared_job_inputs

    context = MagicMock()

    job_execution_context_factory = MagicMock()
    job_execution_context_factory.create.return_value = context

    job_handler = MagicMock()

    job_handler_loader = MagicMock()
    job_handler_loader.load.return_value = job_handler

    validation_handler = MagicMock()
    validation_handler.validate_or_handle_failure.return_value = True

    execution_record = MagicMock()

    execution_supervisor = MagicMock()
    execution_supervisor.start.return_value = execution_record

    execution_registry = MagicMock()
    status_publisher = MagicMock()
    workspace_cleaner = MagicMock()

    preparation_handler = JobPreparationHandler(
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
        status_publisher,
        workspace_cleaner,
        "test-worker-id",
    )

    consumer = JobConsumer(
        manifest_loader,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
        "test-worker-id",
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
        prepared_job_inputs,
    )

    job_handler_loader.load.assert_called_once_with()

    validation_handler.validate_or_handle_failure.assert_called_once_with(
        handler=job_handler,
        context=context,
        manifest=manifest,
        submitted_ack=ack,
    )

    job_handler.validate.assert_not_called()

    status_publisher.publish_in_progress.assert_called_once_with(
        42,
        "job-1",
        "SOLVING_SLAE",
        "test-worker-id",
        0,
        "Start job execution",
    )

    execution_supervisor.start.assert_called_once()

    supervised_execution_request = execution_supervisor.start.call_args.args[0]

    assert supervised_execution_request.context is context
    assert supervised_execution_request.worker_id == "test-worker-id"
    assert (
        supervised_execution_request.manifest_object_key
        == "jobs/42/job-1/manifest.json"
    )
    assert supervised_execution_request.manifest is manifest
    assert supervised_execution_request.submitted_ack is ack

    execution_registry.add.assert_called_once_with(execution_record)

    ack.ack.assert_not_called()
    ack.nack.assert_not_called()


def test_job_consumer_returns_early_when_validation_handler_handles_failure() -> None:
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    input_artifact_preparer = MagicMock()
    job_execution_context_factory = MagicMock()

    job_handler = MagicMock()

    job_handler_loader = MagicMock()
    job_handler_loader.load.return_value = job_handler

    validation_handler = MagicMock()
    validation_handler.validate_or_handle_failure.return_value = False

    execution_supervisor = MagicMock()
    execution_registry = MagicMock()
    status_publisher = MagicMock()
    workspace_cleaner = MagicMock()

    preparation_handler = JobPreparationHandler(
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
        status_publisher,
        workspace_cleaner,
        "test-worker-id",
    )

    consumer = JobConsumer(
        manifest_loader,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
        "test-worker-id",
    )

    ack = MagicMock()
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    validation_handler.validate_or_handle_failure.assert_called_once_with(
        handler=job_handler,
        context=job_execution_context_factory.create.return_value,
        manifest=manifest,
        submitted_ack=ack,
    )

    status_publisher.publish_in_progress.assert_not_called()
    execution_supervisor.start.assert_not_called()
    execution_registry.add.assert_not_called()

    job_handler.validate.assert_not_called()

    ack.ack.assert_not_called()
    ack.nack.assert_not_called()


def test_job_consumer_returns_early_when_job_preparation_handler_handles_failure() -> (
    None
):
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    preparation_handler = MagicMock(spec=JobPreparationHandler)
    preparation_handler.prepare_or_handle_failure.return_value = None

    validation_handler = MagicMock()
    execution_supervisor = MagicMock()
    execution_registry = MagicMock()
    status_publisher = MagicMock()

    consumer = JobConsumer(
        manifest_loader,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
        "test-worker-id",
    )

    ack = MagicMock()
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    manifest_loader.load.assert_called_once_with("jobs/42/job-1/manifest.json")

    preparation_handler.prepare_or_handle_failure.assert_called_once_with(
        manifest_object_key="jobs/42/job-1/manifest.json",
        manifest=manifest,
        submitted_ack=ack,
    )

    validation_handler.validate_or_handle_failure.assert_not_called()

    status_publisher.publish_in_progress.assert_not_called()
    execution_supervisor.start.assert_not_called()
    execution_registry.add.assert_not_called()

    ack.ack.assert_not_called()
    ack.nack.assert_not_called()


def test_job_consumer_registers_execution_before_publishing_in_progress() -> None:
    events: list[str] = []

    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    context = MagicMock()
    job_handler = MagicMock()

    preparation_handler = MagicMock(spec=JobPreparationHandler)
    preparation_handler.prepare_or_handle_failure.return_value = PreparedJob(
        context=context,
        handler=job_handler,
    )

    validation_handler = MagicMock()
    validation_handler.validate_or_handle_failure.return_value = True

    execution_record = MagicMock()

    execution_supervisor = MagicMock()

    def start_execution(_request):
        events.append("start")
        return execution_record

    execution_supervisor.start.side_effect = start_execution

    execution_registry = MagicMock()

    def register_execution(record):
        assert record is execution_record
        events.append("register")

    execution_registry.add.side_effect = register_execution

    status_publisher = MagicMock()

    def publish_in_progress(*_args):
        events.append("publish_in_progress")

    status_publisher.publish_in_progress.side_effect = publish_in_progress

    consumer = JobConsumer(
        manifest_loader,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
        "test-worker-id",
    )

    ack = MagicMock()
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    assert events == [
        "start",
        "register",
        "publish_in_progress",
    ]

    preparation_handler.prepare_or_handle_failure.assert_called_once_with(
        manifest_object_key="jobs/42/job-1/manifest.json",
        manifest=manifest,
        submitted_ack=ack,
    )

    validation_handler.validate_or_handle_failure.assert_called_once_with(
        handler=job_handler,
        context=context,
        manifest=manifest,
        submitted_ack=ack,
    )

    status_publisher.publish_in_progress.assert_called_once_with(
        42,
        "job-1",
        "SOLVING_SLAE",
        "test-worker-id",
        0,
        "Start job execution",
    )

    execution_supervisor.start.assert_called_once()
    execution_registry.add.assert_called_once_with(execution_record)

    ack.ack.assert_not_called()
    ack.nack.assert_not_called()
