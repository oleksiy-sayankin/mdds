# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.execution.job_preparation_handler import (
    JobPreparationHandler,
    PreparedJob,
)
from mdds_worker_runtime.execution.validation_handler import ValidationFailed
from mdds_worker_runtime.job_state import JobStateTransitionCoordinator
from mdds_worker_runtime.queue.queue_client import QueueMessage, Acknowledger


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

    workspace = _workspace(manifest)

    job_workspace_factory = MagicMock()
    job_workspace_factory.create.return_value = workspace

    context = MagicMock()
    context.user_id = 42
    context.job_id = "job-1"
    context.job_type = "SOLVING_SLAE"
    context.workspace = workspace

    job_execution_context_factory = MagicMock()
    job_execution_context_factory.create.return_value = context

    job_handler = MagicMock()

    job_handler_loader = MagicMock()
    job_handler_loader.load.return_value = job_handler

    validation_handler = MagicMock()
    validation_handler.validate.return_value = True

    execution_record = MagicMock()

    execution_supervisor = MagicMock()
    execution_supervisor.start.return_value = execution_record

    execution_registry = MagicMock()
    status_publisher = MagicMock()
    preparation_handler = JobPreparationHandler(
        execution_registry,
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
    )

    job_state_transition_coordinator = JobStateTransitionCoordinator()

    consumer = JobConsumer(
        manifest_loader,
        job_workspace_factory,
        job_state_transition_coordinator,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
    )

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    manifest_loader.load.assert_called_once_with("jobs/42/job-1/manifest.json")
    job_workspace_factory.create.assert_called_once_with(manifest)

    input_artifact_preparer.prepare.assert_called_once_with(workspace)

    job_execution_context_factory.create.assert_called_once_with(
        workspace=workspace,
        prepared_job_inputs=prepared_job_inputs,
    )

    job_handler_loader.load.assert_called_once_with()

    validation_handler.validate.assert_called_once_with(
        handler=job_handler,
        context=context,
    )

    job_handler.validate.assert_not_called()

    status_publisher.publish_in_progress.assert_called_once_with(
        workspace,
        0,
        "Start job execution",
    )

    execution_supervisor.start.assert_called_once()
    supervised_execution_request = execution_supervisor.start.call_args.args[0]

    assert supervised_execution_request.context is context

    execution_registry.attach_context.assert_called_once_with(
        job_id="job-1",
        context=context,
    )

    ack_mock.ack.assert_not_called()
    ack_mock.nack.assert_not_called()


def test_job_consumer_rejects_null_manifest_loader() -> None:
    with pytest.raises(ValueError, match="manifest_loader cannot be null"):
        JobConsumer(
            None,
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )


def test_job_consumer_rejects_null_validation_handler() -> None:
    with pytest.raises(ValueError, match="validation_handler cannot be null"):
        JobConsumer(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            None,
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )


def test_job_consumer_rejects_null_execution_registry() -> None:
    with pytest.raises(ValueError, match="execution_registry cannot be null"):
        JobConsumer(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            None,
            MagicMock(),
        )


def test_job_consumer_rejects_null_status_publisher() -> None:
    with pytest.raises(ValueError, match="status_publisher cannot be null"):
        JobConsumer(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            None,
        )


def test_job_consumer_delegates_validation_and_continues_happy_path() -> None:
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    job_workspace_factory = MagicMock()
    workspace = _workspace(manifest)
    job_workspace_factory.create.return_value = workspace

    job_state_transition_coordinator = JobStateTransitionCoordinator()

    prepared_job_inputs = MagicMock()

    input_artifact_preparer = MagicMock()
    input_artifact_preparer.prepare.return_value = prepared_job_inputs

    context = MagicMock()
    context.user_id = 42
    context.job_id = "job-1"
    context.job_type = "SOLVING_SLAE"
    context.workspace = workspace

    job_execution_context_factory = MagicMock()
    job_execution_context_factory.create.return_value = context

    job_handler = MagicMock()

    job_handler_loader = MagicMock()
    job_handler_loader.load.return_value = job_handler

    validation_handler = MagicMock()
    validation_handler.validate.return_value = True

    execution_record = MagicMock()

    execution_supervisor = MagicMock()
    execution_supervisor.start.return_value = execution_record

    execution_registry = MagicMock()
    status_publisher = MagicMock()

    preparation_handler = JobPreparationHandler(
        execution_registry,
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
    )

    consumer = JobConsumer(
        manifest_loader,
        job_workspace_factory,
        job_state_transition_coordinator,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
    )

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    manifest_loader.load.assert_called_once_with("jobs/42/job-1/manifest.json")

    job_workspace_factory.create.assert_called_once_with(manifest)

    input_artifact_preparer.prepare.assert_called_once_with(workspace)

    job_execution_context_factory.create.assert_called_once_with(
        workspace=workspace,
        prepared_job_inputs=prepared_job_inputs,
    )

    job_handler_loader.load.assert_called_once_with()

    validation_handler.validate.assert_called_once_with(
        handler=job_handler,
        context=context,
    )

    job_handler.validate.assert_not_called()

    status_publisher.publish_in_progress.assert_called_once_with(
        workspace,
        0,
        "Start job execution",
    )

    execution_supervisor.start.assert_called_once()

    supervised_execution_request = execution_supervisor.start.call_args.args[0]

    assert supervised_execution_request.context is context

    execution_registry.attach_context.assert_called_once_with(
        job_id="job-1",
        context=context,
    )
    execution_registry.replace.assert_not_called()

    ack_mock.ack.assert_not_called()
    ack_mock.nack.assert_not_called()


def test_job_consumer_returns_early_when_validation_handler_handles_failure() -> None:
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"
    manifest.inputs = {"matrix": MagicMock()}

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest
    job_workspace_factory = MagicMock()

    job_state_transition_coordinator = JobStateTransitionCoordinator()
    input_artifact_preparer = MagicMock()
    job_execution_context_factory = MagicMock()

    job_handler = MagicMock()

    job_handler_loader = MagicMock()
    job_handler_loader.load.return_value = job_handler

    validation_handler = MagicMock()
    validation_handler.validate.side_effect = ValidationFailed()

    execution_supervisor = MagicMock()
    execution_registry = MagicMock()
    status_publisher = MagicMock()

    workspace = _workspace(manifest)
    job_workspace_factory.create.return_value = workspace

    prepared_job_inputs = MagicMock()
    input_artifact_preparer.prepare.return_value = prepared_job_inputs

    context = MagicMock()
    job_execution_context_factory.create.return_value = context

    preparation_handler = JobPreparationHandler(
        execution_registry,
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
    )

    consumer = JobConsumer(
        manifest_loader,
        job_workspace_factory,
        job_state_transition_coordinator,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
    )

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    validation_handler.validate.assert_called_once_with(
        handler=job_handler,
        context=job_execution_context_factory.create.return_value,
    )

    status_publisher.publish_in_progress.assert_not_called()
    execution_supervisor.start.assert_not_called()
    execution_registry.replace.assert_not_called()

    job_handler.validate.assert_not_called()

    ack_mock.ack.assert_called_once()
    ack_mock.nack.assert_not_called()


def test_job_consumer_returns_early_when_job_preparation_handler_handles_failure() -> (
    None
):
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    job_workspace_factory = MagicMock()
    workspace = _workspace(manifest)
    job_workspace_factory.create.return_value = workspace

    job_state_transition_coordinator = JobStateTransitionCoordinator()

    preparation_handler = MagicMock(spec=JobPreparationHandler)
    preparation_handler.prepare.side_effect = RuntimeError()

    validation_handler = MagicMock()
    execution_supervisor = MagicMock()
    execution_registry = MagicMock()
    status_publisher = MagicMock()

    consumer = JobConsumer(
        manifest_loader,
        job_workspace_factory,
        job_state_transition_coordinator,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
    )

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    manifest_loader.load.assert_called_once_with("jobs/42/job-1/manifest.json")

    manifest_loader.load.assert_called_once_with("jobs/42/job-1/manifest.json")
    job_workspace_factory.create.assert_called_once_with(manifest)

    preparation_handler.prepare.assert_called_once_with(
        workspace=workspace,
    )

    validation_handler.validate.assert_not_called()

    status_publisher.publish_in_progress.assert_not_called()
    execution_supervisor.start.assert_not_called()
    execution_registry.add.assert_called_once()

    ack_mock.ack.assert_called_once()
    ack_mock.nack.assert_not_called()


def test_job_consumer_registers_local_job_then_replaces_with_supervised_execution_before_publishing_in_progress() -> (
    None
):
    events: list[str] = []

    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"

    manifest_loader = MagicMock()
    manifest_loader.load.return_value = manifest

    job_workspace_factory = MagicMock()
    workspace = _workspace(manifest)
    job_workspace_factory.create.return_value = workspace

    job_state_transition_coordinator = JobStateTransitionCoordinator()
    context = MagicMock()
    context.user_id = 42
    context.job_id = "job-1"
    context.job_type = "SOLVING_SLAE"
    context.workspace = workspace
    job_handler = MagicMock()

    preparation_handler = MagicMock(spec=JobPreparationHandler)
    preparation_handler.prepare.return_value = PreparedJob(
        context=context,
        handler=job_handler,
    )

    validation_handler = MagicMock()
    validation_handler.validate.return_value = True

    execution_record = MagicMock()

    execution_supervisor = MagicMock()

    def start_execution(_request):
        events.append("start")
        return execution_record

    execution_supervisor.start.side_effect = start_execution

    execution_registry = MagicMock()

    def add_initial_record(record):
        assert record.workspace is workspace
        assert record.job_id == "job-1"
        assert record.user_id == 42
        assert record.job_type == "SOLVING_SLAE"
        assert record.worker_id == "test-worker-id"
        assert record.context is None
        assert record.process_record is None
        events.append("add_initial_record")

    execution_registry.add.side_effect = add_initial_record

    status_publisher = MagicMock()

    def publish_in_progress(*_args):
        events.append("publish_in_progress")

    status_publisher.publish_in_progress.side_effect = publish_in_progress

    consumer = JobConsumer(
        manifest_loader,
        job_workspace_factory,
        job_state_transition_coordinator,
        preparation_handler,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
    )

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    message = QueueMessage(
        payload=JobMessageDTO(
            manifestObjectKey="jobs/42/job-1/manifest.json",
        )
    )

    consumer.handle(message, ack)

    assert events == [
        "add_initial_record",
        "start",
        "publish_in_progress",
    ]

    preparation_handler.prepare.assert_called_once_with(
        workspace=workspace,
    )

    validation_handler.validate.assert_called_once_with(
        handler=job_handler,
        context=context,
    )

    execution_supervisor.start.assert_called_once()
    execution_registry.add.assert_called_once()

    status_publisher.publish_in_progress.assert_called_once_with(
        workspace,
        0,
        "Start job execution",
    )

    ack_mock.ack.assert_not_called()
    ack_mock.nack.assert_not_called()


def _workspace(manifest: MagicMock) -> MagicMock:
    workspace = MagicMock(name="workspace")
    workspace.manifest = manifest
    workspace.user_id = manifest.user_id
    workspace.job_id = manifest.job_id
    workspace.job_type = manifest.job_type
    workspace.worker_id = "test-worker-id"
    return workspace
