# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import logging

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_preparation_handler import (
    JobPreparationHandler,
    PreparedJob,
)
from mdds_worker_runtime.execution.models import (
    WorkerJobStatus,
    ExecutionRecord,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.supervisor import (
    ExecutionSupervisor,
    SupervisedExecutionRequest,
)
from mdds_worker_runtime.execution.workspace import JobWorkspaceFactory, JobWorkspace
from mdds_worker_runtime.job_state import JobStateTransitionCoordinator
from mdds_worker_runtime.manifest.loader import ManifestLoader
from mdds_worker_runtime.queue.queue_client import (
    Acknowledger,
    MessageHandler,
    QueueMessage,
)

logger = logging.getLogger(__name__)

_JOB_PREPARATION_ERROR_MESSAGE = "Worker-side job preparation failed."
_UNEXPECTED_VALIDATION_ERROR_MESSAGE = "Worker-side validation processing failed."
_VALIDATION_FAILED_MESSAGE = "Worker-side semantic validation failed."
_SUPERVISED_EXECUTION_START_ERROR_MESSAGE = (
    "Worker-side supervised execution start failed."
)


class JobConsumer(MessageHandler[JobMessageDTO]):
    """Consumes submitted job messages and starts supervised job execution.

    The consumed payload mirrors Java ``JobMessageDTO`` and contains
    manifestObjectKey, which points to manifest.json in object storage.

    This consumer owns the submitted-message happy path after manifest identity
    is known:

    1. ``SUBMITTED`` -> ``INPUTS_PREPARED``;
    2. ``INPUTS_PREPARED`` -> ``VALIDATED``;
    3. ``VALIDATED`` -> ``IN_PROGRESS``.

    Each happy-path step is executed as a coordinator-owned transition. If a
    step fails, the consumer asks the same coordinator to commit a separate
    terminal fallback transition from the unchanged source state. Low-level
    preparation and validation handlers must not publish terminal statuses,
    acknowledge the submitted message, or call coordinator transitions.

    On the successful start path, the submitted job message is not acknowledged
    here. The Acknowledger is stored in ``WorkerJobStateRecord`` and is acknowledged
    later by ``ExecutionWatcher``, ``TimeoutWatcher``, or ``CancellationRequestHandler`` after a
    committed terminal state: ``DONE``, ``ERROR``, or ``CANCELLED``.

    Store the submitted-message acknowledger in the coordinator as soon as
    job identity is known. Pre-execution terminal transitions may need it before
    an ``ExecutionRecord`` exists.
    """

    def __init__(
        self,
        manifest_loader: ManifestLoader,
        job_workspace_factory: JobWorkspaceFactory,
        job_state_transition_coordinator: JobStateTransitionCoordinator,
        job_preparation_handler: JobPreparationHandler,
        execution_supervisor: ExecutionSupervisor,
        execution_registry: ExecutionRegistry,
        status_publisher: StatusPublisher,
    ) -> None:
        if manifest_loader is None:
            raise ValueError("manifest_loader cannot be null.")
        if job_workspace_factory is None:
            raise ValueError("job_workspace_factory cannot be null.")
        if job_state_transition_coordinator is None:
            raise ValueError("job_state_transition_coordinator cannot be null.")
        if job_preparation_handler is None:
            raise ValueError("job_preparation_handler cannot be null.")
        if execution_supervisor is None:
            raise ValueError("execution_supervisor cannot be null.")
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if status_publisher is None:
            raise ValueError("status_publisher cannot be null.")

        self._manifest_loader = manifest_loader
        self._job_workspace_factory = job_workspace_factory
        self._job_state_transition_coordinator = job_state_transition_coordinator
        self._job_preparation_handler = job_preparation_handler
        self._execution_supervisor = execution_supervisor
        self._execution_registry = execution_registry
        self._status_publisher = status_publisher

    def handle(
        self,
        message: QueueMessage[JobMessageDTO],
        ack: Acknowledger,
    ) -> None:
        """Handle a submitted job message."""
        manifest_object_key = message.payload.manifest_object_key

        # Identity is still unknown here. Let the exception escape to the
        # generic message handling path. Do not publish job-level status here.
        manifest = self._manifest_loader.load(manifest_object_key)
        workspace = self._job_workspace_factory.create(manifest)
        record = ExecutionRecord(
            workspace=workspace,
        )
        self._execution_registry.add(record)
        self._job_state_transition_coordinator.create(
            job_id=workspace.job_id, submitted_ack=ack
        )

        # Preparing job inputs
        prepare_result = self._job_state_transition_coordinator.transition(
            job_id=workspace.job_id,
            target_state=WorkerJobStatus.INPUTS_PREPARED,
            operation=lambda: self._prepare_job(
                workspace=workspace,
            ),
        )

        # Handle inputs preparation failure
        if prepare_result.failed:
            error = prepare_result.error
            error_message = self._message_or_fallback(
                error, _JOB_PREPARATION_ERROR_MESSAGE
            )
            terminal_result = self._job_state_transition_coordinator.transition(
                job_id=workspace.job_id,
                target_state=WorkerJobStatus.ERROR,
                operation=lambda: self._status_publisher.publish_error(
                    workspace=workspace,
                    message=error_message,
                ),
            )
            if terminal_result.failed:
                logger.error(
                    "Failed to publish error",
                    extra={
                        "component": "job_state_transition_coordinator",
                        "event": "terminal_transition_failed",
                        "jobId": workspace.job_id,
                    },
                )
            return

        if not prepare_result.committed or prepare_result.value is None:
            return

        prepared_job = prepare_result.value

        # Start job execution
        start_result = self._job_state_transition_coordinator.transition(
            job_id=workspace.job_id,
            target_state=WorkerJobStatus.IN_PROGRESS,
            operation=lambda: self._start_supervised_execution(
                prepared_job=prepared_job,
            ),
        )

        if start_result.failed:
            error = start_result.error
            error_message = self._message_or_fallback(
                error, _SUPERVISED_EXECUTION_START_ERROR_MESSAGE
            )
            terminal_result = self._job_state_transition_coordinator.transition(
                job_id=workspace.job_id,
                target_state=WorkerJobStatus.ERROR,
                operation=lambda: self._status_publisher.publish_error(
                    workspace=workspace,
                    message=error_message,
                ),
            )
            if terminal_result.failed:
                logger.error(
                    "Failed to publish error",
                    extra={
                        "component": "job_state_transition_coordinator",
                        "event": "terminal_transition_failed",
                        "jobId": workspace.job_id,
                    },
                )
            return

    def _prepare_job(
        self,
        *,
        workspace: JobWorkspace,
    ) -> PreparedJob:
        """Prepare local job context and publish INPUTS_PREPARED.

        This method is intentionally a happy-path transition operation. It must
        let preparation exceptions escape so that the caller can request a
        separate SUBMITTED -> ERROR terminal transition through the coordinator.
        """
        prepared_job = self._job_preparation_handler.prepare(
            workspace=workspace,
        )

        self._status_publisher.publish_inputs_prepared(
            workspace=workspace,
            message="Worker prepared job inputs.",
        )

        return prepared_job

    def _start_supervised_execution(
        self,
        *,
        prepared_job: PreparedJob,
    ) -> None:
        """Start supervised execution and publish initial IN_PROGRESS.

        If a failure happens after the execution record has been registered but
        before the transition can be committed, the local execution record is
        removed as a best-effort compensation. The exception is then re-raised so
        the caller can request VALIDATED -> ERROR through the coordinator.
        """

        supervised_execution_request = SupervisedExecutionRequest(
            context=prepared_job.context,
        )

        process_record = self._execution_supervisor.start(supervised_execution_request)
        self._execution_registry.attach_process_record(
            job_id=prepared_job.context.job_id, process_record=process_record
        )

        self._status_publisher.publish_in_progress(
            prepared_job.context.workspace,
            0,
            "Start job execution",
        )

    @staticmethod
    def _message_or_fallback(error: Exception | None, fallback: str) -> str:
        if error is None:
            return fallback

        message = str(error).strip()
        if message:
            return message

        return fallback
