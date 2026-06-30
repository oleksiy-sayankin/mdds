# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.job_preparation_handler import (
    JobPreparationHandler,
    PreparedJob,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.supervisor import (
    ExecutionSupervisor,
    SupervisedExecutionRequest,
)
from mdds_worker_runtime.execution.validation_handler import ValidationHandler
from mdds_worker_runtime.manifest.loader import ManifestLoader
from mdds_worker_runtime.queue.queue_client import (
    Acknowledger,
    MessageHandler,
    QueueMessage,
)


class JobConsumer(MessageHandler[JobMessageDTO]):
    """Consumes submitted job messages and starts supervised job execution.

    The consumed payload mirrors Java JobMessageDTO and contains manifestObjectKey,
    which points to manifest.json in object storage.

    This handler must be fast and non-blocking with respect to long-running job
    execution. It validates the job, starts a supervised child process, registers
    the execution record, publishes IN_PROGRESS, and returns.

    The submitted job message is not acknowledged on the successful start path.
    The Acknowledger is stored in ExecutionRecord and is acknowledged later by
    ExecutionWatcher, TimeoutWatcher, or cancellation handling after the job
    reaches a terminal state: DONE, ERROR, or CANCELLED.

    If worker-side semantic validation fails before supervised execution starts,
    the handler publishes VALIDATION_FAILED and acknowledges the submitted job
    message immediately because the message was processed terminally and should
    not be retried.
    """

    def __init__(
        self,
        manifest_loader: ManifestLoader,
        job_preparation_handler: JobPreparationHandler,
        validation_handler: ValidationHandler,
        execution_supervisor: ExecutionSupervisor,
        execution_registry: ExecutionRegistry,
        status_publisher: StatusPublisher,
        worker_id: str,
    ) -> None:
        if manifest_loader is None:
            raise ValueError("manifest_loader cannot be null.")
        if job_preparation_handler is None:
            raise ValueError("job_preparation_handler cannot be null.")
        if validation_handler is None:
            raise ValueError("validation_handler cannot be null.")
        if execution_supervisor is None:
            raise ValueError("execution_supervisor cannot be null.")
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if status_publisher is None:
            raise ValueError("status_publisher cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")

        self._manifest_loader = manifest_loader
        self._job_preparation_handler = job_preparation_handler
        self._validation_handler = validation_handler
        self._execution_supervisor = execution_supervisor
        self._execution_registry = execution_registry
        self._status_publisher = status_publisher
        self._worker_id = worker_id.strip()

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

        prepared_job = self._job_preparation_handler.prepare_or_handle_failure(
            manifest_object_key=manifest_object_key,
            manifest=manifest,
            submitted_ack=ack,
        )

        if prepared_job is None:
            return

        if not self._validation_handler.validate_or_handle_failure(
            handler=prepared_job.handler,
            context=prepared_job.context,
            manifest=manifest,
            submitted_ack=ack,
        ):
            return

        self._start_supervised_execution(
            prepared_job=prepared_job,
            manifest_object_key=manifest_object_key,
            manifest=manifest,
            submitted_ack=ack,
        )

    def _start_supervised_execution(
        self,
        *,
        prepared_job: PreparedJob,
        manifest_object_key: str,
        manifest,
        submitted_ack: Acknowledger,
    ) -> None:
        supervised_execution_request = SupervisedExecutionRequest(
            context=prepared_job.context,
            worker_id=self._worker_id,
            manifest_object_key=manifest_object_key,
            manifest=manifest,
            submitted_ack=submitted_ack,
        )

        self._status_publisher.publish_in_progress(
            manifest.user_id,
            manifest.job_id,
            manifest.job_type,
            self._worker_id,
            0,
            "Start job execution",
        )

        execution_record = self._execution_supervisor.start(
            supervised_execution_request
        )
        self._execution_registry.add(execution_record)
