# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from mdds_worker_runtime.dto.messages import JobMessageDTO
from mdds_worker_runtime.execution.artifacts import InputArtifactPreparer
from mdds_worker_runtime.execution.context import (
    JobExecutionContextFactory,
)
from mdds_worker_runtime.execution.handler_loader import JobHandlerLoader
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.supervisor import (
    ExecutionSupervisor,
    SupervisedExecutionRequest,
)
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
        input_artifact_preparer: InputArtifactPreparer,
        context_factory: JobExecutionContextFactory,
        job_handler_loader: JobHandlerLoader,
        execution_supervisor: ExecutionSupervisor,
        execution_registry: ExecutionRegistry,
        worker_id: str,
    ) -> None:
        if manifest_loader is None:
            raise ValueError("manifest_loader cannot be null.")
        if input_artifact_preparer is None:
            raise ValueError("input_artifact_preparer cannot be null.")
        if context_factory is None:
            raise ValueError("context_factory cannot be null.")
        if job_handler_loader is None:
            raise ValueError("job_handler_loader cannot be null.")
        if execution_supervisor is None:
            raise ValueError("execution_supervisor cannot be null.")
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")

        self._manifest_loader = manifest_loader
        self._input_artifact_preparer = input_artifact_preparer
        self._context_factory = context_factory
        self._job_handler_loader = job_handler_loader
        self._execution_supervisor = execution_supervisor
        self._execution_registry = execution_registry
        self._worker_id = worker_id

    def handle(
        self,
        message: QueueMessage[JobMessageDTO],
        ack: Acknowledger,
    ) -> None:
        """Handle a submitted job message.

        Expected flow:

        1. Read manifestObjectKey from JobMessageDTO.
        2. Load manifest.json from object storage.
        3. Prepare JobExecutionContext from the manifest.
        4. Run fast worker-side semantic validation:
           JobHandler.validate(JobExecutionContext).
        5. If validation fails:
           - publish VALIDATION_FAILED to the status queue;
           - acknowledge the submitted job message;
           - return without starting a supervised process.
        6. Start supervised execution process for JobHandler.execute(...).
        7. Create and register ExecutionRecord in ExecutionRegistry.
           The record keeps process handle, parent IPC connection, manifest,
           started_at timestamp, and the original submitted-message Acknowledger.
        8. Publish IN_PROGRESS to the status queue.
        9. Return without acknowledging the submitted job message.

        After this method returns, long-running execution is owned by runtime
        background services:

        - ExecutionWatcher observes process completion, commits output artifacts,
          publishes DONE or ERROR, and acknowledges the submitted job message.
        - TimeoutWatcher terminates expired executions, publishes ERROR, and
          acknowledges the submitted job message.
        - CleanupWatcher removes terminal execution records and local resources
          after terminal status publication and acknowledgement.
        """
        manifest_object_key = message.payload.manifest_object_key
        manifest = self._manifest_loader.load(manifest_object_key)

        prepared_job_inputs = self._input_artifact_preparer.prepare(
            manifest.user_id,
            manifest.job_id,
            manifest.inputs,
        )

        context = self._context_factory.create(manifest, prepared_job_inputs)

        handler = self._job_handler_loader.load()
        handler.validate(context)

        supervised_execution_request = SupervisedExecutionRequest(
            context=context,
            worker_id=self._worker_id,
            manifest_object_key=manifest_object_key,
            manifest=manifest,
            submitted_ack=ack,
        )

        execution_record = self._execution_supervisor.start(
            supervised_execution_request
        )
        self._execution_registry.add(execution_record)
