# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from mdds_worker_runtime.dto.messages import JobMessageDTO
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
