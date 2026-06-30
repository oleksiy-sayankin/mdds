# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import logging
from dataclasses import dataclass

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.artifacts import InputArtifactPreparer
from mdds_worker_runtime.execution.context import (
    JobExecutionContext,
    JobExecutionContextFactory,
)
from mdds_worker_runtime.execution.handler import JobHandler
from mdds_worker_runtime.execution.handler_loader import JobHandlerLoader
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.workspace_cleaner import LocalJobWorkspaceCleaner
from mdds_worker_runtime.queue.queue_client import Acknowledger

logger = logging.getLogger(__name__)

_JOB_PREPARATION_ERROR_MESSAGE = "Worker-side job preparation failed."


@dataclass(frozen=True)
class PreparedJob:
    """Job prepared for worker-side semantic validation and execution."""

    context: JobExecutionContext
    handler: JobHandler


class JobPreparationHandler:
    """Prepares a submitted job after manifest identity is known.

    Manifest loading stays outside this component because before manifest is
    loaded the Worker cannot reliably determine jobId, userId, and jobType.

    This handler owns the pre-validation preparation failure policy:
    once job identity is known, preparation failures are converted into terminal
    ERROR status updates and the submitted job message is acknowledged only
    after successful ERROR publication.
    """

    def __init__(
        self,
        input_artifact_preparer: InputArtifactPreparer,
        context_factory: JobExecutionContextFactory,
        job_handler_loader: JobHandlerLoader,
        status_publisher: StatusPublisher,
        workspace_cleaner: LocalJobWorkspaceCleaner,
        worker_id: str,
    ) -> None:
        if input_artifact_preparer is None:
            raise ValueError("input_artifact_preparer cannot be null.")
        if context_factory is None:
            raise ValueError("context_factory cannot be null.")
        if job_handler_loader is None:
            raise ValueError("job_handler_loader cannot be null.")
        if status_publisher is None:
            raise ValueError("status_publisher cannot be null.")
        if workspace_cleaner is None:
            raise ValueError("workspace_cleaner cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")

        self._input_artifact_preparer = input_artifact_preparer
        self._context_factory = context_factory
        self._job_handler_loader = job_handler_loader
        self._status_publisher = status_publisher
        self._workspace_cleaner = workspace_cleaner
        self._worker_id = worker_id.strip()

    def prepare_or_handle_failure(
        self,
        *,
        manifest_object_key: str,
        manifest: JobManifest,
        submitted_ack: Acknowledger,
    ) -> PreparedJob | None:
        """Prepare job context and handler, or publish ERROR on failure."""
        if manifest_object_key is None or manifest_object_key.strip() == "":
            raise ValueError("manifest_object_key cannot be null or blank.")
        if manifest is None:
            raise ValueError("manifest cannot be null.")
        if submitted_ack is None:
            raise ValueError("submitted_ack cannot be null.")

        try:
            prepared_job_inputs = self._input_artifact_preparer.prepare(
                manifest.user_id,
                manifest.job_id,
                manifest.inputs,
            )

            context = self._context_factory.create(manifest, prepared_job_inputs)

            handler = self._job_handler_loader.load()

            return PreparedJob(
                context=context,
                handler=handler,
            )

        except Exception as error:
            logger.exception(
                "Worker-side job preparation failed.",
                extra={
                    "component": "job_preparation_handler",
                    "event": "job_preparation_failed",
                    "jobId": manifest.job_id,
                    "userId": manifest.user_id,
                    "jobType": manifest.job_type,
                    "workerId": self._worker_id,
                    "manifestObjectKey": manifest_object_key,
                    "errorType": type(error).__name__,
                },
            )

            self._handle_failure(
                manifest=manifest,
                submitted_ack=submitted_ack,
                error=error,
            )
            return None

    def _handle_failure(
        self,
        *,
        manifest: JobManifest,
        submitted_ack: Acknowledger,
        error: Exception,
    ) -> None:
        message = str(error).strip() or _JOB_PREPARATION_ERROR_MESSAGE

        self._status_publisher.publish_error(
            user_id=manifest.user_id,
            job_id=manifest.job_id,
            job_type=manifest.job_type,
            worker_id=self._worker_id,
            message=message,
        )

        submitted_ack.ack()

        self._cleanup_workspace_after_terminal_preparation_failure(manifest)

        logger.info(
            "Worker-side job preparation failure was processed.",
            extra={
                "component": "job_preparation_handler",
                "event": "job_preparation_failure_processed",
                "jobId": manifest.job_id,
                "userId": manifest.user_id,
                "jobType": manifest.job_type,
                "workerId": self._worker_id,
                "errorType": type(error).__name__,
            },
        )

    def _cleanup_workspace_after_terminal_preparation_failure(
        self,
        manifest: JobManifest,
    ) -> None:
        try:
            self._workspace_cleaner.cleanup_job_workspace(
                manifest.user_id,
                manifest.job_id,
            )
        except Exception:
            logger.exception(
                "Failed to cleanup local job workspace after preparation failure.",
                extra={
                    "component": "job_preparation_handler",
                    "event": "job_preparation_workspace_cleanup_failed",
                    "jobId": manifest.job_id,
                    "userId": manifest.user_id,
                    "jobType": manifest.job_type,
                    "workerId": self._worker_id,
                },
            )
