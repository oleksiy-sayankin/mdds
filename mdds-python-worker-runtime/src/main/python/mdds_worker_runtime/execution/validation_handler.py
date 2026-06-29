# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import logging
from pathlib import Path
import shutil

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.queue.queue_client import Acknowledger

logger = logging.getLogger(__name__)


class ValidationFailed(RuntimeError):
    """Raised when worker-side semantic validation fails."""


class ValidationHandler:
    """Handles worker-side semantic validation before supervised execution starts."""

    def __init__(
        self,
        status_publisher: StatusPublisher,
        worker_id: str,
    ) -> None:
        if status_publisher is None:
            raise ValueError("status_publisher cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")

        self._status_publisher = status_publisher
        self._worker_id = worker_id.strip()

    def validate_or_handle_failure(
        self,
        *,
        handler: JobHandler,
        context: JobExecutionContext,
        manifest: JobManifest,
        submitted_ack: Acknowledger,
    ) -> bool:
        if handler is None:
            raise ValueError("handler cannot be null.")
        if context is None:
            raise ValueError("context cannot be null.")
        if manifest is None:
            raise ValueError("manifest cannot be null.")
        if submitted_ack is None:
            raise ValueError("submitted_ack cannot be null.")

        try:
            handler.validate(context)
            return True
        except ValidationFailed as error:
            logger.warning(
                "Worker-side semantic validation failed.",
                exc_info=True,
                extra={
                    "component": "validation_handler",
                    "event": "validation_failed",
                    "jobId": manifest.job_id,
                    "userId": manifest.user_id,
                    "jobType": manifest.job_type,
                    "workerId": self._worker_id,
                },
            )

            self._handle_validation_failed(
                context=context,
                manifest=manifest,
                submitted_ack=submitted_ack,
                error=error,
            )
            return False

    def _handle_validation_failed(
        self,
        *,
        context: JobExecutionContext,
        manifest: JobManifest,
        submitted_ack: Acknowledger,
        error: ValidationFailed,
    ) -> None:
        message = str(error).strip() or "Worker-side semantic validation failed."

        self._status_publisher.publish_validation_failed(
            user_id=manifest.user_id,
            job_id=manifest.job_id,
            job_type=manifest.job_type,
            worker_id=self._worker_id,
            message=message,
        )

        submitted_ack.ack()
        self._cleanup_local_workspace(context)

        logger.info(
            "Worker-side validation failure was processed.",
            extra={
                "component": "validation_handler",
                "event": "validation_failure_processed",
                "jobId": manifest.job_id,
                "userId": manifest.user_id,
                "jobType": manifest.job_type,
                "workerId": self._worker_id,
                "localPath": str(context.work_dir),
            },
        )

    def _cleanup_local_workspace(self, context: JobExecutionContext) -> None:
        work_dir = Path(context.work_dir)

        try:
            if not work_dir.exists():
                logger.info(
                    "Local job workspace is already absent after validation failure.",
                    extra={
                        "component": "validation_handler",
                        "event": "validation_failed_workspace_already_absent",
                        "jobId": context.job_id,
                        "userId": context.user_id,
                        "jobType": context.job_type,
                        "workerId": self._worker_id,
                        "localPath": str(work_dir),
                    },
                )
                return

            if not work_dir.is_dir():
                raise ValueError(
                    "Local job workspace path is not a directory: "
                    f"jobId='{context.job_id}', workDir='{work_dir}'."
                )

            shutil.rmtree(work_dir)

            logger.info(
                "Local job workspace deleted after validation failure.",
                extra={
                    "component": "validation_handler",
                    "event": "validation_failed_workspace_deleted",
                    "jobId": context.job_id,
                    "userId": context.user_id,
                    "jobType": context.job_type,
                    "workerId": self._worker_id,
                    "localPath": str(work_dir),
                },
            )

        except Exception:
            logger.exception(
                "Local job workspace cleanup failed after validation failure.",
                extra={
                    "component": "validation_handler",
                    "event": "validation_failed_workspace_cleanup_failed",
                    "jobId": context.job_id,
                    "userId": context.user_id,
                    "jobType": context.job_type,
                    "workerId": self._worker_id,
                    "localPath": str(work_dir),
                },
            )
