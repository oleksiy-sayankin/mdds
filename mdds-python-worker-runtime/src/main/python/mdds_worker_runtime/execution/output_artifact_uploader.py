# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.storage.s3_client import S3Storage

logger = logging.getLogger(__name__)


class OutputArtifactUploadError(RuntimeError):
    """Raised when Worker Runtime cannot upload declared output artifacts."""


@dataclass(frozen=True)
class UploadedOutputArtifact:
    """Description of one successfully uploaded output artifact."""

    slot: str
    object_key: str
    local_path: Path
    size_bytes: int


@dataclass(frozen=True)
class OutputArtifactUploadResult:
    """Result of uploading all declared output artifacts for one job."""

    user_id: int
    job_id: str
    job_type: str
    uploaded_artifacts: tuple[UploadedOutputArtifact, ...]


class OutputArtifactUploader:
    """Uploads declared job output artifacts to object storage.

    The uploader is a Worker Runtime infrastructure component. It uploads
    only output artifacts declared in JobExecutionContext.outputs and uses
    object keys prepared from manifest.outputs.
    """

    def __init__(self, storage: S3Storage) -> None:
        if storage is None:
            raise ValueError("storage cannot be null.")

        self._storage = storage

    def upload(self, context: JobExecutionContext) -> OutputArtifactUploadResult:
        if context is None:
            raise ValueError("context cannot be null.")

        uploaded_artifacts: list[UploadedOutputArtifact] = []

        for output_slot, artifact in context.outputs.items():
            local_path = artifact.local_path
            object_key = artifact.object_key

            self._validate_output_artifact(
                job_id=context.job_id,
                slot=output_slot,
                object_key=object_key,
                local_path=local_path,
            )

            size_bytes = local_path.stat().st_size

            try:
                self._storage.upload_file(object_key, local_path)
            except Exception as exc:
                raise OutputArtifactUploadError(
                    "Failed to upload output artifact "
                    f"for jobId='{context.job_id}', slot='{output_slot}', "
                    f"objectKey='{object_key}', localPath='{local_path}': {exc}"
                ) from exc

            uploaded_artifact = UploadedOutputArtifact(
                slot=output_slot,
                object_key=object_key,
                local_path=local_path,
                size_bytes=size_bytes,
            )
            uploaded_artifacts.append(uploaded_artifact)

            logger.info(
                "Output artifact uploaded.",
                extra={
                    "component": "output_artifact_uploader",
                    "event": "output_artifact_uploaded",
                    "userId": context.user_id,
                    "jobId": context.job_id,
                    "jobType": context.job_type,
                    "outputSlots": output_slot,
                    "objectKey": object_key,
                    "localPath": str(local_path),
                    "sizeBytes": uploaded_artifact.size_bytes,
                },
            )

        result = OutputArtifactUploadResult(
            user_id=context.user_id,
            job_id=context.job_id,
            job_type=context.job_type,
            uploaded_artifacts=tuple(uploaded_artifacts),
        )

        logger.info(
            "Output artifact upload phase completed.",
            extra={
                "component": "output_artifact_uploader",
                "event": "output_artifact_upload_phase_completed",
                "userId": context.user_id,
                "jobId": context.job_id,
                "jobType": context.job_type,
                "uploadedArtifactCount": len(result.uploaded_artifacts),
            },
        )

        return result

    @staticmethod
    def _validate_output_artifact(
        job_id: str,
        slot: str,
        object_key: str,
        local_path: Path,
    ) -> None:
        if slot is None or slot.strip() == "":
            raise OutputArtifactUploadError(
                f"Output slot cannot be null or blank for jobId='{job_id}'."
            )

        if object_key is None or object_key.strip() == "":
            raise OutputArtifactUploadError(
                "Output object key cannot be null or blank "
                f"for jobId='{job_id}', slot='{slot}'."
            )

        if local_path is None:
            raise OutputArtifactUploadError(
                f"Output local path cannot be null for jobId='{job_id}', slot='{slot}'."
            )

        if not local_path.exists():
            raise OutputArtifactUploadError(
                "Expected output artifact does not exist "
                f"for jobId='{job_id}', slot='{slot}', localPath='{local_path}'."
            )

        if not local_path.is_file():
            raise OutputArtifactUploadError(
                "Expected output artifact is not a regular file "
                f"for jobId='{job_id}', slot='{slot}', localPath='{local_path}'."
            )
