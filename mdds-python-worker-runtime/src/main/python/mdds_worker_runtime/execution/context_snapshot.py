# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import JobManifest, ArtifactRef
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.models import JobWorkspace

SNAPSHOT_VERSION = 1


class JobExecutionContextSnapshotError(RuntimeError):
    """Raised when a JobExecutionContext snapshot cannot be saved or loaded."""


@dataclass(frozen=True)
class ArtifactSnapshot:
    """Serializable artifact description used to reconstruct context facades."""

    object_key: str
    local_path: str
    format: str

    @staticmethod
    def from_artifact(
        artifact: PreparedInputArtifact | PreparedOutputArtifact,
    ) -> "ArtifactSnapshot":
        return ArtifactSnapshot(
            object_key=artifact.object_key,
            local_path=str(artifact.local_path),
            format=_artifact_format_to_string(artifact.format),
        )

    @staticmethod
    def from_dict(data: Mapping[str, Any]) -> "ArtifactSnapshot":
        return ArtifactSnapshot(
            object_key=str(data["objectKey"]),
            local_path=str(data["localPath"]),
            format=str(data["format"]),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "objectKey": self.object_key,
            "localPath": self.local_path,
            "format": self.format,
        }

    def to_prepared_input_artifact(self) -> PreparedInputArtifact:
        return PreparedInputArtifact(
            object_key=self.object_key,
            local_path=Path(self.local_path),
            format=ArtifactFormat(self.format),
        )

    def to_prepared_output_artifact(self) -> PreparedOutputArtifact:
        return PreparedOutputArtifact(
            object_key=self.object_key,
            local_path=Path(self.local_path),
            format=ArtifactFormat(self.format),
        )


@dataclass(frozen=True)
class JobExecutionContextSnapshot:
    """Serializable representation of JobExecutionContext.

    The snapshot is an inter-process transport format. It is intentionally
    separate from JobExecutionContext so the handler-facing context API does
    not become responsible for JSON, filesystem persistence, or multiprocessing.
    """

    snapshot_version: int
    manifest_version: int
    user_id: int
    job_id: str
    job_type: str
    work_dir: str
    input_dir: str
    output_dir: str
    inputs: Mapping[str, ArtifactSnapshot]
    outputs: Mapping[str, ArtifactSnapshot]
    params: Mapping[str, Any]
    worker_id: str

    @staticmethod
    def from_context(context: JobExecutionContext) -> "JobExecutionContextSnapshot":
        if context is None:
            raise ValueError("context cannot be null.")

        manifest = context.workspace.manifest

        return JobExecutionContextSnapshot(
            snapshot_version=SNAPSHOT_VERSION,
            manifest_version=manifest.manifest_version,
            user_id=manifest.user_id,
            job_id=manifest.job_id,
            job_type=manifest.job_type,
            work_dir=str(context.workspace.work_dir),
            input_dir=str(context.workspace.input_dir),
            output_dir=str(context.workspace.output_dir),
            inputs={
                slot: ArtifactSnapshot.from_artifact(artifact)
                for slot, artifact in context.inputs._artifacts.items()
            },
            outputs={
                slot: ArtifactSnapshot.from_artifact(artifact)
                for slot, artifact in context.outputs._artifacts.items()
            },
            params=dict(context.params._params),
            worker_id=context.workspace.worker_id,
        )

    @staticmethod
    def from_dict(data: Mapping[str, Any]) -> "JobExecutionContextSnapshot":
        snapshot_version = int(data["snapshotVersion"])

        if snapshot_version != SNAPSHOT_VERSION:
            raise JobExecutionContextSnapshotError(
                f"Unsupported JobExecutionContext snapshot version: {snapshot_version}"
            )

        return JobExecutionContextSnapshot(
            snapshot_version=snapshot_version,
            manifest_version=int(data["manifestVersion"]),
            user_id=int(data["userId"]),
            job_id=str(data["jobId"]),
            job_type=str(data["jobType"]),
            work_dir=str(data["workDir"]),
            input_dir=str(data["inputDir"]),
            output_dir=str(data["outputDir"]),
            inputs={
                slot: ArtifactSnapshot.from_dict(artifact)
                for slot, artifact in data["inputs"].items()
            },
            outputs={
                slot: ArtifactSnapshot.from_dict(artifact)
                for slot, artifact in data["outputs"].items()
            },
            params=dict(data["params"]),
            worker_id=data["workerId"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshotVersion": self.snapshot_version,
            "manifestVersion": self.manifest_version,
            "userId": self.user_id,
            "jobId": self.job_id,
            "jobType": self.job_type,
            "workDir": self.work_dir,
            "inputDir": self.input_dir,
            "outputDir": self.output_dir,
            "inputs": {
                slot: artifact.to_dict() for slot, artifact in self.inputs.items()
            },
            "outputs": {
                slot: artifact.to_dict() for slot, artifact in self.outputs.items()
            },
            "params": dict(self.params),
            "workerId": self.worker_id,
        }

    def to_context(self) -> JobExecutionContext:
        manifest = JobManifest(
            manifest_version=self.manifest_version,
            user_id=self.user_id,
            job_id=self.job_id,
            job_type=self.job_type,
            inputs={
                slot: ArtifactRef(
                    object_key=artifact.object_key,
                    format=ArtifactFormat(artifact.format),
                )
                for slot, artifact in self.inputs.items()
            },
            params=dict(self.params),
            outputs={
                slot: ArtifactRef(
                    object_key=artifact.object_key,
                    format=ArtifactFormat(artifact.format),
                )
                for slot, artifact in self.outputs.items()
            },
        )

        workspace = JobWorkspace(
            manifest=manifest,
            work_dir=Path(self.work_dir),
            input_dir=Path(self.input_dir),
            output_dir=Path(self.output_dir),
            worker_id=self.worker_id,
        )

        return JobExecutionContext(
            workspace=workspace,
            inputs=InputArtifacts(
                {
                    slot: artifact.to_prepared_input_artifact()
                    for slot, artifact in self.inputs.items()
                }
            ),
            outputs=OutputArtifacts(
                {
                    slot: artifact.to_prepared_output_artifact()
                    for slot, artifact in self.outputs.items()
                }
            ),
            params=JobParameters(self.params),
        )


class JobExecutionContextSnapshotStore:
    """Persists and loads JobExecutionContext snapshots."""

    def save(self, context: JobExecutionContext, path: Path) -> None:
        if context is None:
            raise ValueError("context cannot be null.")
        if path is None:
            raise ValueError("path cannot be null.")

        snapshot = JobExecutionContextSnapshot.from_context(context)

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    snapshot.to_dict(),
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
        except TypeError as exc:
            raise JobExecutionContextSnapshotError(
                f"JobExecutionContext snapshot is not JSON serializable: {path}"
            ) from exc
        except OSError as exc:
            raise JobExecutionContextSnapshotError(
                f"Cannot save JobExecutionContext snapshot: {path}"
            ) from exc

    def load(self, path: Path) -> JobExecutionContext:
        if path is None:
            raise ValueError("path cannot be null.")

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise JobExecutionContextSnapshotError(
                f"Cannot read JobExecutionContext snapshot: {path}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise JobExecutionContextSnapshotError(
                f"Cannot parse JobExecutionContext snapshot: {path}"
            ) from exc

        try:
            return JobExecutionContextSnapshot.from_dict(data).to_context()
        except (KeyError, TypeError, ValueError) as exc:
            raise JobExecutionContextSnapshotError(
                f"Invalid JobExecutionContext snapshot: {path}"
            ) from exc


def _artifact_format_to_string(artifact_format: ArtifactFormat) -> str:
    if artifact_format is None:
        raise ValueError("artifact format cannot be null.")

    return str(artifact_format.value)
