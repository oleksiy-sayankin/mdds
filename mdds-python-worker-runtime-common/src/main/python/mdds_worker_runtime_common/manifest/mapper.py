# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from pathlib import Path
from typing import Mapping

from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime_common.domain.manifest import (
    ArtifactRef,
    NodeRunIdentity,
    WorkerManifest,
)
from mdds_worker_runtime_common.manifest.models import (
    ManifestArtifactDTO,
    WorkerManifestDTO,
)


class WorkerManifestMappingError(ValueError):
    """A valid manifest DTO cannot be converted to the domain model."""


def to_domain(dto: WorkerManifestDTO) -> WorkerManifest:
    """Convert a validated transport DTO to the business model."""
    if dto is None:
        raise WorkerManifestMappingError("Worker Manifest DTO must not be null.")

    try:
        return WorkerManifest(
            api_version=dto.api_version,
            kind=dto.kind,
            execution=NodeRunIdentity(
                user_id=dto.execution.user_id,
                dag_run_id=dto.execution.dag_run_id,
                node_id=dto.execution.node_id,
            ),
            inputs=_artifacts_to_domain(dto.inputs),
            params=dto.params,
            outputs=_artifacts_to_domain(dto.outputs),
        )
    except ValueError as exc:
        raise WorkerManifestMappingError(str(exc)) from exc


def _artifacts_to_domain(
    artifacts: Mapping[str, ManifestArtifactDTO],
) -> dict[str, ArtifactRef]:
    return {
        slot: ArtifactRef(
            path=Path(artifact.path),
            format=ArtifactFormat.from_raw(artifact.format),
        )
        for slot, artifact in artifacts.items()
    }
