# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
from typing import Any, cast

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef, JobManifest
from mdds_worker_runtime.manifest.models import JobManifestDTO, ManifestArtifactDTO

logger = logging.getLogger(__name__)


def to_domain(dto: JobManifestDTO) -> JobManifest:
    """Converts a JobManifestDTO object to a JobManifest. Class
    JobManifest uses enums for artifacts format and is designed to be
    a part of domain logic."""
    logger.debug(
        "Converting manifest DTO to domain model.",
        extra={
            "component": "manifest_mapper",
            "event": "manifest_domain_mapping_started",
            "jobId": dto.job_id,
            "userId": dto.user_id,
            "jobType": dto.job_type,
            "inputSlots": sorted(dto.inputs.keys()),
            "outputSlots": sorted(dto.outputs.keys()),
            "paramNames": sorted(dto.params.keys()),
        },
    )
    return JobManifest(
        manifest_version=dto.manifest_version,
        user_id=dto.user_id,
        job_id=dto.job_id,
        job_type=dto.job_type,
        inputs=_artifacts_to_domain(dto.inputs),
        params=cast(dict[str, Any], dict(dto.params)),
        outputs=_artifacts_to_domain(dto.outputs),
    )


def _artifacts_to_domain(
    artifacts: dict[str, ManifestArtifactDTO],
) -> dict[str, ArtifactRef]:
    """Convert a ManifestArtifactDTO object to a ManifestRef."""
    return {
        slot_name: ArtifactRef(
            slot_name=slot_name,
            object_key=artifact.object_key,
            format=ArtifactFormat.from_raw(artifact.format),
        )
        for slot_name, artifact in artifacts.items()
    }
