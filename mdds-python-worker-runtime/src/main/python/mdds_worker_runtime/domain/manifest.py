# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from dataclasses import dataclass
from typing import Any

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat


@dataclass(frozen=True)
class ArtifactRef:
    """Represents artifact reference: input or output slot"""

    slot_name: str
    object_key: str
    format: ArtifactFormat


@dataclass(frozen=True)
class JobManifest:
    manifest_version: int
    user_id: int
    job_id: str
    job_type: str
    # Note: nested dictionaries remain mutable even though the dataclass is frozen.
    # This may be replaced with immutable mappings in a future version.
    inputs: dict[str, ArtifactRef]
    params: dict[str, Any]
    outputs: dict[str, ArtifactRef]
