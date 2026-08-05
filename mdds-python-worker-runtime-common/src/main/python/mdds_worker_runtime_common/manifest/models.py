# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints

NonBlankStr = Annotated[
    str,
    StringConstraints(min_length=1, pattern=r"\S"),
]


class _ManifestDTO(BaseModel):
    """Common strict configuration for manifest transport objects."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_by_alias=True,
        validate_by_name=False,
    )


class ManifestExecutionDTO(_ManifestDTO):
    """Execution object exactly as represented in manifest JSON."""

    user_id: int = Field(alias="userId")
    dag_run_id: NonBlankStr = Field(alias="dagRunId")
    node_id: NonBlankStr = Field(alias="nodeId")


class ManifestArtifactDTO(_ManifestDTO):
    """Single input or output object represented in manifest JSON."""

    path: NonBlankStr
    format: NonBlankStr


class WorkerManifestDTO(_ManifestDTO):
    """Validated transport representation of worker-manifest.json."""

    api_version: Literal["mdds/v1"] = Field(alias="apiVersion")
    kind: Literal["WorkerManifest"]
    execution: ManifestExecutionDTO
    inputs: dict[NonBlankStr, ManifestArtifactDTO]
    params: dict[NonBlankStr, JsonValue]
    outputs: dict[NonBlankStr, ManifestArtifactDTO]
