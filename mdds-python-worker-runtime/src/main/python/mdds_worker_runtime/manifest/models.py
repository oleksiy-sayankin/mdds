# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints

NonBlankStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ManifestArtifactDTO(BaseModel):
    """Single input or output manifest artifact."""

    object_key: NonBlankStr = Field(alias="objectKey")
    format: NonBlankStr

    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        extra="forbid",  # Reject unknown manifest fields.
    )


class JobManifestDTO(BaseModel):
    """Job manifest."""

    manifest_version: Literal[1] = Field(alias="manifestVersion")
    user_id: int = Field(alias="userId", gt=0)
    job_id: NonBlankStr = Field(alias="jobId")
    job_type: NonBlankStr = Field(alias="jobType")
    inputs: dict[str, ManifestArtifactDTO] = Field(default_factory=dict)
    params: dict[str, JsonValue] = Field(default_factory=dict)
    outputs: dict[str, ManifestArtifactDTO] = Field(default_factory=dict)

    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        extra="forbid",
    )
