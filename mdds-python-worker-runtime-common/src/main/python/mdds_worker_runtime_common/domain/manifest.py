# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat


@dataclass(frozen=True)
class NodeRunIdentity:
    """Identity of the Node Run described by a Worker Manifest."""

    user_id: int
    dag_run_id: str
    node_id: str


@dataclass(frozen=True)
class ArtifactRef:
    """Local artifact path and its business format."""

    path: Path
    format: ArtifactFormat

    def __post_init__(self) -> None:
        if not self.path.is_absolute():
            raise ValueError(f"Artifact path must be absolute: '{self.path}'.")


@dataclass(frozen=True)
class WorkerManifest:
    """Business representation of a validated Worker Manifest."""

    api_version: str
    kind: str
    execution: NodeRunIdentity
    inputs: Mapping[str, ArtifactRef]
    params: Mapping[str, Any]
    outputs: Mapping[str, ArtifactRef]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "inputs",
            MappingProxyType(dict(self.inputs)),
        )
        object.__setattr__(
            self,
            "params",
            MappingProxyType(deepcopy(dict(self.params))),
        )
        object.__setattr__(
            self,
            "outputs",
            MappingProxyType(dict(self.outputs)),
        )
