# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, ItemsView, Any

from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat


@dataclass(frozen=True)
class PreparedInputArtifact:
    """Description of an input artifact materialized locally by Argo."""

    local_path: Path
    format: ArtifactFormat


@dataclass(frozen=True)
class PreparedOutputArtifact:
    """Description of an output artifact materialized locally by Argo."""

    local_path: Path
    format: ArtifactFormat


@dataclass(frozen=True)
class Execution:
    user_id: int
    dag_run_id: str
    node_id: str
    attempt_id: str


@dataclass(frozen=True)
class InputArtifacts:
    """Handler-facing read-only access to prepared input artifacts.

    This is a public API for WorkerHandler implementations.
    It hides the internal slot-to-artifact mapping and exposes stable
    slot-based access methods.
    """

    _artifacts: Mapping[str, PreparedInputArtifact] = field(repr=False)

    def __post_init__(self) -> None:
        if self._artifacts is None:
            raise ValueError("input artifacts cannot be null.")
        object.__setattr__(
            self,
            "_artifacts",
            MappingProxyType(dict(self._artifacts)),
        )

    def get(self, slot: str) -> PreparedInputArtifact:
        _validate_name(slot, "input slot")
        try:
            return self._artifacts[slot]
        except KeyError as exc:
            raise KeyError(f"Input slot is not available: {slot}") from exc

    def path(self, slot: str) -> Path:
        return self.get(slot).local_path

    def read(self, slot: str) -> bytes:
        return self.path(slot).read_bytes()

    def items(self) -> ItemsView[str, PreparedInputArtifact]:
        return self._artifacts.items()


@dataclass(frozen=True)
class OutputArtifacts:
    """Handler-facing access to declared output artifacts.

    The handler writes output bytes to local files only.
    """

    _artifacts: Mapping[str, PreparedOutputArtifact] = field(repr=False)

    def __post_init__(self) -> None:
        if self._artifacts is None:
            raise ValueError("output artifacts cannot be null.")
        object.__setattr__(
            self,
            "_artifacts",
            MappingProxyType(dict(self._artifacts)),
        )

    def get(self, slot: str) -> PreparedOutputArtifact:
        _validate_name(slot, "output slot")
        try:
            return self._artifacts[slot]
        except KeyError as exc:
            raise KeyError(f"Output slot is not declared: {slot}") from exc

    def path(self, slot: str) -> Path:
        return self.get(slot).local_path

    def write(self, slot: str, data: bytes) -> None:
        if data is None:
            raise ValueError("output data cannot be null.")
        if not isinstance(data, bytes):
            raise TypeError("output data must be bytes.")

        output_path = self.path(slot)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(data)

    def items(self) -> ItemsView[str, PreparedOutputArtifact]:
        return self._artifacts.items()


@dataclass(frozen=True)
class WorkerParameters:
    """Handler-facing read-only access to manifest execution parameters."""

    _params: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        if self._params is None:
            raise ValueError("worker parameters cannot be null.")
        object.__setattr__(
            self,
            "_params",
            MappingProxyType(dict(self._params)),
        )

    def get(self, name: str, default: Any = None) -> Any:
        _validate_name(name, "parameter name")
        return self._params.get(name, default)

    def required(self, name: str) -> Any:
        _validate_name(name, "parameter name")
        if name not in self._params:
            raise KeyError(f"Required parameter is missing: {name}")
        return self._params[name]

    def items(self) -> ItemsView[str, Any]:
        return self._params.items()


def _validate_name(value: str, name: str) -> None:
    if value is None or value.strip() == "":
        raise ValueError(f"{name} cannot be null or blank.")
