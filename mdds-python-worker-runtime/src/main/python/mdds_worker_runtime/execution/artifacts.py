# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from collections.abc import ItemsView
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.execution.object_keys import file_name_from_object_key
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.storage.s3_client import S3Storage


@dataclass(frozen=True)
class PreparedInputArtifact:
    """Runtime-internal description of an input artifact downloaded to local disk."""

    object_key: str
    local_path: Path
    format: ArtifactFormat


@dataclass(frozen=True)
class PreparedOutputArtifact:
    """Runtime-internal description of an output artifact expected on local disk."""

    object_key: str
    local_path: Path
    format: ArtifactFormat


@dataclass(frozen=True)
class PreparedJobInputs:
    """Runtime-internal result of preparing all input artifacts for one job."""

    inputs: Mapping[str, PreparedInputArtifact]


class InputArtifactPreparer:
    """Downloads declared input artifacts into local worker job workspace."""

    def __init__(
        self,
        storage: S3Storage,
    ) -> None:
        if storage is None:
            raise ValueError("storage cannot be null.")

        self._storage = storage

    def prepare(
        self,
        workspace: JobWorkspace,
    ) -> PreparedJobInputs:
        if workspace is None:
            raise ValueError("workspace cannot be null.")
        workspace.input_dir.mkdir(parents=True, exist_ok=True)

        prepared_inputs: dict[str, PreparedInputArtifact] = {}
        used_local_paths: set[Path] = set()

        for input_slot, artifact_ref in workspace.manifest.inputs.items():
            self._validate_slot_name("input_slot", input_slot)
            if artifact_ref is None:
                raise ValueError(
                    f"Input artifact ref for slot '{input_slot}' cannot be null."
                )
            file_name = file_name_from_object_key(artifact_ref.object_key)
            local_path = workspace.input_dir / file_name
            if local_path in used_local_paths:
                raise ValueError(f"Duplicate local input artifact path: {local_path}")
            used_local_paths.add(local_path)

            self._storage.download_file(artifact_ref.object_key, local_path)
            prepared_inputs[input_slot] = PreparedInputArtifact(
                object_key=artifact_ref.object_key,
                local_path=local_path,
                format=artifact_ref.format,
            )

        return PreparedJobInputs(
            inputs=prepared_inputs,
        )

    @staticmethod
    def _validate_path_segment(name: str, value: str) -> None:
        if value is None or value.strip() == "":
            raise ValueError(f"{name} cannot be null or blank.")

        if value in {".", ".."} or "/" in value or "\\" in value:
            raise ValueError(f"{name} is not a safe path segment: {value}")

    @staticmethod
    def _validate_slot_name(name: str, value: str) -> None:
        if value is None or value.strip() == "":
            raise ValueError(f"{name} cannot be null or blank.")

        if value in {".", ".."} or "/" in value or "\\" in value:
            raise ValueError(f"{name} is not a valid slot name: {value}")


@dataclass(frozen=True)
class InputArtifacts:
    """Handler-facing read-only access to prepared input artifacts.

    This is a public API for JobHandler implementations.
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


@dataclass(frozen=True)
class OutputArtifacts:
    """Handler-facing access to declared output artifacts.

    The handler writes output bytes to local files only.
    The Worker Runtime uploads those files to object storage after
    successful execution.
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
class JobParameters:
    """Handler-facing read-only access to manifest execution parameters."""

    _params: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        if self._params is None:
            raise ValueError("job parameters cannot be null.")
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


def _validate_name(value: str, name: str) -> None:
    if value is None or value.strip() == "":
        raise ValueError(f"{name} cannot be null or blank.")
