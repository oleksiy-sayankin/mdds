# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef
from mdds_worker_runtime.execution.object_keys import file_name_from_object_key
from mdds_worker_runtime.storage.s3_client import S3Storage


@dataclass(frozen=True)
class PreparedInputArtifact:
    """Prepared local input artifact."""

    object_key: str
    local_path: Path
    format: ArtifactFormat


@dataclass(frozen=True)
class PreparedOutputArtifact:
    object_key: str
    local_path: Path
    format: ArtifactFormat


@dataclass(frozen=True)
class PreparedJobInputs:
    """Prepared local input artifacts for one job."""

    user_id: int
    job_id: str
    input_dir: Path
    inputs: Mapping[str, PreparedInputArtifact]


class InputArtifactPreparer:
    """Downloads declared input artifacts into local worker job workspace."""

    def __init__(self, storage: S3Storage, jobs_root: Path) -> None:
        if storage is None:
            raise ValueError("storage cannot be null.")
        if jobs_root is None:
            raise ValueError("jobs_root cannot be null.")

        self._storage = storage
        self._jobs_root = jobs_root

    def prepare(
        self,
        user_id: int,
        job_id: str,
        inputs: Mapping[str, ArtifactRef],
    ) -> PreparedJobInputs:
        if inputs is None:
            raise ValueError("inputs cannot be null.")
        self._validate_path_segment("job_id", job_id)
        input_dir = self._jobs_root / str(user_id) / job_id / "in"
        input_dir.mkdir(parents=True, exist_ok=True)

        prepared_inputs: dict[str, PreparedInputArtifact] = {}
        used_local_paths: set[Path] = set()

        for input_slot, artifact_ref in inputs.items():
            self._validate_slot_name("input_slot", input_slot)
            if artifact_ref is None:
                raise ValueError(
                    f"Input artifact ref for slot '{input_slot}' cannot be null."
                )
            file_name = file_name_from_object_key(artifact_ref.object_key)
            local_path = input_dir / file_name
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
            user_id=user_id,
            job_id=job_id,
            input_dir=input_dir,
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
