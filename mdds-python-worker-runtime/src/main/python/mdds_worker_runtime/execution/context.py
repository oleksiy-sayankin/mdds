# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.artifacts import (
    PreparedInputArtifact,
    PreparedOutputArtifact,
    PreparedJobInputs,
    InputArtifacts,
    OutputArtifacts,
    JobParameters,
)
from mdds_worker_runtime.execution.object_keys import file_name_from_object_key


@dataclass(frozen=True)
class JobExecutionContext:
    def __post_init__(self) -> None:
        if self.inputs is None:
            raise ValueError("inputs cannot be null.")
        if self.outputs is None:
            raise ValueError("outputs cannot be null.")
        if self.params is None:
            raise ValueError("params cannot be null.")

    user_id: int
    job_id: str
    job_type: str

    work_dir: Path
    input_dir: Path
    output_dir: Path

    inputs: InputArtifacts
    outputs: OutputArtifacts
    params: JobParameters

    def input(self, slot: str) -> PreparedInputArtifact:
        return self.inputs.get(slot)

    def input_path(self, slot: str) -> Path:
        return self.inputs.path(slot)

    def output(self, slot: str) -> PreparedOutputArtifact:
        return self.outputs.get(slot)

    def output_path(self, slot: str) -> Path:
        return self.outputs.path(slot)

    def param(self, name: str, default: Any = None) -> Any:
        return self.params.get(name, default)

    def required_param(self, name: str) -> Any:
        return self.params.required(name)


class JobExecutionContextFactory:
    def __init__(self, jobs_root: Path) -> None:
        if jobs_root is None:
            raise ValueError("jobs_root cannot be null.")
        self._jobs_root = jobs_root

    def create(
        self,
        manifest: JobManifest,
        prepared_job_inputs: PreparedJobInputs,
    ) -> JobExecutionContext:
        if manifest is None:
            raise ValueError("manifest cannot be null.")
        if prepared_job_inputs is None:
            raise ValueError("prepared_inputs cannot be null.")

        if prepared_job_inputs.user_id != manifest.user_id:
            raise ValueError("prepared inputs user_id does not match manifest user_id.")

        if prepared_job_inputs.job_id != manifest.job_id:
            raise ValueError("prepared inputs job_id does not match manifest job_id.")

        work_dir = self._jobs_root / str(manifest.user_id) / manifest.job_id
        input_dir = prepared_job_inputs.input_dir
        output_dir = work_dir / "out"
        output_dir.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, PreparedOutputArtifact] = {}
        used_local_paths: set[Path] = set()

        for slot, artifact in manifest.outputs.items():
            local_path = output_dir / file_name_from_object_key(artifact.object_key)

            if local_path in used_local_paths:
                raise ValueError(f"Duplicate local output artifact path: {local_path}")

            used_local_paths.add(local_path)

            outputs[slot] = PreparedOutputArtifact(
                object_key=artifact.object_key,
                local_path=local_path,
                format=artifact.format,
            )

        return JobExecutionContext(
            user_id=manifest.user_id,
            job_id=manifest.job_id,
            job_type=manifest.job_type,
            work_dir=work_dir,
            input_dir=input_dir,
            output_dir=output_dir,
            inputs=InputArtifacts(prepared_job_inputs.inputs),
            outputs=OutputArtifacts(outputs),
            params=JobParameters(manifest.params),
        )
