# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mdds_worker_runtime.execution.artifacts import (
    PreparedInputArtifact,
    PreparedOutputArtifact,
    PreparedJobInputs,
    InputArtifacts,
    OutputArtifacts,
    JobParameters,
)
from mdds_worker_runtime.execution.object_keys import file_name_from_object_key
from mdds_worker_runtime.execution.workspace import JobWorkspace


@dataclass(frozen=True)
class JobExecutionContext:
    def __post_init__(self) -> None:
        if self.workspace is None:
            raise ValueError("workspace cannot be null.")
        if self.inputs is None:
            raise ValueError("inputs cannot be null.")
        if self.outputs is None:
            raise ValueError("outputs cannot be null.")
        if self.params is None:
            raise ValueError("params cannot be null.")

    workspace: JobWorkspace
    inputs: InputArtifacts
    outputs: OutputArtifacts
    params: JobParameters

    @property
    def user_id(self) -> int:
        return self.workspace.user_id

    @property
    def job_id(self) -> str:
        return self.workspace.job_id

    @property
    def job_type(self) -> str:
        return self.workspace.job_type

    @property
    def work_dir(self) -> Path:
        return self.workspace.work_dir

    @property
    def input_dir(self) -> Path:
        return self.workspace.input_dir

    @property
    def output_dir(self) -> Path:
        return self.workspace.output_dir

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
        workspace: JobWorkspace,
        prepared_job_inputs: PreparedJobInputs,
    ) -> JobExecutionContext:
        if workspace is None:
            raise ValueError("workspace cannot be null.")
        if prepared_job_inputs is None:
            raise ValueError("prepared_job_inputs cannot be null.")
        workspace.output_dir.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, PreparedOutputArtifact] = {}
        used_local_paths: set[Path] = set()

        for slot, artifact in workspace.manifest.outputs.items():
            local_path = workspace.output_dir / file_name_from_object_key(
                artifact.object_key
            )

            if local_path in used_local_paths:
                raise ValueError(f"Duplicate local output artifact path: {local_path}")

            used_local_paths.add(local_path)

            outputs[slot] = PreparedOutputArtifact(
                object_key=artifact.object_key,
                local_path=local_path,
                format=artifact.format,
            )

        return JobExecutionContext(
            workspace=workspace,
            inputs=InputArtifacts(prepared_job_inputs.inputs),
            outputs=OutputArtifacts(outputs),
            params=JobParameters(workspace.manifest.params),
        )
