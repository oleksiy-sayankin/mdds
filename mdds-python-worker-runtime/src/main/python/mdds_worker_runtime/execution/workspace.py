# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from dataclasses import dataclass
from pathlib import Path

from mdds_worker_runtime.domain.manifest import JobManifest


@dataclass(frozen=True)
class JobWorkspace:
    manifest: JobManifest
    work_dir: Path
    input_dir: Path
    output_dir: Path
    worker_id: str

    @property
    def user_id(self) -> int:
        return self.manifest.user_id

    @property
    def job_id(self) -> str:
        return self.manifest.job_id

    @property
    def job_type(self) -> str:
        return self.manifest.job_type


class JobWorkspaceFactory:
    def __init__(self, jobs_root: Path, worker_id: str) -> None:
        if jobs_root is None:
            raise ValueError("jobs_root cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")

        self._jobs_root = jobs_root
        self._worker_id = worker_id

    def create(self, manifest: JobManifest) -> JobWorkspace:
        if manifest is None:
            raise ValueError("manifest cannot be null.")
        if manifest.job_id is None or manifest.job_id.strip() == "":
            raise ValueError("job_id cannot be null or blank.")

        work_dir = self._jobs_root / str(manifest.user_id) / manifest.job_id

        return JobWorkspace(
            manifest=manifest,
            work_dir=work_dir,
            input_dir=work_dir / "in",
            output_dir=work_dir / "out",
            worker_id=self._worker_id,
        )
