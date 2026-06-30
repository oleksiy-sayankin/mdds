# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from pathlib import Path
import shutil


class LocalJobWorkspaceCleaner:
    """Removes local workspace for a job that never reached ExecutionRegistry."""

    def __init__(self, jobs_root: Path) -> None:
        if jobs_root is None:
            raise ValueError("jobs_root cannot be null.")

        self._jobs_root = jobs_root

    def cleanup_job_workspace(self, user_id: int, job_id: str) -> None:
        if user_id is None:
            raise ValueError("user_id cannot be null.")
        self._validate_path_segment("job_id", job_id)

        workspace_path = self._jobs_root / str(user_id) / job_id

        if not workspace_path.exists():
            return

        shutil.rmtree(workspace_path)

    @staticmethod
    def _validate_path_segment(name: str, value: str) -> None:
        if value is None or value.strip() == "":
            raise ValueError(f"{name} cannot be null or blank.")

        if value in {".", ".."} or "/" in value or "\\" in value:
            raise ValueError(f"{name} is not a safe path segment: {value}")
