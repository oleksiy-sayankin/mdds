# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from pathlib import Path

import pytest

from mdds_worker_runtime.execution.workspace_cleaner import LocalJobWorkspaceCleaner


def test_local_job_workspace_cleaner_removes_whole_job_workspace(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "42" / "job-1"
    input_dir = workspace / "in"
    output_dir = workspace / "out"

    input_dir.mkdir(parents=True)
    output_dir.mkdir()

    (input_dir / "matrix.csv").write_text("1,2\n", encoding="utf-8")
    (output_dir / "solution.csv").write_text("3\n", encoding="utf-8")

    sibling_workspace = tmp_path / "42" / "job-2"
    sibling_workspace.mkdir(parents=True)
    (sibling_workspace / "keep.txt").write_text("keep", encoding="utf-8")

    cleaner = LocalJobWorkspaceCleaner(tmp_path)

    cleaner.cleanup_job_workspace(42, "job-1")

    assert not workspace.exists()
    assert sibling_workspace.exists()
    assert (sibling_workspace / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_local_job_workspace_cleaner_is_noop_when_workspace_does_not_exist(
    tmp_path: Path,
) -> None:
    cleaner = LocalJobWorkspaceCleaner(tmp_path)

    cleaner.cleanup_job_workspace(42, "missing-job")

    assert tmp_path.exists()


def test_local_job_workspace_cleaner_rejects_null_jobs_root() -> None:
    with pytest.raises(ValueError, match="jobs_root cannot be null."):
        LocalJobWorkspaceCleaner(None)


def test_local_job_workspace_cleaner_rejects_null_user_id(tmp_path: Path) -> None:
    cleaner = LocalJobWorkspaceCleaner(tmp_path)

    with pytest.raises(ValueError, match="user_id cannot be null."):
        cleaner.cleanup_job_workspace(None, "job-1")


@pytest.mark.parametrize("job_id", [None, "", " "])
def test_local_job_workspace_cleaner_rejects_blank_job_id(
    tmp_path: Path,
    job_id: str | None,
) -> None:
    cleaner = LocalJobWorkspaceCleaner(tmp_path)

    with pytest.raises(ValueError, match="job_id cannot be null or blank."):
        cleaner.cleanup_job_workspace(42, job_id)


@pytest.mark.parametrize(
    "job_id", [".", "..", "../job-1", "nested/job-1", r"nested\job-1"]
)
def test_local_job_workspace_cleaner_rejects_unsafe_job_id(
    tmp_path: Path,
    job_id: str,
) -> None:
    cleaner = LocalJobWorkspaceCleaner(tmp_path)

    with pytest.raises(ValueError, match="job_id is not a safe path segment"):
        cleaner.cleanup_job_workspace(42, job_id)
