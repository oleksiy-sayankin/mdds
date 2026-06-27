# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.output_artifact_uploader import (
    OutputArtifactUploadError,
    OutputArtifactUploadResult,
    OutputArtifactUploader,
    UploadedOutputArtifact,
)
from mdds_worker_runtime.storage.s3_client import S3Storage


def test_output_artifact_uploader_uploads_declared_output_artifact(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    output_path = tmp_path / "jobs" / "42" / "job-1" / "out" / "solution.csv"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"1.0\n2.0\n")

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=output_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    result = uploader.upload(context)

    storage.upload_file.assert_called_once_with(
        "jobs/42/job-1/out/solution.csv",
        output_path,
    )

    assert result == OutputArtifactUploadResult(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        uploaded_artifacts=(
            UploadedOutputArtifact(
                slot="solution",
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=output_path,
                size_bytes=len(b"1.0\n2.0\n"),
            ),
        ),
    )


def test_output_artifact_uploader_uploads_all_declared_output_artifacts(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    solution_path = tmp_path / "jobs" / "42" / "job-1" / "out" / "solution.csv"
    diagnostics_path = tmp_path / "jobs" / "42" / "job-1" / "out" / "diagnostics.txt"

    solution_path.parent.mkdir(parents=True)
    solution_path.write_bytes(b"1.0\n2.0\n")
    diagnostics_path.write_bytes(b"solver converged\n")

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=solution_path,
                format=ArtifactFormat.CSV,
            ),
            "diagnostics": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/diagnostics.json",
                local_path=diagnostics_path,
                format=ArtifactFormat.JSON,
            ),
        },
    )

    result = uploader.upload(context)

    storage.upload_file.assert_any_call(
        "jobs/42/job-1/out/solution.csv",
        solution_path,
    )
    storage.upload_file.assert_any_call(
        "jobs/42/job-1/out/diagnostics.json",
        diagnostics_path,
    )
    assert storage.upload_file.call_count == 2

    assert result.user_id == 42
    assert result.job_id == "job-1"
    assert result.job_type == "SOLVING_SLAE"
    assert result.uploaded_artifacts == (
        UploadedOutputArtifact(
            slot="solution",
            object_key="jobs/42/job-1/out/solution.csv",
            local_path=solution_path,
            size_bytes=len(b"1.0\n2.0\n"),
        ),
        UploadedOutputArtifact(
            slot="diagnostics",
            object_key="jobs/42/job-1/out/diagnostics.json",
            local_path=diagnostics_path,
            size_bytes=len(b"solver converged\n"),
        ),
    )


def test_output_artifact_uploader_uses_manifest_derived_object_key(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    output_path = tmp_path / "out" / "local-name.csv"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"result\n")

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/manifest-name.csv",
                local_path=output_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    uploader.upload(context)

    storage.upload_file.assert_called_once_with(
        "jobs/42/job-1/out/manifest-name.csv",
        output_path,
    )


def test_output_artifact_uploader_ignores_undeclared_files_in_output_directory(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    output_dir = tmp_path / "jobs" / "42" / "job-1" / "out"
    declared_path = output_dir / "solution.csv"
    undeclared_path = output_dir / "debug.tmp"

    output_dir.mkdir(parents=True)
    declared_path.write_bytes(b"solution\n")
    undeclared_path.write_bytes(b"must not be uploaded\n")

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=declared_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    result = uploader.upload(context)

    storage.upload_file.assert_called_once_with(
        "jobs/42/job-1/out/solution.csv",
        declared_path,
    )
    assert len(result.uploaded_artifacts) == 1
    assert result.uploaded_artifacts[0].local_path == declared_path


def test_output_artifact_uploader_rejects_null_storage() -> None:
    with pytest.raises(ValueError, match="storage cannot be null."):
        OutputArtifactUploader(cast(Any, None))


def test_output_artifact_uploader_rejects_null_context() -> None:
    uploader = OutputArtifactUploader(MagicMock(spec=S3Storage))

    with pytest.raises(ValueError, match="context cannot be null."):
        uploader.upload(cast(Any, None))


def test_output_artifact_uploader_rejects_missing_expected_output_artifact(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    output_path = tmp_path / "jobs" / "42" / "job-1" / "out" / "solution.csv"

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=output_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    with pytest.raises(
        OutputArtifactUploadError,
        match="Expected output artifact does not exist",
    ):
        uploader.upload(context)

    storage.upload_file.assert_not_called()


def test_output_artifact_uploader_rejects_directory_as_output_artifact(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    output_path = tmp_path / "jobs" / "42" / "job-1" / "out" / "solution.csv"
    output_path.mkdir(parents=True)

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=output_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    with pytest.raises(
        OutputArtifactUploadError,
        match="Expected output artifact is not a regular file",
    ):
        uploader.upload(context)

    storage.upload_file.assert_not_called()


@pytest.mark.parametrize("object_key", [None, "", " "])
def test_output_artifact_uploader_rejects_null_or_blank_object_key(
    tmp_path: Path,
    object_key: str | None,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    output_path = tmp_path / "out" / "solution.csv"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"solution\n")

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key=cast(str, object_key),
                local_path=output_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    with pytest.raises(
        OutputArtifactUploadError,
        match="Output object key cannot be null or blank",
    ):
        uploader.upload(context)

    storage.upload_file.assert_not_called()


def test_output_artifact_uploader_rejects_null_local_path(tmp_path: Path) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=cast(Path, None),
                format=ArtifactFormat.CSV,
            )
        },
    )

    with pytest.raises(
        OutputArtifactUploadError,
        match="Output local path cannot be null",
    ):
        uploader.upload(context)

    storage.upload_file.assert_not_called()


def test_output_artifact_uploader_wraps_storage_upload_failure(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    storage.upload_file.side_effect = RuntimeError("S3 is unavailable")

    uploader = OutputArtifactUploader(storage)

    output_path = tmp_path / "out" / "solution.csv"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"solution\n")

    context = _create_context(
        tmp_path,
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=output_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    with pytest.raises(
        OutputArtifactUploadError,
        match="Failed to upload output artifact",
    ) as exc_info:
        uploader.upload(context)

    assert "S3 is unavailable" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, RuntimeError)

    storage.upload_file.assert_called_once_with(
        "jobs/42/job-1/out/solution.csv",
        output_path,
    )


def test_output_artifact_uploader_returns_empty_result_when_no_outputs_declared(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    context = _create_context(tmp_path, outputs={})

    result = uploader.upload(context)

    assert result == OutputArtifactUploadResult(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        uploaded_artifacts=(),
    )
    storage.upload_file.assert_not_called()


def _create_context(
    tmp_path: Path,
    outputs: dict[str, PreparedOutputArtifact],
) -> JobExecutionContext:
    work_dir = tmp_path / "jobs" / "42" / "job-1"
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    return JobExecutionContext(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        work_dir=work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        inputs=InputArtifacts({}),
        outputs=OutputArtifacts(outputs),
        params=JobParameters({"solvingMethod": "numpy_exact_solver"}),
    )


@pytest.mark.parametrize("output_slot", [None, "", " "])
def test_output_artifact_uploader_rejects_null_or_blank_output_slot(
    tmp_path: Path,
    output_slot: str | None,
) -> None:
    storage = MagicMock(spec=S3Storage)
    uploader = OutputArtifactUploader(storage)

    output_path = tmp_path / "out" / "solution.csv"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"solution\n")

    context = _create_context(
        tmp_path,
        outputs={
            cast(str, output_slot): PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=output_path,
                format=ArtifactFormat.CSV,
            )
        },
    )

    with pytest.raises(
        OutputArtifactUploadError,
        match="Output slot cannot be null or blank",
    ):
        uploader.upload(context)

    storage.upload_file.assert_not_called()
