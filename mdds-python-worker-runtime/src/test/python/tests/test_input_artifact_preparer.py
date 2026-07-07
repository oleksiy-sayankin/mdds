# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef, JobManifest
from mdds_worker_runtime.execution.artifacts import (
    InputArtifactPreparer,
    PreparedInputArtifact,
    PreparedJobInputs,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.storage.s3_client import S3Storage

WORKER_ID = "worker-1"


def artifact_ref(object_key: str | None) -> ArtifactRef:
    return ArtifactRef(
        object_key=cast(str, object_key),
        format=ArtifactFormat.CSV,
    )


def test_prepare_downloads_input_artifacts_to_local_job_workspace(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    inputs = {
        "matrix": artifact_ref("jobs/42/job-1/in/matrix.csv"),
        "rhs": artifact_ref("jobs/42/job-1/in/rhs.csv"),
    }
    workspace = _workspace(tmp_path, inputs=inputs)

    prepared = preparer.prepare(workspace)

    expected_input_dir = tmp_path / "42" / "job-1" / "in"

    assert prepared == PreparedJobInputs(
        inputs={
            "matrix": PreparedInputArtifact(
                object_key="jobs/42/job-1/in/matrix.csv",
                local_path=expected_input_dir / "matrix.csv",
                format=ArtifactFormat.CSV,
            ),
            "rhs": PreparedInputArtifact(
                object_key="jobs/42/job-1/in/rhs.csv",
                local_path=expected_input_dir / "rhs.csv",
                format=ArtifactFormat.CSV,
            ),
        },
    )
    assert expected_input_dir.is_dir()

    storage.download_file.assert_any_call(
        "jobs/42/job-1/in/matrix.csv",
        expected_input_dir / "matrix.csv",
    )
    storage.download_file.assert_any_call(
        "jobs/42/job-1/in/rhs.csv",
        expected_input_dir / "rhs.csv",
    )
    assert storage.download_file.call_count == 2


def test_prepare_uses_file_name_from_s3_object_key_basename(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    workspace = _workspace(
        tmp_path,
        inputs={
            "matrix": artifact_ref(
                "jobs/42/job-1/in/nested/matrix.csv",
            ),
        },
    )

    prepared = preparer.prepare(workspace)

    expected_local_path = tmp_path / "42" / "job-1" / "in" / "matrix.csv"

    assert prepared.inputs["matrix"].local_path == expected_local_path
    storage.download_file.assert_called_once_with(
        "jobs/42/job-1/in/nested/matrix.csv",
        expected_local_path,
    )


def test_input_artifact_preparer_rejects_null_storage() -> None:
    with pytest.raises(ValueError, match="storage cannot be null"):
        InputArtifactPreparer(cast(S3Storage, None))


def test_prepare_rejects_null_workspace() -> None:
    preparer = InputArtifactPreparer(MagicMock(spec=S3Storage))

    with pytest.raises(ValueError, match="workspace cannot be null"):
        preparer.prepare(cast(JobWorkspace, None))


@pytest.mark.parametrize(
    "input_slot",
    [
        "",
        "   ",
        ".",
        "..",
        "../matrix",
        "matrix/file",
        r"matrix\file",
    ],
)
def test_prepare_rejects_invalid_input_slot(
    tmp_path: Path,
    input_slot: str,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    workspace = _workspace(
        tmp_path,
        inputs={
            input_slot: artifact_ref("jobs/42/job-1/in/matrix.csv"),
        },
    )

    with pytest.raises(ValueError, match="input_slot"):
        preparer.prepare(workspace)

    storage.download_file.assert_not_called()


def test_prepare_rejects_null_input_slot(tmp_path: Path) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    inputs = cast(
        dict[str, ArtifactRef],
        cast(
            object,
            {
                None: artifact_ref("jobs/42/job-1/in/matrix.csv"),
            },
        ),
    )
    workspace = _workspace(tmp_path, inputs=inputs)

    with pytest.raises(ValueError, match="input_slot"):
        preparer.prepare(workspace)

    storage.download_file.assert_not_called()


@pytest.mark.parametrize(
    "object_key",
    [
        None,
        "",
        "   ",
    ],
)
def test_prepare_rejects_null_or_blank_object_key(
    tmp_path: Path,
    object_key: str | None,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    workspace = _workspace(
        tmp_path,
        inputs={
            "matrix": artifact_ref(object_key),
        },
    )

    with pytest.raises(ValueError, match="object_key cannot be null or blank"):
        preparer.prepare(workspace)

    storage.download_file.assert_not_called()


def test_prepare_rejects_object_key_without_file_name(tmp_path: Path) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    workspace = _workspace(
        tmp_path,
        inputs={
            "matrix": artifact_ref("jobs/42/job-1/in/"),
        },
    )

    with pytest.raises(ValueError, match="Object key has no file name"):
        preparer.prepare(workspace)

    storage.download_file.assert_not_called()


@pytest.mark.parametrize(
    "object_key",
    [
        ".",
        "..",
    ],
)
def test_prepare_rejects_object_key_with_invalid_file_name(
    tmp_path: Path,
    object_key: str,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    workspace = _workspace(
        tmp_path,
        inputs={
            "matrix": artifact_ref(object_key),
        },
    )

    with pytest.raises(ValueError, match="Object key has no valid file name"):
        preparer.prepare(workspace)

    storage.download_file.assert_not_called()


def test_prepare_rejects_duplicate_local_input_artifact_path(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    workspace = _workspace(
        tmp_path,
        inputs={
            "matrix": artifact_ref(
                "jobs/42/job-1/in/a/matrix.csv",
            ),
            "rhs": artifact_ref(
                "jobs/42/job-1/in/b/matrix.csv",
            ),
        },
    )

    with pytest.raises(ValueError, match="Duplicate local input artifact path"):
        preparer.prepare(workspace)

    expected_first_local_path = tmp_path / "42" / "job-1" / "in" / "matrix.csv"

    storage.download_file.assert_called_once_with(
        "jobs/42/job-1/in/a/matrix.csv",
        expected_first_local_path,
    )


def test_prepared_input_artifact_is_immutable() -> None:
    artifact = PreparedInputArtifact(
        object_key="jobs/42/job-1/in/matrix.csv",
        local_path=Path("/tmp/matrix.csv"),
        format=ArtifactFormat.CSV,
    )

    with pytest.raises(FrozenInstanceError):
        setattr(artifact, "object_key", "jobs/42/job-1/in/rhs.csv")


def test_prepared_job_inputs_is_immutable() -> None:
    prepared = PreparedJobInputs(
        inputs={},
    )

    with pytest.raises(FrozenInstanceError):
        setattr(prepared, "job_id", "job-2")


def test_prepare_rejects_null_artifact_ref(tmp_path: Path) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage)

    inputs = cast(
        dict[str, ArtifactRef],
        cast(object, {"matrix": None}),
    )
    workspace = _workspace(tmp_path, inputs=inputs)

    with pytest.raises(
        ValueError,
        match="Input artifact ref for slot 'matrix' cannot be null",
    ):
        preparer.prepare(workspace)

    storage.download_file.assert_not_called()


def _workspace(
    tmp_path: Path,
    *,
    inputs: dict[str, ArtifactRef],
    user_id: int = 42,
    job_id: str = "job-1",
) -> JobWorkspace:
    work_dir = tmp_path / str(user_id) / job_id

    manifest = JobManifest(
        manifest_version=1,
        user_id=user_id,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        inputs=inputs,
        params={},
        outputs={},
    )

    return JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=work_dir / "in",
        output_dir=work_dir / "out",
        worker_id=WORKER_ID,
    )
