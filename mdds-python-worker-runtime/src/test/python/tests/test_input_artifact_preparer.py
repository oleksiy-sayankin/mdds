# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef
from mdds_worker_runtime.execution.artifacts import (
    InputArtifactPreparer,
    PreparedInputArtifact,
    PreparedJobInputs,
)
from mdds_worker_runtime.storage.s3_client import S3Storage


def artifact_ref(object_key: str | None) -> ArtifactRef:
    return ArtifactRef(
        object_key=object_key,
        format=ArtifactFormat.CSV,
    )


def test_prepare_downloads_input_artifacts_to_local_job_workspace(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage, tmp_path)

    inputs = {
        "matrix": artifact_ref("jobs/42/job-1/in/matrix.csv"),
        "rhs": artifact_ref("jobs/42/job-1/in/rhs.csv"),
    }

    prepared = preparer.prepare(
        user_id=42,
        job_id="job-1",
        inputs=inputs,
    )

    expected_input_dir = tmp_path / "42" / "job-1" / "in"

    assert prepared == PreparedJobInputs(
        user_id=42,
        job_id="job-1",
        input_dir=expected_input_dir,
        inputs={
            "matrix": PreparedInputArtifact(
                object_key="jobs/42/job-1/in/matrix.csv",
                local_path=expected_input_dir / "matrix.csv",
            ),
            "rhs": PreparedInputArtifact(
                object_key="jobs/42/job-1/in/rhs.csv",
                local_path=expected_input_dir / "rhs.csv",
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
    preparer = InputArtifactPreparer(storage, tmp_path)

    prepared = preparer.prepare(
        user_id=42,
        job_id="job-1",
        inputs={
            "matrix": artifact_ref(
                "jobs/42/job-1/in/nested/matrix.csv",
            ),
        },
    )

    expected_local_path = tmp_path / "42" / "job-1" / "in" / "matrix.csv"

    assert prepared.inputs["matrix"].local_path == expected_local_path
    storage.download_file.assert_called_once_with(
        "jobs/42/job-1/in/nested/matrix.csv",
        expected_local_path,
    )


def test_input_artifact_preparer_rejects_null_storage(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="storage cannot be null"):
        InputArtifactPreparer(None, tmp_path)


def test_input_artifact_preparer_rejects_null_jobs_root() -> None:
    with pytest.raises(ValueError, match="jobs_root cannot be null"):
        InputArtifactPreparer(MagicMock(spec=S3Storage), None)


def test_prepare_rejects_null_inputs(tmp_path: Path) -> None:
    preparer = InputArtifactPreparer(MagicMock(spec=S3Storage), tmp_path)

    with pytest.raises(ValueError, match="inputs cannot be null"):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs=None,
        )


@pytest.mark.parametrize(
    "job_id",
    [
        None,
        "",
        "   ",
        ".",
        "..",
        "../evil",
        "evil/job",
        r"evil\job",
    ],
)
def test_prepare_rejects_unsafe_job_id(
    tmp_path: Path,
    job_id: str | None,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage, tmp_path)

    with pytest.raises(ValueError, match="job_id"):
        preparer.prepare(
            user_id=42,
            job_id=job_id,
            inputs={},
        )

    storage.download_file.assert_not_called()


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
    preparer = InputArtifactPreparer(storage, tmp_path)

    with pytest.raises(ValueError, match="input_slot"):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs={
                input_slot: artifact_ref("jobs/42/job-1/in/matrix.csv"),
            },
        )

    storage.download_file.assert_not_called()


def test_prepare_rejects_null_input_slot(tmp_path: Path) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage, tmp_path)

    inputs = cast(
        dict[str, ArtifactRef],
        cast(
            object,
            {
                None: artifact_ref("jobs/42/job-1/in/matrix.csv"),
            },
        ),
    )

    with pytest.raises(ValueError, match="input_slot"):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs=inputs,
        )

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
    preparer = InputArtifactPreparer(storage, tmp_path)

    with pytest.raises(ValueError, match="object_key cannot be null or blank"):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs={
                "matrix": artifact_ref(object_key),
            },
        )

    storage.download_file.assert_not_called()


def test_prepare_rejects_object_key_without_file_name(tmp_path: Path) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage, tmp_path)

    with pytest.raises(ValueError, match="Object key has no file name"):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs={
                "matrix": artifact_ref("jobs/42/job-1/in/"),
            },
        )

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
    preparer = InputArtifactPreparer(storage, tmp_path)

    with pytest.raises(ValueError, match="Object key has no valid file name"):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs={
                "matrix": artifact_ref(object_key),
            },
        )

    storage.download_file.assert_not_called()


def test_prepare_rejects_duplicate_local_input_artifact_path(
    tmp_path: Path,
) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage, tmp_path)

    with pytest.raises(ValueError, match="Duplicate local input artifact path"):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs={
                "matrix": artifact_ref(
                    "jobs/42/job-1/in/a/matrix.csv",
                ),
                "rhs": artifact_ref(
                    "jobs/42/job-1/in/b/matrix.csv",
                ),
            },
        )

    expected_first_local_path = tmp_path / "42" / "job-1" / "in" / "matrix.csv"

    storage.download_file.assert_called_once_with(
        "jobs/42/job-1/in/a/matrix.csv",
        expected_first_local_path,
    )


def test_prepared_input_artifact_is_immutable() -> None:
    artifact = PreparedInputArtifact(
        object_key="jobs/42/job-1/in/matrix.csv",
        local_path=Path("/tmp/matrix.csv"),
    )

    with pytest.raises(FrozenInstanceError):
        setattr(artifact, "object_key", "jobs/42/job-1/in/rhs.csv")


def test_prepared_job_inputs_is_immutable() -> None:
    prepared = PreparedJobInputs(
        user_id=42,
        job_id="job-1",
        input_dir=Path("/tmp/in"),
        inputs={},
    )

    with pytest.raises(FrozenInstanceError):
        setattr(prepared, "job_id", "job-2")


def test_prepare_rejects_null_artifact_ref(tmp_path: Path) -> None:
    storage = MagicMock(spec=S3Storage)
    preparer = InputArtifactPreparer(storage, tmp_path)

    inputs = cast(
        dict[str, ArtifactRef],
        cast(object, {"matrix": None}),
    )

    with pytest.raises(
        ValueError,
        match="Input artifact ref for slot 'matrix' cannot be null",
    ):
        preparer.prepare(
            user_id=42,
            job_id="job-1",
            inputs=inputs,
        )

    storage.download_file.assert_not_called()
