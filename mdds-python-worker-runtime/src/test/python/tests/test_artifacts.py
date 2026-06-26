# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from pathlib import Path
from typing import cast, Any

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
    OutputArtifacts,
    JobParameters,
)


def test_input_artifacts_reads_bytes(tmp_path: Path) -> None:
    matrix_path = tmp_path / "matrix.csv"
    matrix_path.write_bytes(b"1,2\n3,4\n")

    artifacts = InputArtifacts(
        {
            "matrix": PreparedInputArtifact(
                object_key="jobs/42/job-1/in/matrix.csv",
                local_path=matrix_path,
                format=ArtifactFormat.CSV,
            )
        }
    )

    assert artifacts.read("matrix") == b"1,2\n3,4\n"


def test_output_artifacts_writes_bytes(tmp_path: Path) -> None:
    solution_path = tmp_path / "out" / "solution.csv"

    artifacts = OutputArtifacts(
        {
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=solution_path,
                format=ArtifactFormat.CSV,
            )
        }
    )

    artifacts.write("solution", b"1.0\n2.0\n")

    assert solution_path.read_bytes() == b"1.0\n2.0\n"


def test_job_parameters_required_returns_existing_value() -> None:
    params = JobParameters({"solvingMethod": "numpy_exact_solver"})

    assert params.required("solvingMethod") == "numpy_exact_solver"


def test_job_parameters_required_rejects_missing_value() -> None:
    params = JobParameters({})

    with pytest.raises(KeyError, match="Required parameter is missing: solvingMethod"):
        params.required("solvingMethod")


def test_input_artifacts_take_immutable_snapshot() -> None:
    source = {
        "matrix": PreparedInputArtifact(
            object_key="jobs/42/job-1/in/matrix.csv",
            local_path=Path("/tmp/matrix.csv"),
            format=ArtifactFormat.CSV,
        )
    }

    artifacts = InputArtifacts(source)
    source.clear()

    assert artifacts.get("matrix").object_key == "jobs/42/job-1/in/matrix.csv"


def test_input_artifacts_rejects_null_artifacts() -> None:
    with pytest.raises(ValueError, match="input artifacts cannot be null."):
        InputArtifacts(cast(Any, None))


def test_output_artifacts_rejects_null_artifacts() -> None:
    with pytest.raises(ValueError, match="output artifacts cannot be null."):
        OutputArtifacts(cast(Any, None))


def test_output_artifacts_write_rejects_null_data(tmp_path: Path) -> None:
    solution_path = tmp_path / "out" / "solution.csv"

    artifacts = OutputArtifacts(
        {
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=solution_path,
                format=ArtifactFormat.CSV,
            )
        }
    )

    with pytest.raises(ValueError, match="output data cannot be null."):
        artifacts.write("solution", cast(Any, None))


def test_output_artifacts_write_rejects_non_bytes_data(tmp_path: Path) -> None:
    solution_path = tmp_path / "out" / "solution.csv"

    artifacts = OutputArtifacts(
        {
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=solution_path,
                format=ArtifactFormat.CSV,
            )
        }
    )

    with pytest.raises(TypeError, match="output data must be bytes."):
        artifacts.write("solution", cast(Any, "1.0\n2.0\n"))


def test_job_parameters_rejects_null_params() -> None:
    with pytest.raises(ValueError, match="job parameters cannot be null."):
        JobParameters(cast(Any, None))
