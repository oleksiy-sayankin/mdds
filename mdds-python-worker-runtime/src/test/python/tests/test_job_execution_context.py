# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from pathlib import Path
from typing import cast, Any

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.execution.artifacts import (
    PreparedInputArtifact,
    PreparedOutputArtifact,
    InputArtifacts,
    OutputArtifacts,
    JobParameters,
)
from mdds_worker_runtime.execution.context import JobExecutionContext


def test_context_returns_input_artifact() -> None:
    matrix_artifact = PreparedInputArtifact(
        object_key="jobs/42/job-1/in/matrix.csv",
        local_path=Path("/tmp/mdds/jobs/42/job-1/in/matrix.csv"),
        format=ArtifactFormat.CSV,
    )
    context = _create_context(inputs={"matrix": matrix_artifact})

    assert context.input("matrix") == matrix_artifact


def test_context_returns_input_path() -> None:
    matrix_path = Path("/tmp/mdds/jobs/42/job-1/in/matrix.csv")
    context = _create_context(
        inputs={
            "matrix": PreparedInputArtifact(
                object_key="jobs/42/job-1/in/matrix.csv",
                local_path=matrix_path,
                format=ArtifactFormat.CSV,
            )
        }
    )

    assert context.input_path("matrix") == matrix_path


def test_context_returns_output_artifact() -> None:
    solution_artifact = PreparedOutputArtifact(
        object_key="jobs/42/job-1/out/solution.csv",
        local_path=Path("/tmp/mdds/jobs/42/job-1/out/solution.csv"),
        format=ArtifactFormat.CSV,
    )
    context = _create_context(outputs={"solution": solution_artifact})

    assert context.output("solution") == solution_artifact


def test_context_returns_output_path() -> None:
    solution_path = Path("/tmp/mdds/jobs/42/job-1/out/solution.csv")
    context = _create_context(
        outputs={
            "solution": PreparedOutputArtifact(
                object_key="jobs/42/job-1/out/solution.csv",
                local_path=solution_path,
                format=ArtifactFormat.CSV,
            )
        }
    )

    assert context.output_path("solution") == solution_path


def test_context_returns_optional_param() -> None:
    context = _create_context(params={"solvingMethod": "numpy_exact_solver"})

    assert context.param("solvingMethod") == "numpy_exact_solver"


def test_context_returns_default_param() -> None:
    context = _create_context(params={})

    assert context.param("missingParam", "default-value") == "default-value"


def test_context_returns_required_param() -> None:
    context = _create_context(params={"solvingMethod": "numpy_exact_solver"})

    assert context.required_param("solvingMethod") == "numpy_exact_solver"


def test_context_rejects_missing_required_param() -> None:
    context = _create_context(params={})

    with pytest.raises(KeyError) as exc_info:
        context.required_param("solvingMethod")

    assert "Required parameter is missing: solvingMethod" in str(exc_info.value)


def test_context_rejects_blank_input_slot() -> None:
    context = _create_context()

    with pytest.raises(ValueError, match="input slot cannot be null or blank."):
        context.input(" ")


def test_context_rejects_blank_output_slot() -> None:
    context = _create_context()

    with pytest.raises(ValueError, match="output slot cannot be null or blank."):
        context.output(" ")


def test_context_rejects_blank_parameter_name() -> None:
    context = _create_context()

    with pytest.raises(ValueError, match="parameter name cannot be null or blank."):
        context.param(" ")


def test_context_rejects_missing_input_slot() -> None:
    context = _create_context(inputs={})

    with pytest.raises(KeyError) as exc_info:
        context.input("matrix")

    assert "Input slot is not available: matrix" in str(exc_info.value)


def test_context_rejects_missing_output_slot() -> None:
    context = _create_context(outputs={})

    with pytest.raises(KeyError) as exc_info:
        context.output("solution")

    assert "Output slot is not declared: solution" in str(exc_info.value)


def _create_context(
    inputs: dict[str, PreparedInputArtifact] | None = None,
    outputs: dict[str, PreparedOutputArtifact] | None = None,
    params: dict[str, object] | None = None,
) -> JobExecutionContext:
    return JobExecutionContext(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        work_dir=Path("/tmp/mdds/jobs/42/job-1"),
        input_dir=Path("/tmp/mdds/jobs/42/job-1/in"),
        output_dir=Path("/tmp/mdds/jobs/42/job-1/out"),
        inputs=InputArtifacts(inputs or {}),
        outputs=OutputArtifacts(outputs or {}),
        params=JobParameters(params or {}),
    )


def test_context_required_param_rejects_blank_parameter_name() -> None:
    context = _create_context()

    with pytest.raises(ValueError, match="parameter name cannot be null or blank."):
        context.required_param(" ")


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


def test_context_rejects_null_inputs() -> None:
    with pytest.raises(ValueError, match="inputs cannot be null."):
        JobExecutionContext(
            user_id=42,
            job_id="job-1",
            job_type="SOLVING_SLAE",
            work_dir=Path("/tmp/mdds/jobs/42/job-1"),
            input_dir=Path("/tmp/mdds/jobs/42/job-1/in"),
            output_dir=Path("/tmp/mdds/jobs/42/job-1/out"),
            inputs=cast(Any, None),
            outputs=OutputArtifacts({}),
            params=JobParameters({}),
        )


def test_context_rejects_null_outputs() -> None:
    with pytest.raises(ValueError, match="outputs cannot be null."):
        JobExecutionContext(
            user_id=42,
            job_id="job-1",
            job_type="SOLVING_SLAE",
            work_dir=Path("/tmp/mdds/jobs/42/job-1"),
            input_dir=Path("/tmp/mdds/jobs/42/job-1/in"),
            output_dir=Path("/tmp/mdds/jobs/42/job-1/out"),
            inputs=InputArtifacts({}),
            outputs=cast(Any, None),
            params=JobParameters({}),
        )


def test_context_rejects_null_params() -> None:
    with pytest.raises(ValueError, match="params cannot be null."):
        JobExecutionContext(
            user_id=42,
            job_id="job-1",
            job_type="SOLVING_SLAE",
            work_dir=Path("/tmp/mdds/jobs/42/job-1"),
            input_dir=Path("/tmp/mdds/jobs/42/job-1/in"),
            output_dir=Path("/tmp/mdds/jobs/42/job-1/out"),
            inputs=InputArtifacts({}),
            outputs=OutputArtifacts({}),
            params=cast(Any, None),
        )
