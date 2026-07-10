# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_python_worker_solving_slae.handler import SlaeJobHandler

_MATRIX_INPUT_SLOT = "matrix"
_RHS_INPUT_SLOT = "rhs"
_SOLUTION_OUTPUT_SLOT = "solution"
_SOLVING_METHOD_PARAM = "solvingMethod"

_EXPECTED_SOLUTION = np.asarray([1.0, 2.0, 3.0], dtype=float)

_MATRIX = [
    [10.0, 2.0, 1.0],
    [1.0, 5.0, 1.0],
    [2.0, 3.0, 10.0],
]

_RHS = (np.asarray(_MATRIX, dtype=float) @ _EXPECTED_SOLUTION).tolist()

_SOLVING_METHODS = [
    "numpy_exact_solver",
    "numpy_lstsq_solver",
    "numpy_pinv_solver",
    "petsc_solver",
    "scipy_gmres_solver",
]


@pytest.mark.parametrize("solving_method", _SOLVING_METHODS)
def test_slae_job_handler_solves_3x3_job_from_local_workspace(
    tmp_path: Path,
    solving_method: str,
) -> None:
    context = _create_job_execution_context(tmp_path, solving_method)

    SlaeJobHandler().execute(context)

    solution_path = context.outputs.path(_SOLUTION_OUTPUT_SLOT)
    assert solution_path.is_file()

    actual_solution = np.asarray(_read_csv_vector(solution_path), dtype=float)

    assert np.allclose(actual_solution, _EXPECTED_SOLUTION, atol=1e-8)


@pytest.mark.parametrize(
    ("matrix_rows", "rhs_rows", "expected_message"),
    [
        (
            [
                [10.0, "not-a-number", 1.0],
                [1.0, 5.0, 1.0],
                [2.0, 3.0, 10.0],
            ],
            [[17.0], [14.0], [38.0]],
            "Input artifact 'matrix' contains a non-numeric value in row 1.",
        ),
        (
            [
                [10.0, 2.0, 1.0],
                [1.0, 5.0, 1.0],
                [2.0, 3.0, 10.0],
            ],
            [[17.0], ["not-a-number"], [38.0]],
            "Input artifact 'rhs' contains a non-numeric value in row 2.",
        ),
        (
            [
                [10.0, 2.0, 1.0],
                [1.0, 5.0],
                [2.0, 3.0, 10.0],
            ],
            [[17.0], [14.0], [38.0]],
            "Input artifact 'matrix' must be rectangular.",
        ),
        (
            [],
            [[17.0], [14.0], [38.0]],
            "Input artifact 'matrix' cannot be empty.",
        ),
        (
            [
                [10.0, 2.0, 1.0],
                [1.0, 5.0, 1.0],
                [2.0, 3.0, 10.0],
            ],
            [],
            "Input artifact 'rhs' cannot be empty.",
        ),
        (
            [
                [10.0, 2.0, 1.0],
                [1.0, 5.0, 1.0],
                [2.0, 3.0, 10.0],
            ],
            [[17.0], [14.0, 15.0], [38.0]],
            "Input artifact 'rhs' must contain exactly one value per row.",
        ),
        (
            [
                [10.0, 2.0, 1.0],
                [1.0, 5.0, 1.0],
                [2.0, 3.0, 10.0],
            ],
            [[17.0], [14.0]],
            "SLAE matrix row count must match rhs vector size.",
        ),
    ],
)
def test_slae_job_handler_rejects_invalid_input_artifacts(
    tmp_path: Path,
    matrix_rows: list[list[object]],
    rhs_rows: list[list[object]],
    expected_message: str,
) -> None:
    context = _create_job_execution_context(
        tmp_path,
        "numpy_exact_solver",
        matrix_rows=matrix_rows,
        rhs_rows=rhs_rows,
    )

    with pytest.raises(ValueError, match=expected_message):
        SlaeJobHandler().execute(context)

    assert not context.outputs.path(_SOLUTION_OUTPUT_SLOT).exists()


@pytest.mark.parametrize(
    ("solving_method", "expected_message"),
    [
        (" ", "SLAE solving method cannot be blank."),
        ("unknown_solver", "Unsupported SLAE solving method: unknown_solver."),
    ],
)
def test_slae_job_handler_rejects_invalid_solving_method(
    tmp_path: Path,
    solving_method: str,
    expected_message: str,
) -> None:
    context = _create_job_execution_context(tmp_path, solving_method)

    with pytest.raises(ValueError, match=expected_message):
        SlaeJobHandler().execute(context)

    assert not context.outputs.path(_SOLUTION_OUTPUT_SLOT).exists()


def _create_job_execution_context(
    tmp_path: Path,
    solving_method: str,
    matrix_rows: list[list[object]] | None = None,
    rhs_rows: list[list[object]] | None = None,
) -> JobExecutionContext:
    user_id = 42
    job_id = "job-slae-1"
    job_type = "solving_slae"

    work_dir = tmp_path / "jobs" / str(user_id) / job_id
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    input_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)

    matrix_path = input_dir / "matrix.csv"
    rhs_path = input_dir / "rhs.csv"
    solution_path = output_dir / "solution.csv"

    _write_csv_rows(matrix_path, matrix_rows if matrix_rows is not None else _MATRIX)
    _write_csv_rows(rhs_path, rhs_rows if rhs_rows is not None else _rhs_to_rows(_RHS))

    workspace = cast(
        Any,
        SimpleNamespace(
            user_id=user_id,
            job_id=job_id,
            job_type=job_type,
            work_dir=work_dir,
            input_dir=input_dir,
            output_dir=output_dir,
        ),
    )

    return JobExecutionContext(
        workspace=workspace,
        inputs=InputArtifacts(
            {
                _MATRIX_INPUT_SLOT: PreparedInputArtifact(
                    object_key=f"jobs/{user_id}/{job_id}/in/matrix.csv",
                    local_path=matrix_path,
                    format=ArtifactFormat.CSV,
                ),
                _RHS_INPUT_SLOT: PreparedInputArtifact(
                    object_key=f"jobs/{user_id}/{job_id}/in/rhs.csv",
                    local_path=rhs_path,
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        outputs=OutputArtifacts(
            {
                _SOLUTION_OUTPUT_SLOT: PreparedOutputArtifact(
                    object_key=f"jobs/{user_id}/{job_id}/out/solution.csv",
                    local_path=solution_path,
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        params=JobParameters(
            {
                _SOLVING_METHOD_PARAM: solving_method,
            }
        ),
    )


def _rhs_to_rows(vector: list[float]) -> list[list[float]]:
    return [[value] for value in vector]


def _write_csv_rows(path: Path, rows: list[list[object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerows(rows)


def _read_csv_vector(path: Path) -> list[float]:
    with path.open("r", newline="", encoding="utf-8") as input_file:
        reader = csv.reader(input_file)
        return [float(row[0]) for row in reader if row]
