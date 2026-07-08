# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import csv
import io
from collections.abc import Callable
from typing import Any

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler

from mdds_python_worker_solving_slae.slae_solver.solver_interface import (
    LinearSolverInterface,
)
from mdds_python_worker_solving_slae.slae_solver.solvers.numpy_exact_solver import (
    NumpyExactSolver,
)
from mdds_python_worker_solving_slae.slae_solver.solvers.numpy_lstsq_solver import (
    NumpyLstsqSolver,
)
from mdds_python_worker_solving_slae.slae_solver.solvers.numpy_pinv_solver import (
    NumpyPinvSolver,
)
from mdds_python_worker_solving_slae.slae_solver.solvers.petsc_solver import (
    PetscSolver,
)
from mdds_python_worker_solving_slae.slae_solver.solvers.scipy_gmres_solver import (
    ScipyGmresSolver,
)

_MATRIX_INPUT_SLOT = "matrix"
_RHS_INPUT_SLOT = "rhs"
_SOLUTION_OUTPUT_SLOT = "solution"
_SOLVING_METHOD_PARAM = "solvingMethod"

_SOLVER_FACTORIES: dict[str, Callable[[], LinearSolverInterface]] = {
    "numpy_exact_solver": NumpyExactSolver,
    "numpy_lstsq_solver": NumpyLstsqSolver,
    "numpy_pinv_solver": NumpyPinvSolver,
    "petsc_solver": PetscSolver,
    "scipy_gmres_solver": ScipyGmresSolver,
}


class SlaeJobHandler(JobHandler):
    """JobHandler implementation for solving SLAE jobs."""

    def execute(self, context: JobExecutionContext) -> None:
        """Solve the SLAE and produce the declared solution artifact."""
        matrix = _read_matrix(context)
        rhs = _read_rhs(context)

        _validate_dimensions(matrix, rhs)

        solver = _resolve_solver(context)
        solution = solver.solve(matrix, rhs)

        context.outputs.write(
            _SOLUTION_OUTPUT_SLOT,
            _solution_to_csv_bytes(solution),
        )


def _resolve_solver(context: JobExecutionContext) -> LinearSolverInterface:
    solving_method = str(context.params.required(_SOLVING_METHOD_PARAM)).strip()

    if solving_method == "":
        raise ValueError("SLAE solving method cannot be blank.")

    solver_factory = _SOLVER_FACTORIES.get(solving_method)
    if solver_factory is None:
        supported_methods = ", ".join(sorted(_SOLVER_FACTORIES))
        raise ValueError(
            f"Unsupported SLAE solving method: {solving_method}. "
            f"Supported methods: {supported_methods}."
        )

    return solver_factory()


def _read_matrix(context: JobExecutionContext) -> list[list[float]]:
    matrix = _read_csv_matrix(
        context.inputs.read(_MATRIX_INPUT_SLOT), _MATRIX_INPUT_SLOT
    )

    if len(matrix) == 0:
        raise ValueError("Input artifact 'matrix' cannot be empty.")

    expected_columns = len(matrix[0])
    if expected_columns == 0:
        raise ValueError("Input artifact 'matrix' must contain at least one column.")

    for row_index, row in enumerate(matrix, start=1):
        if len(row) != expected_columns:
            raise ValueError(
                "Input artifact 'matrix' must be rectangular. "
                f"Row 1 has {expected_columns} columns, "
                f"but row {row_index} has {len(row)} columns."
            )

    return matrix


def _read_rhs(context: JobExecutionContext) -> list[float]:
    rows = _read_csv_matrix(context.inputs.read(_RHS_INPUT_SLOT), _RHS_INPUT_SLOT)

    if len(rows) == 0:
        raise ValueError("Input artifact 'rhs' cannot be empty.")

    rhs: list[float] = []
    for row_index, row in enumerate(rows, start=1):
        if len(row) != 1:
            raise ValueError(
                "Input artifact 'rhs' must contain exactly one value per row. "
                f"Row {row_index} has {len(row)} values."
            )
        rhs.append(row[0])

    return rhs


def _read_csv_matrix(raw_bytes: bytes, input_slot: str) -> list[list[float]]:
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError(
            f"Input artifact '{input_slot}' must be a UTF-8 encoded CSV file."
        ) from error

    rows: list[list[float]] = []

    try:
        reader = csv.reader(io.StringIO(text))
        for row_index, raw_row in enumerate(reader, start=1):
            if _is_blank_csv_row(raw_row):
                continue

            try:
                rows.append([float(cell.strip()) for cell in raw_row])
            except ValueError as error:
                raise ValueError(
                    f"Input artifact '{input_slot}' contains a non-numeric "
                    f"value in row {row_index}."
                ) from error
    except csv.Error as error:
        raise ValueError(
            f"Input artifact '{input_slot}' is not a valid CSV file."
        ) from error

    return rows


def _is_blank_csv_row(row: list[str]) -> bool:
    return len(row) == 0 or all(cell.strip() == "" for cell in row)


def _validate_dimensions(matrix: list[list[float]], rhs: list[float]) -> None:
    if len(matrix) != len(rhs):
        raise ValueError(
            "SLAE matrix row count must match rhs vector size. "
            f"Matrix rows: {len(matrix)}, rhs size: {len(rhs)}."
        )


def _solution_to_csv_bytes(solution: Any) -> bytes:
    values = _normalize_solution_vector(solution)

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")

    for value in values:
        writer.writerow([format(value, ".17g")])

    return buffer.getvalue().encode("utf-8")


def _normalize_solution_vector(solution: Any) -> list[float]:
    if hasattr(solution, "tolist"):
        solution = solution.tolist()

    if isinstance(solution, int | float):
        return [float(solution)]

    if not isinstance(solution, list | tuple):
        raise RuntimeError(
            f"SLAE solver returned unsupported solution type: {type(solution).__name__}."
        )

    values: list[float] = []
    for index, value in enumerate(solution):
        if isinstance(value, list | tuple):
            if len(value) != 1:
                raise RuntimeError(
                    "SLAE solver returned a nested solution vector with "
                    f"{len(value)} values at index {index}."
                )
            value = value[0]

        values.append(float(value))

    return values
