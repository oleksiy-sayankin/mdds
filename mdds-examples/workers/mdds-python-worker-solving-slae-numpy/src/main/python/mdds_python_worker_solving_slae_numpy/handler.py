# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import csv
from io import StringIO

import numpy as np
from numpy.typing import NDArray

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.handler import WorkerHandler

_MATRIX_INPUT_SLOT = "matrix"
_RHS_INPUT_SLOT = "rhs"
_SOLUTION_OUTPUT_SLOT = "solution"


class SlaeWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> None:
        matrix = _read_matrix(context, _MATRIX_INPUT_SLOT)
        rhs = _read_vector(context, _RHS_INPUT_SLOT)

        _validate_dimensions(matrix, rhs)

        try:
            solution = np.linalg.solve(matrix, rhs)
        except np.linalg.LinAlgError as error:
            raise ValueError(
                f"Input slot '{_MATRIX_INPUT_SLOT}' must contain "
                "a non-singular matrix."
            ) from error

        context.outputs.write(_SOLUTION_OUTPUT_SLOT, _write_vector(solution))


def _read_matrix(
    context: WorkerExecutionContext,
    slot: str,
) -> NDArray[np.float64]:
    rows = _read_numeric_csv(context, slot, "matrix")

    if not rows or not rows[0] or any(len(row) != len(rows[0]) for row in rows):
        raise ValueError(
            f"Input slot '{slot}' must contain a non-empty rectangular CSV matrix."
        )

    return np.asarray(rows, dtype=np.float64)


def _read_vector(
    context: WorkerExecutionContext,
    slot: str,
) -> NDArray[np.float64]:
    rows = _read_numeric_csv(context, slot, "vector")

    if not rows or any(len(row) != 1 for row in rows):
        raise ValueError(
            f"Input slot '{slot}' must contain a non-empty numeric CSV vector "
            "with one value per row."
        )

    return np.asarray([row[0] for row in rows], dtype=np.float64)


def _read_numeric_csv(
    context: WorkerExecutionContext,
    slot: str,
    input_kind: str,
) -> list[list[float]]:
    try:
        content = context.inputs.read(slot).decode("utf-8")
        return [
            [float(value) for value in row]
            for row in csv.reader(StringIO(content), strict=True)
        ]
    except (csv.Error, ValueError) as error:
        raise ValueError(
            f"Input slot '{slot}' must contain a numeric CSV {input_kind}."
        ) from error


def _validate_dimensions(
    matrix: NDArray[np.float64],
    rhs: NDArray[np.float64],
) -> None:
    row_count, column_count = matrix.shape

    if row_count != column_count:
        raise ValueError(
            f"Input slot '{_MATRIX_INPUT_SLOT}' must contain a square matrix: "
            f"got {row_count} rows and {column_count} columns."
        )

    if len(rhs) != row_count:
        raise ValueError(
            "The number of elements in input slot "
            f"'{_RHS_INPUT_SLOT}' must match the size of input slot "
            f"'{_MATRIX_INPUT_SLOT}': got {len(rhs)} and {row_count}."
        )


def _write_vector(vector: NDArray[np.float64]) -> bytes:
    content = StringIO(newline="")
    writer = csv.writer(content, lineterminator="\n")
    writer.writerows((float(value),) for value in vector)
    return content.getvalue().encode("utf-8")
