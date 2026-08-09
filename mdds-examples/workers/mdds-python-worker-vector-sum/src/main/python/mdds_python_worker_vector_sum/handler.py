# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import csv
from io import StringIO

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.handler import WorkerHandler

_VECTOR_A_INPUT_SLOT = "vector-a"
_VECTOR_B_INPUT_SLOT = "vector-b"
_SOLUTION_OUTPUT_SLOT = "solution"


class VectorSumWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> None:
        vector_a = _read_vector(context, _VECTOR_A_INPUT_SLOT)
        vector_b = _read_vector(context, _VECTOR_B_INPUT_SLOT)

        if len(vector_a) != len(vector_b):
            raise ValueError(
                "Input vectors must contain the same number of elements: "
                f"{_VECTOR_A_INPUT_SLOT} has {len(vector_a)}, "
                f"{_VECTOR_B_INPUT_SLOT} has {len(vector_b)}."
            )

        solution = [a + b for a, b in zip(vector_a, vector_b, strict=True)]
        context.outputs.write(_SOLUTION_OUTPUT_SLOT, _write_vector(solution))


def _read_vector(context: WorkerExecutionContext, slot: str) -> list[float]:
    try:
        content = context.inputs.read(slot).decode("utf-8")
        return [
            float(value)
            for row in csv.reader(StringIO(content), strict=True)
            for value in row
        ]
    except (csv.Error, ValueError) as error:
        raise ValueError(
            f"Input slot '{slot}' must contain a numeric CSV vector."
        ) from error


def _write_vector(vector: list[float]) -> bytes:
    content = StringIO(newline="")
    writer = csv.writer(content, lineterminator="\n")
    writer.writerows((value,) for value in vector)
    return content.getvalue().encode("utf-8")
