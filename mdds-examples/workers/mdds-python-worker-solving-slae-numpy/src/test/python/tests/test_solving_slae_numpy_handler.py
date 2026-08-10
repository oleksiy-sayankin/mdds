# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import csv
from unittest.mock import Mock, call

import numpy as np
import pytest
from mdds_python_worker_solving_slae_numpy.handler import SlaeWorkerHandler
from mdds_worker_runtime_common.execution.artifacts import (
    InputArtifacts,
    OutputArtifacts,
)
from mdds_worker_runtime_common.execution.context import WorkerExecutionContext


def _create_context(
    matrix: bytes,
    rhs: bytes,
) -> tuple[Mock, Mock, Mock]:
    inputs = Mock(spec=InputArtifacts)
    inputs.read.side_effect = [matrix, rhs]
    outputs = Mock(spec=OutputArtifacts)
    context = Mock(spec=WorkerExecutionContext)
    context.inputs = inputs
    context.outputs = outputs
    return context, inputs, outputs


def test_execute_solves_square_non_singular_system() -> None:
    context, inputs, outputs = _create_context(
        matrix=b"3,1\n1,2\n",
        rhs=b"9\n8\n",
    )

    SlaeWorkerHandler().execute(context)

    assert inputs.read.call_args_list == [
        call("matrix"),
        call("rhs"),
    ]
    outputs.write.assert_called_once_with(
        "solution",
        b"2.0\n3.0\n",
    )


@pytest.mark.parametrize(
    ("matrix", "rhs", "invalid_slot", "input_kind"),
    [
        (b"not-a-number,1\n1,2\n", b"9\n8\n", "matrix", "matrix"),
        (b"3,1\n1,2\n", b"not-a-number\n8\n", "rhs", "vector"),
    ],
)
def test_execute_rejects_non_numeric_input(
    matrix: bytes,
    rhs: bytes,
    invalid_slot: str,
    input_kind: str,
) -> None:
    context, _, outputs = _create_context(matrix, rhs)

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == (
        f"Input slot '{invalid_slot}' must contain a numeric CSV {input_kind}."
    )
    assert isinstance(error.value.__cause__, ValueError)
    outputs.write.assert_not_called()


def test_execute_rejects_input_with_invalid_utf8() -> None:
    context, _, outputs = _create_context(
        matrix=b"3,1\n1,2\n",
        rhs=b"\xff\n",
    )

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == ("Input slot 'rhs' must contain a numeric CSV vector.")
    assert isinstance(error.value.__cause__, UnicodeDecodeError)
    outputs.write.assert_not_called()


def test_execute_rejects_input_with_unterminated_csv_quote() -> None:
    context, _, outputs = _create_context(
        matrix=b'"1,2\n3,4\n',
        rhs=b"5\n6\n",
    )

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input slot 'matrix' must contain a numeric CSV matrix."
    )
    assert isinstance(error.value.__cause__, csv.Error)
    outputs.write.assert_not_called()


@pytest.mark.parametrize(
    "matrix",
    [
        b"",
        b"\n",
        b"1,2\n3\n",
    ],
)
def test_execute_rejects_empty_or_non_rectangular_matrix(matrix: bytes) -> None:
    context, _, outputs = _create_context(matrix, b"1\n2\n")

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input slot 'matrix' must contain a non-empty rectangular CSV matrix."
    )
    outputs.write.assert_not_called()


@pytest.mark.parametrize(
    "rhs",
    [
        b"",
        b"1,2\n",
    ],
)
def test_execute_rejects_empty_or_multi_column_rhs(rhs: bytes) -> None:
    context, _, outputs = _create_context(
        matrix=b"3,1\n1,2\n",
        rhs=rhs,
    )

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input slot 'rhs' must contain a non-empty numeric CSV vector "
        "with one value per row."
    )
    outputs.write.assert_not_called()


def test_execute_rejects_non_square_matrix() -> None:
    context, _, outputs = _create_context(
        matrix=b"1,2,3\n4,5,6\n",
        rhs=b"7\n8\n",
    )

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input slot 'matrix' must contain a square matrix: " "got 2 rows and 3 columns."
    )
    outputs.write.assert_not_called()


def test_execute_rejects_rhs_with_incompatible_dimension() -> None:
    context, _, outputs = _create_context(
        matrix=b"3,1\n1,2\n",
        rhs=b"9\n8\n7\n",
    )

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == (
        "The number of elements in input slot 'rhs' must match the size of "
        "input slot 'matrix': got 3 and 2."
    )
    outputs.write.assert_not_called()


def test_execute_rejects_singular_matrix() -> None:
    context, _, outputs = _create_context(
        matrix=b"1,2\n2,4\n",
        rhs=b"3\n6\n",
    )

    with pytest.raises(ValueError) as error:
        SlaeWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input slot 'matrix' must contain a non-singular matrix."
    )
    assert isinstance(error.value.__cause__, np.linalg.LinAlgError)
    outputs.write.assert_not_called()
