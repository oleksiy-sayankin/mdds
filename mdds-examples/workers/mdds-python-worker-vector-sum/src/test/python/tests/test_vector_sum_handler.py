# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import csv
from unittest.mock import Mock, call

import pytest
from mdds_python_worker_vector_sum.handler import VectorSumWorkerHandler
from mdds_worker_runtime_common.execution.artifacts import (
    InputArtifacts,
    OutputArtifacts,
)
from mdds_worker_runtime_common.execution.context import WorkerExecutionContext


def test_execute_sums_two_input_vectors() -> None:
    inputs = Mock(spec=InputArtifacts)
    inputs.read.side_effect = [
        b"1\n2\n3\n",
        b"4\n5\n6\n",
    ]
    outputs = Mock(spec=OutputArtifacts)
    context = Mock(spec=WorkerExecutionContext)
    context.inputs = inputs
    context.outputs = outputs

    VectorSumWorkerHandler().execute(context)

    assert inputs.read.call_args_list == [
        call("vector-a"),
        call("vector-b"),
    ]
    outputs.write.assert_called_once_with(
        "solution",
        b"5.0\n7.0\n9.0\n",
    )


def test_execute_rejects_input_vectors_with_different_lengths() -> None:
    inputs = Mock(spec=InputArtifacts)
    inputs.read.side_effect = [
        b"1\n2\n3\n",
        b"4\n5\n",
    ]
    outputs = Mock(spec=OutputArtifacts)
    context = Mock(spec=WorkerExecutionContext)
    context.inputs = inputs
    context.outputs = outputs

    with pytest.raises(ValueError) as error:
        VectorSumWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input vectors must contain the same number of elements: "
        "vector-a has 3, vector-b has 2."
    )
    outputs.write.assert_not_called()


@pytest.mark.parametrize(
    ("vector_a", "vector_b", "invalid_slot"),
    [
        (b"not-a-number\n", b"4\n5\n6\n", "vector-a"),
        (b"1\n2\n3\n", b"4\nnot-a-number\n6\n", "vector-b"),
    ],
)
def test_execute_rejects_non_numeric_input_vector(
    vector_a: bytes,
    vector_b: bytes,
    invalid_slot: str,
) -> None:
    inputs = Mock(spec=InputArtifacts)
    inputs.read.side_effect = [vector_a, vector_b]
    outputs = Mock(spec=OutputArtifacts)
    context = Mock(spec=WorkerExecutionContext)
    context.inputs = inputs
    context.outputs = outputs

    with pytest.raises(ValueError) as error:
        VectorSumWorkerHandler().execute(context)

    assert str(error.value) == (
        f"Input slot '{invalid_slot}' must contain a numeric CSV vector."
    )
    assert isinstance(error.value.__cause__, ValueError)
    outputs.write.assert_not_called()


def test_execute_rejects_input_vector_with_invalid_utf8() -> None:
    inputs = Mock(spec=InputArtifacts)
    inputs.read.side_effect = [
        b"\xff\n",
        b"4\n5\n6\n",
    ]
    outputs = Mock(spec=OutputArtifacts)
    context = Mock(spec=WorkerExecutionContext)
    context.inputs = inputs
    context.outputs = outputs

    with pytest.raises(ValueError) as error:
        VectorSumWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input slot 'vector-a' must contain a numeric CSV vector."
    )
    assert isinstance(error.value.__cause__, UnicodeDecodeError)
    outputs.write.assert_not_called()


def test_execute_rejects_input_vector_with_unterminated_csv_quote() -> None:
    inputs = Mock(spec=InputArtifacts)
    inputs.read.side_effect = [
        b'"1\n',
        b"2\n",
    ]
    outputs = Mock(spec=OutputArtifacts)
    context = Mock(spec=WorkerExecutionContext)
    context.inputs = inputs
    context.outputs = outputs

    with pytest.raises(ValueError) as error:
        VectorSumWorkerHandler().execute(context)

    assert str(error.value) == (
        "Input slot 'vector-a' must contain a numeric CSV vector."
    )
    assert isinstance(error.value.__cause__, csv.Error)
    outputs.write.assert_not_called()
