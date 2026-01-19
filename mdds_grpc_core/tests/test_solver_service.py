# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import time
import unittest
import uuid

import grpc
from unittest.mock import MagicMock

import pytest
from generated import solver_pb2
from service import SolverService


@pytest.mark.parametrize(
    "solving_method",
    [
        "numpy_exact_solver",
        "numpy_lstsq_solver",
        "numpy_pinv_solver",
        "petsc_solver",
        "scipy_gmres_solver",
    ],
)
def test_all_solvers(solving_method):
    """
    Test SolverService.Solve with all solving methods.
    Checks that the response solution matches exact solution.
    """
    service = SolverService()
    # Prepare input system:
    #  4x + 1y = 1
    #  1x + 3y = 2
    # Solution: x = 1 / 11, y = 7 / 11
    matrix = [solver_pb2.Row(values=[4.0, 1.0]), solver_pb2.Row(values=[1.0, 3.0])]
    rhs = [1.0, 2.0]

    task_id = str(uuid.uuid4())

    # Create gRPC request
    request = solver_pb2.SubmitTaskRequest(
        method=solving_method, matrix=matrix, rhs=rhs, taskId=task_id
    )

    # Mock gRPC context
    mock_context = MagicMock(spec=grpc.ServicerContext)

    # Call Solve
    response = service.SubmitTask(request, mock_context)
    assert isinstance(response, solver_pb2.SubmitTaskResponse)
    assert response.requestStatus == solver_pb2.RequestStatus.COMPLETED

    request = solver_pb2.GetTaskStatusRequest(taskId=task_id)

    valid_statuses = {
        solver_pb2.TaskStatus.DONE,
        solver_pb2.TaskStatus.ERROR,
        solver_pb2.TaskStatus.CANCELLED,
    }

    response = service.GetTaskStatus(request, mock_context)
    task_status = response.taskStatus
    attempts = 1
    max_attempts = 20
    while task_status not in valid_statuses and attempts <= max_attempts:
        response = service.GetTaskStatus(request, mock_context)
        assert response.requestStatus == solver_pb2.RequestStatus.COMPLETED
        task_status = response.taskStatus
        time.sleep(1)
        attempts += 1

    assert task_status == solver_pb2.TaskStatus.DONE

    # Expected result from manual calculation
    expected_solution = [1 / 11, 7 / 11]

    # Validate response type and values
    assert abs(response.solution[0] - expected_solution[0]) < 1e-7
    assert abs(response.solution[1] - expected_solution[1]) < 1e-7

    # Verify context was not marked with an error
    mock_context.set_code.assert_not_called()
    mock_context.set_details.assert_not_called()


def test_solve_unknown_method():
    """Test SolverService.Solve when method name is invalid."""
    service = SolverService()
    task_id = str(uuid.uuid4())
    request = solver_pb2.SubmitTaskRequest(
        method="unknown_solver", matrix=[], rhs=[], taskId=task_id
    )

    mock_context = MagicMock(spec=grpc.ServicerContext)
    response = service.SubmitTask(request, mock_context)
    assert isinstance(response, solver_pb2.SubmitTaskResponse)
    assert response.requestStatus == solver_pb2.RequestStatus.DECLINED
    assert "Unknown method" in response.requestStatusDetails


if __name__ == "__main__":
    unittest.main()
