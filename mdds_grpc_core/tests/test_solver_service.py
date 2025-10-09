# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import unittest
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

    # Create gRPC request
    request = solver_pb2.SolveRequest(method=solving_method, matrix=matrix, rhs=rhs)

    # Mock gRPC context
    mock_context = MagicMock(spec=grpc.ServicerContext)

    # Call Solve
    response = service.Solve(request, mock_context)

    # Expected result from manual calculation
    expected_solution = [1 / 11, 7 / 11]

    # Validate response type and values
    assert isinstance(response, solver_pb2.SolveResponse)
    assert abs(response.solution[0] - expected_solution[0]) < 1e-7
    assert abs(response.solution[1] - expected_solution[1]) < 1e-7

    # Verify context was not marked with an error
    mock_context.set_code.assert_not_called()
    mock_context.set_details.assert_not_called()


def test_solve_unknown_method():
    """Test SolverService.Solve when method name is invalid."""
    service = SolverService()
    request = solver_pb2.SolveRequest(method="unknown_solver", matrix=[], rhs=[])

    mock_context = MagicMock(spec=grpc.ServicerContext)
    response = service.Solve(request, mock_context)

    # The response should be empty and context should be set to INVALID_ARGUMENT
    mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    mock_context.set_details.assert_called_once()
    assert len(response.solution) == 0


if __name__ == "__main__":
    unittest.main()
