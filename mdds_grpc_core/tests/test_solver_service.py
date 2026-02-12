# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
import time
import unittest
import uuid
import grpc
import pytest

from unittest.mock import MagicMock
from generated import solver_pb2
from job_registry import JobRegistry
from logging_config import setup_logging
from service import SolverService

setup_logging()

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def setup_registry():
    registry = JobRegistry()
    registry.start()
    yield registry.active
    registry.stop()


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
def test_all_solvers(solving_method, setup_registry):
    """
    Test SolverService.Solve with all solving methods.
    Checks that the response solution matches exact solution.
    """
    logger.info(f"Testing {solving_method}")
    active = setup_registry
    service = SolverService(active)
    # Prepare input system:
    #  4x + 1y = 1
    #  1x + 3y = 2
    # Solution: x = 1 / 11, y = 7 / 11
    matrix = [solver_pb2.Row(values=[4.0, 1.0]), solver_pb2.Row(values=[1.0, 3.0])]
    rhs = [1.0, 2.0]

    job_id = str(uuid.uuid4())

    # Create gRPC request
    request = solver_pb2.SubmitJobRequest(
        method=solving_method, matrix=matrix, rhs=rhs, jobId=job_id
    )

    # Mock gRPC context
    mock_context = MagicMock(spec=grpc.ServicerContext)

    # Call Solve
    response = service.SubmitJob(request, mock_context)
    assert isinstance(response, solver_pb2.SubmitJobResponse)
    assert response.requestStatus == solver_pb2.RequestStatus.COMPLETED

    request = solver_pb2.GetJobStatusRequest(jobId=job_id)

    valid_statuses = {
        solver_pb2.JobStatus.DONE,
        solver_pb2.JobStatus.ERROR,
        solver_pb2.JobStatus.CANCELLED,
    }

    response = service.GetJobStatus(request, mock_context)
    job_status = response.jobStatus
    attempts = 1
    max_attempts = 20
    while job_status not in valid_statuses and attempts <= max_attempts:
        response = service.GetJobStatus(request, mock_context)
        assert response.requestStatus == solver_pb2.RequestStatus.COMPLETED
        job_status = response.jobStatus
        time.sleep(1)
        attempts += 1

    assert job_status == solver_pb2.JobStatus.DONE

    # Expected result from manual calculation
    expected_solution = [1 / 11, 7 / 11]

    # Validate response type and values
    assert abs(response.solution[0] - expected_solution[0]) < 1e-7
    assert abs(response.solution[1] - expected_solution[1]) < 1e-7

    # Verify context was not marked with an error
    mock_context.set_code.assert_not_called()
    mock_context.set_details.assert_not_called()


def test_solve_unknown_method(setup_registry):
    """Test SolverService.Solve when method name is invalid."""
    active = setup_registry
    service = SolverService(active)
    job_id = str(uuid.uuid4())
    request = solver_pb2.SubmitJobRequest(
        method="unknown_solver", matrix=[], rhs=[], jobId=job_id
    )

    mock_context = MagicMock(spec=grpc.ServicerContext)
    response = service.SubmitJob(request, mock_context)
    assert isinstance(response, solver_pb2.SubmitJobResponse)
    assert response.requestStatus == solver_pb2.RequestStatus.DECLINED
    assert "Unknown method" in response.requestStatusDetails


if __name__ == "__main__":
    unittest.main()
