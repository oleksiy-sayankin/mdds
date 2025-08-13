# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import subprocess
import time
import numpy as np
import requests
import pytest

from pathlib import Path

HOST = "127.0.0.1"
PORT = 8000
BASE_URL = f"http://{HOST}:{PORT}"
TIMEOUT = 2

# Directory with test resources
RESOURCES_DIR = Path(__file__).parent / "resources"

# All solver methods to test
SOLVER_METHODS = [
    "numpy_exact_solver",
    "numpy_lstsq_solver",
    "numpy_pinv_solver",
    "petsc_solver",
    "scipy_gmres_solver",
]

SERVER_PROCESS = None


def start_server():
    """
    Start the FastAPI server in a separate process.
    """
    global SERVER_PROCESS
    SERVER_PROCESS = subprocess.Popen(
        ["python", "-m", "run"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    # Wait for server to start
    time.sleep(TIMEOUT)


def stop_server():
    """
    Stop the FastAPI server process.
    """
    global SERVER_PROCESS
    if SERVER_PROCESS:
        SERVER_PROCESS.terminate()
        SERVER_PROCESS.wait()


@pytest.fixture(scope="session", autouse=True)
def server():
    """
    Pytest fixture to start and stop the server once for all tests.
    """
    start_server()
    yield
    stop_server()


@pytest.mark.parametrize("solver_method", SOLVER_METHODS)
def test_solve_endpoint(solver_method):
    """
    Test solving SLAE for different solver methods.
    Send matrix.csv, rhs.csv, method via POST request.
    Compare server's CSV output with expected solution CSV.
    """

    # Prepare files
    files = {
        "matrix": open(RESOURCES_DIR / "matrix.csv", "rb"),
        "rhs": open(RESOURCES_DIR / "rhs.csv", "rb"),
    }
    data = {"method": solver_method}

    # Send POST request
    response = requests.post(f"{BASE_URL}/solve", files=files, data=data)

    # Close file descriptors
    files["matrix"].close()
    files["rhs"].close()

    assert (
        response.status_code == 200
    ), f"Server returned {response.status_code}: {response.text}"

    # Convert CSV response to numpy array
    received_floats = np.loadtxt(
        response.iter_lines(decode_unicode=True), delimiter=","
    )

    expected_file = RESOURCES_DIR / "expected_solution.csv"

    # Load expected values
    expected_floats = np.loadtxt(expected_file, delimiter=",")

    # Compare arrays
    np.testing.assert_allclose(
        received_floats,
        expected_floats,
        rtol=1e-6,
        err_msg=f"Solver {solver_method} returned incorrect result",
    )
