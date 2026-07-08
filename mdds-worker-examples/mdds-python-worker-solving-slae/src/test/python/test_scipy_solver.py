# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import numpy as np
from mdds_python_worker_solving_slae.slae_solver.solvers.scipy_gmres_solver import (
    ScipyGmresSolver,
)


def test_scipy_gmres_solver_sparse_matrix():
    # Create a small linear system
    matrix = [[4, 1], [1, 3]]
    rhs = [1, 2]

    solver = ScipyGmresSolver(tol=1e-10, maxiter=50)
    x = solver.solve(matrix, rhs)

    # Verify Ax ≈ b
    assert np.allclose(np.array(matrix) @ x, np.array(rhs), atol=1e-8)
