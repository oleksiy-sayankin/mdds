# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import unittest
import numpy as np
from slae_solver.solvers.scipy_gmres_solver import ScipyGmresSolver


def test_scipy_gmres_solver_sparse_matrix():
    # Create a small sparse system
    matrix = [[4, 1], [1, 3]]
    rhs = [1, 2]

    solver = ScipyGmresSolver(tol=1e-10, maxiter=50)
    x = solver.solve(matrix, rhs)

    # Verify Ax ≈ b
    assert np.allclose(np.array(matrix) @ x, np.array(rhs), atol=1e-8)


if __name__ == "__main__":
    unittest.main()
