# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import unittest
import numpy as np
from slae_solver.solvers.scipy_gmres_solver import ScipyGmresSolver


def test_scipy_gmres_solver_sparse_matrix():
    # Create a small sparse system
    A = np.array([[4, 1], [1, 3]], dtype=float)
    b = np.array([1, 2], dtype=float)

    solver = ScipyGmresSolver(tol=1e-10, maxiter=50)
    x = solver.solve(A, b)

    # Verify Ax ≈ b
    assert np.allclose(A @ x, b, atol=1e-8)


if __name__ == "__main__":
    unittest.main()
