# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import unittest
import numpy as np
from scipy.sparse import csr_matrix

from slae_solver.solvers.petsc_solver import PETScSolver


def test_petsc_solver_with_sparse_matrix():
    # Simple symmetric positive-definite system
    A = csr_matrix([[4, 1], [1, 3]], dtype=float)
    b = np.array([1, 2], dtype=float)

    solver = PETScSolver(ksp_type="cg", tol=1e-10, maxiter=50)
    x = solver.solve(A, b)

    assert np.allclose(A @ x, b, atol=1e-8)


if __name__ == "__main__":
    unittest.main()
