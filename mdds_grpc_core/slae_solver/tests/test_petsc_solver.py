# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import unittest
import numpy as np

from slae_solver.solvers.petsc_solver import PetscSolver


def test_petsc_solver():
    matrix = [[4, 1], [1, 3]]
    rhs = [1, 2]
    solver = PetscSolver(ksp_type="cg", tol=1e-10, maxiter=50)
    x = solver.solve(matrix, rhs)
    assert np.allclose(np.array(matrix) @ x, np.array(rhs), atol=1e-8)


if __name__ == "__main__":
    unittest.main()
