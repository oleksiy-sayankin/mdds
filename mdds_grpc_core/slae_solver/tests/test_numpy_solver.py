# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import unittest
import numpy as np
from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver
from slae_solver.solvers.numpy_lstsq_solver import NumpyLstsqSolver
from slae_solver.solvers.numpy_pinv_solver import NumpyPinvSolver


def test_exact_solver():
    matrix = [[3, 2], [1, 4]]
    rhs = [10, 8]
    solver = NumpyExactSolver()
    x = solver.solve(matrix, rhs)
    assert np.allclose(np.array(matrix) @ x, np.array(rhs))


def test_lstsq_solver():
    matrix = [[3, 2], [1, 4]]
    rhs = [10, 8]
    solver = NumpyLstsqSolver()
    x = solver.solve(matrix, rhs)
    assert np.allclose(np.array(matrix) @ x, np.array(rhs))


def test_pinv_solver():
    matrix = [[3, 2], [1, 4]]
    rhs = [10, 8]
    solver = NumpyPinvSolver()
    x = solver.solve(matrix, rhs)
    assert np.allclose(np.array(matrix) @ x, np.array(rhs))


if __name__ == "__main__":
    unittest.main()
