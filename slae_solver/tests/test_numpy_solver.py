# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import unittest
import numpy as np
from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver
from slae_solver.solvers.numpy_lstsq_solver import NumpyLeastSquaresSolver
from slae_solver.solvers.numpy_pinv_solver import NumpyPseudoinverseSolver


def test_exact_solver():
    A = np.array([[3, 2], [1, 4]])
    b = np.array([10, 8])
    solver = NumpyExactSolver()
    x = solver.solve(A, b)
    assert np.allclose(A @ x, b)


def test_lstsq_solver():
    A = np.array([[3, 2], [1, 4]])
    b = np.array([10, 8])
    solver = NumpyLeastSquaresSolver()
    x = solver.solve(A, b)
    assert np.allclose(A @ x, b)


def test_pinv_solver():
    A = np.array([[3, 2], [1, 4]])
    b = np.array([10, 8])
    solver = NumpyPseudoinverseSolver()
    x = solver.solve(A, b)
    assert np.allclose(A @ x, b)


if __name__ == "__main__":
    unittest.main()
