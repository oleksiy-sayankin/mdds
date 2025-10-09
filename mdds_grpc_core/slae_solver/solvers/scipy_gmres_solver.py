# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""scipy solver"""

import numpy as np
from scipy.sparse.linalg import gmres
from slae_solver.solver_interface import LinearSolverInterface
from numpy.typing import NDArray


class ScipyGmresSolver(LinearSolverInterface):
    """Solve sparse linear systems using GMRES iterative method from SciPy."""

    def __init__(self, tol=1e-8, maxiter=None):
        self.tol = tol
        self.maxiter = maxiter

    def solve(self, matrix: list[list[float]], rhs: list[float]) -> NDArray:
        matrix_np = np.asarray(matrix, dtype=float)
        rhs_np = np.asarray(rhs, dtype=float)
        x, info = gmres(matrix_np, rhs_np, rtol=self.tol, maxiter=self.maxiter)
        if info != 0:
            raise RuntimeError(f"GMRES did not converge, info={info}")
        return x
