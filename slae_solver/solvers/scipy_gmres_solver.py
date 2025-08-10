# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""scipy solver"""

import numpy as np
from scipy.sparse.linalg import gmres
from slae_solver.solver_interface import LinearSolverInterface


class ScipyGMRESSolver(LinearSolverInterface):
    """Solve sparse linear systems using GMRES iterative method from SciPy."""

    def __init__(self, tol=1e-8, maxiter=None):
        self.tol = tol
        self.maxiter = maxiter

    def solve(self, A, b):
        A_sparse = np.asarray(A, dtype=float)
        b = np.asarray(b, dtype=float)
        x, info = gmres(A_sparse, b, rtol=self.tol, maxiter=self.maxiter)
        if info != 0:
            raise RuntimeError(f"GMRES did not converge, info={info}")
        return x
