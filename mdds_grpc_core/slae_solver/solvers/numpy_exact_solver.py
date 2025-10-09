# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""numpy exact solver"""

import numpy as np
from slae_solver.solver_interface import LinearSolverInterface


class NumpyExactSolver(LinearSolverInterface):
    """Solve square linear systems exactly using numpy.linalg.solve."""

    def solve(self, matrix: list[list[float]], rhs: list[float]) -> list[float]:
        matrix_np = np.asarray(matrix, dtype=float)
        rhs_np = np.asarray(rhs, dtype=float)
        return np.linalg.solve(matrix_np, rhs_np).tolist()
