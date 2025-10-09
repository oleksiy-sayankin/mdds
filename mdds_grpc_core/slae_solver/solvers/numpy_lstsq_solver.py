# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""numpy lsts solver."""

import numpy as np
from slae_solver.solver_interface import LinearSolverInterface


class NumpyLstsqSolver(LinearSolverInterface):
    """Solve linear systems using least squares (numpy.linalg.lstsq)."""

    def solve(self, matrix: list[list[float]], rhs: list[float]) -> list[float]:
        matrix_np = np.asarray(matrix, dtype=float)
        rhs_np = np.asarray(rhs, dtype=float)
        x, *_ = np.linalg.lstsq(matrix_np, rhs_np, rcond=None)
        return x.tolist()
