# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""numpy pinv solver"""

import numpy as np
from slae_solver.solver_interface import LinearSolverInterface


class NumpyPinvSolver(LinearSolverInterface):
    """Solve linear systems using pseudoinverse (numpy.linalg.pinv)."""

    def solve(self, matrix, rhs):
        matrix_np = np.asarray(matrix, dtype=float)
        rhs_np = np.asarray(rhs, dtype=float)
        return np.dot(np.linalg.pinv(matrix_np), rhs_np)
