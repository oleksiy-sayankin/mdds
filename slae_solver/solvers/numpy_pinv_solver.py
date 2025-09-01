# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""numpy pinv solver"""

import numpy as np
from slae_solver.solver_interface import LinearSolverInterface


class NumpyPinvSolver(LinearSolverInterface):
    """Solve linear systems using pseudoinverse (numpy.linalg.pinv)."""

    def solve(self, A, b):
        A = np.asarray(A, dtype=float)
        b = np.asarray(b, dtype=float)
        return np.dot(np.linalg.pinv(A), b)
