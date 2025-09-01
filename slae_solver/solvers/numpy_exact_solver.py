# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""numpy exact solver"""

import numpy as np
from slae_solver.solver_interface import LinearSolverInterface


class NumpyExactSolver(LinearSolverInterface):
    """Solve square linear systems exactly using numpy.linalg.solve."""

    def solve(self, A, b):
        A = np.asarray(A, dtype=float)
        b = np.asarray(b, dtype=float)
        return np.linalg.solve(A, b)
