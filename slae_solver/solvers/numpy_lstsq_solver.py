# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""numpy lsts solver."""

import numpy as np
from slae_solver.solver_interface import LinearSolverInterface


class NumpyLstsqSolver(LinearSolverInterface):
    """Solve linear systems using least squares (numpy.linalg.lstsq)."""

    def solve(self, A, b):
        A = np.asarray(A, dtype=float)
        b = np.asarray(b, dtype=float)
        x, *_ = np.linalg.lstsq(A, b, rcond=None)
        return x
