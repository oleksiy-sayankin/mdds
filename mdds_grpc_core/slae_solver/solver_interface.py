# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Common module with abstract interface
"""
from abc import ABC, abstractmethod
from numpy.typing import NDArray

"""
Abstract interface for all linear system solvers.

Implementations must accept:
    - matrix: matrix of coefficients (2D, dense or sparse)
    - rhs: right-hand side vector (1D or 2D column)
and return:
    - solution vector x (type depends on implementation: could be list, numpy array, etc.)
"""


class LinearSolverInterface(ABC):

    @abstractmethod
    def solve(self, matrix: list[list[float]], rhs: list[float]) -> NDArray:
        """Solve the linear system matrix * x = rhs and return the solution vector."""
        pass
