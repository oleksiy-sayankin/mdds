# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Common module with abstract interface
"""
from abc import ABC, abstractmethod
from typing import Any

"""
Abstract interface for all linear system solvers.

Implementations must accept:
    - A: matrix of coefficients (2D, dense or sparse)
    - b: right-hand side vector (1D or 2D column)
and return:
    - solution vector x (type depends on implementation: could be list, numpy array, etc.)
"""


class LinearSolverInterface(ABC):

    @abstractmethod
    def solve(self, A: Any, b: Any) -> Any:
        """Solve the linear system A x = b and return the solution vector."""
        pass
