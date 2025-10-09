# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""pets solver"""

from slae_solver.solver_interface import LinearSolverInterface
from scipy.sparse import csr_matrix
from numpy.typing import NDArray


class PetscSolver(LinearSolverInterface):
    """Solve linear systems using PETSc (KSP solver)."""

    def __init__(self, ksp_type="gmres", tol=1e-8, maxiter=1000):
        try:
            from petsc4py import PETSc
        except ImportError:
            raise ImportError("petsc4py is required for PETScSolver")
        self.PETSc = PETSc
        self.ksp_type = ksp_type
        self.tol = tol
        self.maxiter = maxiter

    def solve(self, matrix: list[list[float]], rhs: list[float]) -> NDArray:
        from numpy import array

        n = len(rhs)
        PETSc = self.PETSc

        matrix_csr = csr_matrix(matrix, dtype=float)

        # Create PETSc matrix
        # pylint: disable=no-member
        matrix_petsc = PETSc.Mat().createAIJ(
            size=matrix_csr.shape,
            csr=(matrix_csr.indptr, matrix_csr.indices, matrix_csr.data),
        )

        # Create PETSc vectors
        rhs_petsc = PETSc.Vec().createSeq(n)
        rhs_petsc.setValues(range(n), array(rhs, dtype=float))

        x_petsc = PETSc.Vec().createSeq(n)

        # Create solver
        ksp = PETSc.KSP().create()
        ksp.setType(self.ksp_type)
        ksp.setOperators(matrix_petsc)
        ksp.setTolerances(rtol=self.tol, max_it=self.maxiter)
        ksp.solve(rhs_petsc, x_petsc)

        return x_petsc.getArray()
