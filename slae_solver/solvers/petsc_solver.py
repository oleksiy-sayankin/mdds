# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""pets solver"""
import numpy as np

from slae_solver.solver_interface import LinearSolverInterface
from scipy.sparse import csr_matrix


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

    def solve(self, A, b):
        from numpy import array

        n = len(b)
        PETSc = self.PETSc

        # Convert numpy array to csr_matrix
        if isinstance(A, np.ndarray):
            A = csr_matrix(A, dtype=float)

        # Create PETSc matrix
        # pylint: disable=no-member
        mat = PETSc.Mat().createAIJ(size=A.shape, csr=(A.indptr, A.indices, A.data))

        # Create PETSc vectors
        rhs = PETSc.Vec().createSeq(n)
        rhs.setValues(range(n), array(b, dtype=float))

        x = PETSc.Vec().createSeq(n)

        # Create solver
        ksp = PETSc.KSP().create()
        ksp.setType(self.ksp_type)
        ksp.setOperators(mat)
        ksp.setTolerances(rtol=self.tol, max_it=self.maxiter)
        ksp.solve(rhs, x)

        return x.getArray()
