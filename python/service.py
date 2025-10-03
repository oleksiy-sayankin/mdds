# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import grpc
import logging
from generated import solver_pb2, solver_pb2_grpc

# Mapping solver name -> Python class
SOLVER_MAP = {
    "numpy_exact_solver": "NumpyExactSolver",
    "numpy_lstsq_solver": "NumpyLstsqSolver",
    "numpy_pinv_solver": "NumpyPinvSolver",
    "petsc_solver": "PetscSolver",
    "scipy_gmres_solver": "ScipyGmresSolver",
}

logging.basicConfig(
    filename="SolverService.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class SolverService(solver_pb2_grpc.SolverServiceServicer):
    """
    gRPC Service that takes solve request (matrix of coefficients, right hand side and solve method),
    converts Protobuf data to Python data, solves system of linear algebraic equations and send the
    result as Protobuf data back to the caller.
    """

    def Solve(self, request, context):
        method = request.method
        if method not in SOLVER_MAP:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Unknown method: {method}")
            return solver_pb2.SolveResponse()

        # Load solver
        module = __import__(
            f"slae_solver.solvers.{method}", fromlist=[SOLVER_MAP[method]]
        )
        solver_class = getattr(module, SOLVER_MAP[method])
        solver = solver_class()

        # Convert data
        matrix = [list(row.values) for row in request.matrix]
        rhs = list(request.rhs)

        # Solve system of linear algebraic equations
        x = solver.solve(matrix, rhs)

        return solver_pb2.SolveResponse(solution=x)
