# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
"""
Entry point for running server with chosen solver
"""
from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver
from slae_solver.server_factory import create_app
import uvicorn

solver = NumpyExactSolver()
app = create_app(solver)

if __name__ == "__main__":
    uvicorn.run("slae_solver.run:app", host="127.0.0.1", port=8000, reload=True)
