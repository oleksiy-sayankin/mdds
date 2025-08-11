# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Main entry point
"""
import logging
from pathlib import Path
from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
import numpy as np
import io

# Import solvers
from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver
from slae_solver.solvers.numpy_lstsq_solver import NumpyLstsqSolver
from slae_solver.solvers.numpy_pinv_solver import NumpyPinvSolver
from slae_solver.solvers.petsc_solver import PetscSolver
from slae_solver.solvers.scipy_gmres_solver import ScipyGmresSolver

# Create logs directory
Path("logs").mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/server.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

app = FastAPI()

# Solver mapping
SOLVER_MAPPING = {
    "numpy_exact_solver": NumpyExactSolver,
    "numpy_lstsq_solver": NumpyLstsqSolver,
    "numpy_pinv_solver": NumpyPinvSolver,
    "petsc_solver": PetscSolver,
    "scipy_gmres_solver": ScipyGmresSolver,
}


@app.get("/")
def index():
    try:
        with open("slae_solver/client.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load client.html: {e}")


@app.post("/solve")
async def solve_endpoint(
    matrix: UploadFile = File(...), rhs: UploadFile = File(...), method: str = Form(...)
):
    logging.info(f"Received request to solve SLAE with method: {method}")

    if method not in SOLVER_MAPPING:
        logging.error(f"Unknown solver method requested: {method}")
        return {"error": f"Unknown solver method: {method}"}

    # Load files into numpy arrays
    A = np.loadtxt(matrix.file, delimiter=",")
    b = np.loadtxt(rhs.file, delimiter=",")
    logging.info(f"Loaded matrix A shape: {A.shape}, vector b shape: {b.shape}")

    # Instantiate solver
    solver_class = SOLVER_MAPPING[method]
    solver = solver_class()

    # Solve SLAE
    try:
        x = solver.solve(A, b)
        logging.info(f"Solved SLAE successfully using {method}")
    except Exception as e:
        logging.exception("Error while solving SLAE")
        return {"error": str(e)}

    # Prepare CSV output
    output = io.StringIO()
    np.savetxt(output, x, delimiter=",")
    output.seek(0)

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=solution.csv"},
    )
