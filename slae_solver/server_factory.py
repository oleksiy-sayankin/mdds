# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Main entry point
"""

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from slae_solver.solver_interface import LinearSolverInterface
import pandas as pd
import tempfile
import io


def create_app(solver: LinearSolverInterface) -> FastAPI:
    """
    Create FastAPI application that uses provided solver.
    This app is agnostic of the solver implementation details.
    """
    app = FastAPI()

    @app.get("/")
    def index():
        try:
            with open("slae_solver/client.html", "r", encoding="utf-8") as f:
                return HTMLResponse(f.read())
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to load client.html: {e}"
            )

    @app.post("/solve")
    async def solve_endpoint(
        matrix: UploadFile = File(...), vector: UploadFile = File(...)
    ):
        try:
            a = pd.read_csv(io.BytesIO(await matrix.read()), header=None).values
            b = pd.read_csv(io.BytesIO(await vector.read()), header=None).values
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse CSV: {e}")

        try:
            x = solver.solve(a, b)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Solver error: {e}")

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        pd.DataFrame(x).to_csv(tmp.name, index=False, header=False)
        tmp.close()
        return FileResponse(tmp.name, filename="solution.csv")

    return app
