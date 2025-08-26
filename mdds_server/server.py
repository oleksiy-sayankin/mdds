# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Main entry point
"""
import logging
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import numpy as np
import io
import os

from common_logging.setup_logging import setup_logging


# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)

app = FastAPI()

CLIENT_DIR = os.path.join(os.path.dirname(__file__), "..", "mdds_client")
app.mount("/static", StaticFiles(directory=CLIENT_DIR, html=True), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(CLIENT_DIR, "index.html"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/solve")
async def solve_endpoint(
    matrix: UploadFile = File(...), rhs: UploadFile = File(...), method: str = Form(...)
):
    logging.info(f"Received request to solve SLAE with method: {method}")

    # Load files into numpy arrays
    A = np.loadtxt(matrix.file, delimiter=",")
    b = np.loadtxt(rhs.file, delimiter=",")
    logging.info(f"Loaded matrix A shape: {A.shape}, vector b shape: {b.shape}")

    # TODO:
    #  1. Connect RabbitMQ
    #  2. Prepare RabbitMQ message
    #  3. Send Task
    #  4. Wait for result

    # Prepare CSV output
    output = io.StringIO()
    # FIXME: np.savetxt(output, x, delimiter=",")
    output.seek(0)

    #  5. Close connection

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=solution.csv"},
    )
