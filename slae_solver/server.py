from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
import numpy as np
import pandas as pd
import tempfile

app = FastAPI()

@app.get("/")
def read_root():
    """Create client HTML-page."""
    with open("client.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.post("/solve")
async def solve_slae(matrix: UploadFile = File(...), vector: UploadFile = File(...)):
    """Read CSV-files, solve SLAE and send CSV-result."""
    # Read CSV files into arrays in NumPy
    A = np.loadtxt(matrix.file, delimiter=",")
    b = np.loadtxt(vector.file, delimiter=",")

    # Solving SLAE
    try:
        x = np.linalg.solve(A, b)
    except np.linalg.LinAlgError as e:
        return {"error": str(e)}

    # Save solution to temporary CSV file
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    pd.DataFrame(x).to_csv(tmp.name, index=False, header=False)
    tmp.close()

    return FileResponse(tmp.name, filename="solution.csv")
