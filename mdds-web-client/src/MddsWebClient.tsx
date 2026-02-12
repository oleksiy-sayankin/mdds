// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.
import React, { useState, useRef } from "react";
import {
  AppBar,
  Toolbar,
  Typography,
  Container,
  Paper,
  Box,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  LinearProgress,
} from "@mui/material";
import { CloudUpload } from "@mui/icons-material";

export default function App() {
  const [solver, setSolver] = useState("numpy_exact_solver");

  const [matrixFile, setMatrixFile] = useState<File | null>(null);
  const [rhsFile, setRhsFile] = useState<File | null>(null);

  const [progress, setProgress] = useState(0);
  const [isSolving, setIsSolving] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [solutionBlob, setSolutionBlob] = useState<Blob | null>(null);
  const [isDownloadingAllowed, setIsDownloadingAllowed] = useState(false);

  const [isMatrixSelected, setIsMatrixSelected] = useState(false);
  const [isRhsSelected, setIsRhsSelected] = useState(false);

  const matrixFileInputRef = useRef<HTMLInputElement | null>(null);
  const rhsFileInputRef = useRef<HTMLInputElement | null>(null);

  const handleSolverChange = (event: any) => {
    setSolver(event.target.value);
  };

  const handleMatrixUploadClick = () => {
    matrixFileInputRef.current?.click();
  };

  const handleRhsUploadClick = () => {
    rhsFileInputRef.current?.click();
  };

  const handleMatrixFileSelected = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (file) {
      setMatrixFile(file);
      setIsMatrixSelected(true);
      console.log("Matrix CSV selected:", file);
    }
  };

  const handleRhsFileSelected = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (file) {
      setRhsFile(file);
      setIsRhsSelected(true);
      console.log("Right hand side CSV selected:", file);
    }
  };

  const solvers = [
    { value: "numpy_exact_solver", label: "NumPy Exact Solver" },
    { value: "numpy_lstsq_solver", label: "NumPy Least Squares" },
    { value: "numpy_pinv_solver", label: "NumPy Pseudo-Inverse" },
    { value: "petsc_solver", label: "PETSc Solver" },
    { value: "scipy_gmres_solver", label: "SciPy GMRES Solver" },
  ];

  const parseMatrixCsv = (text: string): number[][] => {
    const rows = text
      .trim()
      .split(/\r?\n/)
      .map((r) => r.trim())
      .filter((r) => r.length > 0);

    if (rows.length === 0) {
      throw new Error("Matrix file is empty");
    }

    const matrix: number[][] = rows.map((row, rowIndex) => {
      const items = row
        .split(",")
        .map((v) => v.trim())
        .filter((v) => v.length > 0);

      if (items.length === 0) {
        throw new Error(`Empty row ${rowIndex + 1} in matrix`);
      }

      const nums = items.map((value, colIndex) => {
        const n = Number(value);
        if (Number.isNaN(n)) {
          throw new Error(
            `Invalid number "${value}" in matrix at row ${rowIndex + 1}, column ${colIndex + 1}`,
          );
        }
        return n;
      });

      return nums;
    });

    // Check if matrix rectangular
    const cols = matrix[0].length;
    if (!matrix.every((row) => row.length === cols)) {
      throw new Error(
        "Matrix is not rectangular (rows have different lengths)",
      );
    }

    return matrix;
  };

  const parseVectorCsv = (text: string): number[] => {
    // Support both columns "1.3\n2.2\n3.7" and rows "1.3,2.2,3.7"
    const tokens = text
      .trim()
      .split(/[\s,]+/) // spaces, new line symbols, commas
      .map((t) => t.trim())
      .filter((t) => t.length > 0);

    if (tokens.length === 0) {
      throw new Error("RHS file is empty");
    }

    const vector = tokens.map((value, index) => {
      const n = Number(value);
      if (Number.isNaN(n)) {
        throw new Error(
          `Invalid number "${value}" in RHS at position ${index + 1}`,
        );
      }
      return n;
    });

    return vector;
  };

  const handleSolveClick = async () => {
    if (!matrixFile || !rhsFile) {
      alert("Please select both matrix and RHS files.");
      return;
    }

    setIsSolving(true);
    setProgress(0);
    setIsDownloadingAllowed(false);
    setJobId(null);
    setSolutionBlob(null);

    try {
      const matrixText = await matrixFile.text();
      const rhsText = await rhsFile.text();
      const matrix = parseMatrixCsv(matrixText); // number[][]
      const rhs = parseVectorCsv(rhsText); // number[]

      // Create JSON-object
      const requestBody = {
        dataSourceType: "http_request",
        slaeSolvingMethod: solver,
        params: {
          matrix,
          rhs,
        },
      };

      const response = await fetch("/solve", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        console.error("Solve request failed with status:", response.status);
        alert("Error sending request to server");
        setIsSolving(false);
        return;
      }

      const resultJson = await response.json();
      console.log("Solve response:", resultJson);

      const returnedJobId = resultJson.id;
      setJobId(returnedJobId);
      pollForResult(returnedJobId);
    } catch (error) {
      console.error(error);
      alert("Error sending files");
      setIsSolving(false);
    }
  };

  const pollForResult = async (jobId: string) => {
    const timeoutMs = 30000;
    const pollingInterval = 1000;
    const start = Date.now();

    const checkStatus = async () => {
      if (Date.now() - start > timeoutMs) {
        alert("Timeout waiting for solver result.");
        return;
      }

      try {
        const resp = await fetch(`/result/${jobId}`);
        const json = await resp.json();

        console.log("Polling result:", json);

        if (json.jobStatus === "DONE") {
          setProgress(100);
          setIsDownloadingAllowed(true);

          // Create Blob with solution
          const solutionText = JSON.stringify(json.solution, null, 2);
          setSolutionBlob(
            new Blob([solutionText], { type: "application/json" }),
          );
          return;
        }

        if (json.jobStatus === "ERROR") {
          alert("Error: " + json.errorMessage);
          return;
        }
        setProgress(json.progress ?? 0);
        setTimeout(checkStatus, pollingInterval);
      } catch (err) {
        console.error("Polling error", err);
        setTimeout(checkStatus, pollingInterval);
      }
    };

    checkStatus();
  };

  const handleDownload = () => {
    if (!solutionBlob) return;

    const url = URL.createObjectURL(solutionBlob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "solution.json";
    link.click();
    URL.revokeObjectURL(url);
  };

  return (
    <>
      <AppBar position="static" color="primary" enableColorOnDark>
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            MDDS Solver UI
          </Typography>
          <Button
            color="inherit"
            href="https://github.com/oleksiy-sayankin/mdds"
          >
            GitHub
          </Button>
        </Toolbar>
      </AppBar>

      <Container maxWidth="md" sx={{ mt: 6 }}>
        <Paper elevation={4} sx={{ p: 4, borderRadius: 3 }}>
          <Box textAlign="center" mb={3}>
            <Typography variant="h5" gutterBottom>
              Solving of System of Linear Algebraic Equations
            </Typography>
            <Typography variant="body1" color="text.secondary">
              Upload matrix of the coefficients, right hand side vector and
              chose solving method
            </Typography>
          </Box>

          <Box textAlign="center" mt={4}>
            <Button
              variant="contained"
              startIcon={<CloudUpload />}
              size="large"
              color="secondary"
              onClick={handleMatrixUploadClick}
            >
              Upload matrix coefficients
            </Button>
            <input
              type="file"
              accept=".csv"
              ref={matrixFileInputRef}
              style={{ display: "none" }}
              onChange={handleMatrixFileSelected}
            />
            <Box sx={{ minHeight: 24, mt: 1 }}>
              {matrixFile && (
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Selected matrix file: {matrixFile.name}
                </Typography>
              )}
            </Box>
            <br />
            <br />
            <Button
              variant="contained"
              startIcon={<CloudUpload />}
              size="large"
              color="secondary"
              onClick={handleRhsUploadClick}
            >
              Upload right hand side vector
            </Button>
            <input
              type="file"
              accept=".csv"
              ref={rhsFileInputRef}
              style={{ display: "none" }}
              onChange={handleRhsFileSelected}
            />
            <Box sx={{ minHeight: 24, mt: 1 }}>
              {rhsFile && (
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Selected right hand side file: {rhsFile.name}
                </Typography>
              )}
            </Box>
            <br /> <br />{" "}
            <FormControl
              fullWidth
              variant="outlined"
              sx={{ maxWidth: 400, mx: "auto" }}
            >
              {" "}
              <InputLabel id="solver-label">Solving method</InputLabel>{" "}
              <Select
                labelId="solver-label"
                value={solver}
                label="Solving method"
                onChange={handleSolverChange}
              >
                {" "}
                {solvers.map((s) => (
                  <MenuItem key={s.value} value={s.value}>
                    {" "}
                    {s.label}{" "}
                  </MenuItem>
                ))}{" "}
              </Select>{" "}
            </FormControl>
            <Box textAlign="center" mt={4}>
              <Button
                variant="contained"
                color="primary"
                size="large"
                disabled={!isMatrixSelected || !isRhsSelected}
                onClick={handleSolveClick}
              >
                Solve
              </Button>
            </Box>
            {isSolving && (
              <Box sx={{ mt: 4 }}>
                <LinearProgress variant="determinate" value={progress} />
                <Typography variant="body2" sx={{ mt: 1 }}>
                  {progress}% completed
                </Typography>
              </Box>
            )}
            <Box textAlign="center" mt={4}>
              <Button
                variant="contained"
                color="success"
                size="large"
                disabled={!isDownloadingAllowed}
                onClick={handleDownload}
              >
                Download solution
              </Button>
            </Box>
          </Box>
        </Paper>
      </Container>
    </>
  );
}
