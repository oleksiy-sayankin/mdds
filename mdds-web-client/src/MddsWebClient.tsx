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
  const [taskId, setTaskId] = useState<string | null>(null);
  const [solutionBlob, setSolutionBlob] = useState<Blob | null>(null);
  const [isDownloadingAllowed, setIsDownloadingAllowed] = useState(false);

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
      console.log("Matrix CSV selected:", file);
    }
  };

  const handleRhsFileSelected = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (file) {
      setRhsFile(file);
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

  const handleSolveClick = async () => {
    if (!matrixFile || !rhsFile) {
      alert("Please select both matrix and RHS files.");
      return;
    }

    setIsSolving(true);
    setProgress(0);
    setIsDownloadingAllowed(false);
    setTaskId(null);
    setSolutionBlob(null);

    try {
      const matrixText = await matrixFile.text();
      const rhsText = await rhsFile.text();

      const formData = new FormData();
      formData.append("matrix", matrixText);
      formData.append("rhs", rhsText);
      formData.append("slaeSolvingMethod", solver);
      formData.append("dataSourceType", "http_request");

      const response = await fetch(`/solve`, {
        method: "POST",
        body: formData,
      });

      const resultJson = await response.json();
      console.log("Solve response:", resultJson);

      const returnedTaskId = resultJson.id;
      setTaskId(returnedTaskId);
      pollForResult(returnedTaskId);
    } catch (error) {
      console.error(error);
      alert("Error sending files");
    }
  };

  const pollForResult = async (taskId: string) => {
    const timeoutMs = 30000;
    const pollingInterval = 1000;
    const start = Date.now();

    const checkStatus = async () => {
      if (Date.now() - start > timeoutMs) {
        alert("Timeout waiting for solver result.");
        return;
      }

      try {
        const resp = await fetch(`/result/${taskId}`);
        const json = await resp.json();

        console.log("Polling result:", json);

        if (json.taskStatus === "DONE") {
          setProgress(100);
          setIsDownloadingAllowed(true);

          // Create Blob with solution
          const solutionText = JSON.stringify(json.solution, null, 2);
          setSolutionBlob(
            new Blob([solutionText], { type: "application/json" }),
          );
          return;
        }

        if (json.taskStatus === "ERROR") {
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
