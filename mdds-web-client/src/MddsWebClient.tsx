// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.
import React from "react";
import {
  AppBar,
  Toolbar,
  Typography,
  Container,
  Paper,
  Box,
  Button,
} from "@mui/material";
import { CloudUpload } from "@mui/icons-material";

export default function App() {
  return (
    <>
      <AppBar position="static" color="primary" enableColorOnDark>
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            MDDS Solver UI
          </Typography>
          <Button color="inherit" href="https://github.com/oleksiy-sayankin/mdds">
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
              Upload matrix of the coefficients, right hand side vector and chose
              solving method
            </Typography>
          </Box>

          <Box textAlign="center" mt={4}>
            <Button
              variant="contained"
              startIcon={<CloudUpload />}
              size="large"
              color="secondary"
              onClick={() => alert("Not implemented yet")}
            >
              Upload files
            </Button>
          </Box>
        </Paper>
      </Container>
    </>
  );
}
