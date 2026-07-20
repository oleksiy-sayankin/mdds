// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Bootstraps the React application and mounts the MDDS web client
 * into the root HTML element.
 */

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { CssBaseline, ThemeProvider, createTheme } from "@mui/material";
import MddsWebClient from "./MddsWebClient";

const theme = createTheme();

// Global styles from MUI
import "@fontsource/roboto/300.css";
import "@fontsource/roboto/400.css";
import "@fontsource/roboto/500.css";
import "@fontsource/roboto/700.css";

const rootElement = document.getElementById("root");

if (!rootElement) {
  throw new Error("Root element was not found.");
}

createRoot(rootElement).render(
  <StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <MddsWebClient />
    </ThemeProvider>
  </StrictMode>,
);
