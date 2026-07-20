// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Provides the common application shell for the web client.
 *
 * This component owns only visual layout concerns such as page structure,
 * header area, and content container. It should not contain job lifecycle
 * or REST API logic.
 */

import type { PropsWithChildren } from "react";
import { Box, Container } from "@mui/material";

export function AppLayout({ children }: Readonly<PropsWithChildren>) {
  return (
    <Box
      component="main"
      sx={{
        minHeight: "100vh",
        bgcolor: "background.default",
        py: {
          xs: 2,
          sm: 4,
        },
      }}
    >
      <Container maxWidth="md">{children}</Container>
    </Box>
  );
}
