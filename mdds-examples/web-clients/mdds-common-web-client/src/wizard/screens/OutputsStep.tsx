// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Displays the SLAE solution download state and emits download actions.
 */

import {
  Button,
  Chip,
  CircularProgress,
  Paper,
  Stack,
  Typography,
} from "@mui/material";

import type { DownloadState } from "../WizardState";

export interface OutputsStepProps {
  downloadState: DownloadState;
  onDownload: () => void;
  onCancelDownload: () => void;
}

function DownloadStatus({ state }: Readonly<{ state: DownloadState }>) {
  switch (state) {
    case "REQUESTING_URL":
      return (
        <Stack direction="row" alignItems="center" spacing={1}>
          <CircularProgress size={18} />

          <Typography variant="body2">Preparing...</Typography>
        </Stack>
      );

    case "DOWNLOADING":
      return (
        <Stack direction="row" alignItems="center" spacing={1}>
          <CircularProgress size={18} />

          <Typography variant="body2">Downloading...</Typography>
        </Stack>
      );

    case "DOWNLOADED":
      return (
        <Chip
          label="Downloaded"
          size="small"
          color="success"
          variant="outlined"
        />
      );

    case "FAILED_DOWNLOAD":
      return (
        <Chip
          label="Download failed"
          size="small"
          color="error"
          variant="outlined"
        />
      );

    case "CANCELLED_DOWNLOAD":
      return (
        <Chip
          label="Cancelled"
          size="small"
          color="warning"
          variant="outlined"
        />
      );

    case "IDLE":
    default:
      return <Chip label="Available" size="small" variant="outlined" />;
  }
}

export function OutputsStep({
  downloadState,
  onDownload,
  onCancelDownload,
}: Readonly<OutputsStepProps>) {
  const downloadInProgress =
    downloadState === "REQUESTING_URL" || downloadState === "DOWNLOADING";

  const downloadButtonLabel =
    downloadState === "DOWNLOADED" ||
    downloadState === "FAILED_DOWNLOAD" ||
    downloadState === "CANCELLED_DOWNLOAD"
      ? "Download again"
      : "Download";

  return (
    <Paper
      component="section"
      variant="outlined"
      aria-labelledby="outputs-step-title"
      sx={{
        minHeight: 260,
        p: {
          xs: 2,
          sm: 3,
        },
      }}
    >
      <Stack spacing={4}>
        <Stack
          direction={{
            xs: "column",
            sm: "row",
          }}
          justifyContent="space-between"
          alignItems={{
            xs: "flex-start",
            sm: "center",
          }}
          spacing={1}
        >
          <Typography id="outputs-step-title" component="h2" variant="h6">
            Download Job Outputs
          </Typography>

          <Typography variant="body2" color="text.secondary">
            Step 7 of 7
          </Typography>
        </Stack>

        <Stack
          direction={{
            xs: "column",
            sm: "row",
          }}
          alignItems={{
            xs: "flex-start",
            sm: "center",
          }}
          spacing={2}
        >
          <Typography
            fontWeight={500}
            sx={{
              width: {
                xs: "auto",
                sm: 120,
              },
              flexShrink: 0,
            }}
          >
            Solution
          </Typography>

          <Typography
            color="text.secondary"
            sx={{
              width: {
                xs: "auto",
                sm: 80,
              },
              flexShrink: 0,
            }}
          >
            CSV
          </Typography>

          <Stack
            direction={{
              xs: "column",
              sm: "row",
            }}
            alignItems={{
              xs: "flex-start",
              sm: "center",
            }}
            justifyContent="space-between"
            spacing={2}
            sx={{
              flexGrow: 1,
              width: {
                xs: "100%",
                sm: "auto",
              },
            }}
          >
            <DownloadStatus state={downloadState} />

            <Button
              type="button"
              variant={downloadInProgress ? "outlined" : "contained"}
              color={downloadInProgress ? "error" : "primary"}
              onClick={downloadInProgress ? onCancelDownload : onDownload}
            >
              {downloadInProgress ? "Cancel download" : downloadButtonLabel}
            </Button>
          </Stack>
        </Stack>
      </Stack>
    </Paper>
  );
}
