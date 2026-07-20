// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Displays input upload state and emits stop and retry events.
 */

import { useState } from "react";
import {
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Paper,
  Stack,
  Typography,
} from "@mui/material";

import type {
  InputFiles,
  InputSlotState,
  InputSlotStates,
  UploadManagerState,
} from "../WizardState";

export interface UploadStepProps {
  files: InputFiles;
  inputSlotStates: InputSlotStates;
  uploadManagerState: UploadManagerState;
  onStopUploading: () => void;
  onRetry: () => void;
}

interface UploadFileRowProps {
  label: string;
  file: File | null;
  state: InputSlotState;
  uploadManagerState: UploadManagerState;
}

function UploadStatus({
  state,
  uploadManagerState,
}: Readonly<{
  state: InputSlotState;
  uploadManagerState: UploadManagerState;
}>) {
  if (uploadManagerState === "STOPPED_BY_USER" && state === "UPLOADING") {
    return <Chip label="Stopped" size="small" variant="outlined" />;
  }

  switch (state) {
    case "UPLOADING":
      return (
        <Stack direction="row" alignItems="center" spacing={1}>
          <CircularProgress size={18} />

          <Typography variant="body2">Uploading...</Typography>
        </Stack>
      );

    case "UPLOADED":
      return (
        <Chip
          label="Uploaded"
          size="small"
          color="success"
          variant="outlined"
        />
      );

    case "FAILED":
      return (
        <Chip label="Failed" size="small" color="error" variant="outlined" />
      );

    case "FILE_SELECTED":
      return <Chip label="Ready" size="small" variant="outlined" />;

    case "EMPTY":
    default:
      return <Chip label="No file" size="small" variant="outlined" />;
  }
}

function UploadFileRow({
  label,
  file,
  state,
  uploadManagerState,
}: Readonly<UploadFileRowProps>) {
  return (
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
            sm: 90,
          },
          flexShrink: 0,
        }}
      >
        {label}
      </Typography>

      <Typography
        color={file ? "text.primary" : "text.secondary"}
        title={file?.name}
        sx={{
          flexGrow: 1,
          minWidth: 0,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {file?.name ?? "No file selected"}
      </Typography>

      <UploadStatus state={state} uploadManagerState={uploadManagerState} />
    </Stack>
  );
}

export function UploadStep({
  files,
  inputSlotStates,
  uploadManagerState,
  onStopUploading,
  onRetry,
}: Readonly<UploadStepProps>) {
  const [stopDialogOpen, setStopDialogOpen] = useState(false);

  const handleStopConfirmed = () => {
    setStopDialogOpen(false);
    onStopUploading();
  };

  return (
    <>
      <Paper
        component="section"
        variant="outlined"
        aria-labelledby="upload-step-title"
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
            <Typography id="upload-step-title" component="h2" variant="h6">
              Upload Job Inputs
            </Typography>

            <Typography variant="body2" color="text.secondary">
              Step 3 of 7
            </Typography>
          </Stack>

          <Stack spacing={3}>
            <UploadFileRow
              label="Matrix"
              file={files.matrix}
              state={inputSlotStates.matrix}
              uploadManagerState={uploadManagerState}
            />

            <UploadFileRow
              label="RHS"
              file={files.rhs}
              state={inputSlotStates.rhs}
              uploadManagerState={uploadManagerState}
            />
          </Stack>

          <Stack
            direction={{
              xs: "column",
              sm: "row",
            }}
            justifyContent="flex-end"
            spacing={1}
          >
            <Button
              type="button"
              variant="text"
              disabled={uploadManagerState !== "RUNNING"}
              onClick={() => setStopDialogOpen(true)}
            >
              Stop uploading
            </Button>

            <Button
              type="button"
              variant="outlined"
              disabled={uploadManagerState !== "FAILED"}
              onClick={onRetry}
            >
              Retry
            </Button>
          </Stack>
        </Stack>
      </Paper>

      <Dialog
        open={stopDialogOpen}
        onClose={() => setStopDialogOpen(false)}
        aria-labelledby="stop-upload-dialog-title"
      >
        <DialogTitle id="stop-upload-dialog-title">Stop uploading?</DialogTitle>

        <DialogContent>
          <DialogContentText>
            The current upload operation will stop. Files that have already been
            uploaded will remain uploaded. You can return to file selection
            using Previous.
          </DialogContentText>
        </DialogContent>

        <DialogActions>
          <Button type="button" onClick={() => setStopDialogOpen(false)}>
            Continue uploading
          </Button>

          <Button
            type="button"
            color="error"
            variant="contained"
            onClick={handleStopConfirmed}
          >
            Stop upload
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
