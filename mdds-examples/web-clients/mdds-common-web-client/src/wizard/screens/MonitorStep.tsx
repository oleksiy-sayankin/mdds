// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Displays the last confirmed job status, progress, and message.
 *
 * Emits cancellation and monitoring recovery events.
 */

import type { ReactNode } from "react";

import {
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  LinearProgress,
  Paper,
  Stack,
  Typography,
} from "@mui/material";

import type {
  CancellationState,
  MonitorState,
  PublicJobStatus,
} from "../WizardState";

export interface MonitorStepProps {
  jobStatus: PublicJobStatus;
  jobProgress: number;
  jobMessage: string;
  monitorState: MonitorState;
  cancellationState: CancellationState;
  onRequestCancellation: () => void;
  onDismissCancellation: () => void;
  onConfirmCancellation: () => void;
  onRetryMonitoring: () => void;
  onStartNewJob: () => void;
}

function JobStatusChip({
  status,
}: Readonly<{
  status: PublicJobStatus;
}>) {
  switch (status) {
    case "DONE":
      return <Chip label={status} size="small" color="success" />;

    case "ERROR":
      return <Chip label={status} size="small" color="error" />;

    case "CANCELLED":
      return <Chip label={status} size="small" color="warning" />;

    case "IN_PROGRESS":
      return <Chip label={status} size="small" color="primary" />;

    case "INPUTS_PREPARED":
      return (
        <Chip label={status} size="small" color="info" variant="outlined" />
      );

    case "SUBMITTED":
      return <Chip label={status} size="small" variant="outlined" />;
  }
}

export function MonitorStep({
  jobStatus,
  jobProgress,
  jobMessage,
  monitorState,
  cancellationState,
  onRequestCancellation,
  onDismissCancellation,
  onConfirmCancellation,
  onRetryMonitoring,
  onStartNewJob,
}: Readonly<MonitorStepProps>) {
  const cancellationAvailable =
    jobStatus === "IN_PROGRESS" &&
    (cancellationState === "IDLE" || cancellationState === "FAILED");

  const jobFailed = jobStatus === "ERROR";

  const cancellationInProgress =
    cancellationState === "REQUESTING" || cancellationState === "RECONCILING";

  const monitoringFailed = monitorState === "FAILED";

  let monitorActions: ReactNode;

  if (jobFailed) {
    monitorActions = (
      <Button type="button" variant="contained" onClick={onStartNewJob}>
        Start new job
      </Button>
    );
  } else if (monitoringFailed) {
    monitorActions = (
      <Stack
        direction={{
          xs: "column",
          sm: "row",
        }}
        spacing={2}
      >
        <Button type="button" variant="outlined" onClick={onStartNewJob}>
          Start new job
        </Button>

        <Button type="button" variant="contained" onClick={onRetryMonitoring}>
          Retry monitoring
        </Button>
      </Stack>
    );
  } else {
    monitorActions = (
      <Button
        type="button"
        color="error"
        variant="outlined"
        disabled={
          !cancellationAvailable ||
          cancellationInProgress ||
          monitorState !== "RUNNING"
        }
        onClick={onRequestCancellation}
      >
        Cancel job
      </Button>
    );
  }

  return (
    <>
      <Paper
        component="section"
        variant="outlined"
        aria-labelledby="run-step-title"
        sx={{
          minHeight: 340,
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
            <Typography id="run-step-title" component="h2" variant="h6">
              Monitor Job Progress
            </Typography>

            <Typography variant="body2" color="text.secondary">
              Step 6 of 7
            </Typography>
          </Stack>

          <Stack direction="row" alignItems="center" spacing={2}>
            <Typography fontWeight={500} sx={{ minWidth: 140 }}>
              Status
            </Typography>

            <JobStatusChip status={jobStatus} />
          </Stack>

          <Stack spacing={1}>
            <Stack direction="row" justifyContent="space-between" spacing={2}>
              <Typography fontWeight={500}>Estimated progress</Typography>

              <Typography variant="body2" color="text.secondary">
                {jobProgress}%
              </Typography>
            </Stack>

            <LinearProgress
              variant="determinate"
              value={jobProgress}
              aria-label={`Estimated job progress: ${jobProgress}%`}
              sx={{
                height: 10,
                borderRadius: 1,
              }}
            />
          </Stack>

          <Stack
            direction={{
              xs: "column",
              sm: "row",
            }}
            spacing={2}
          >
            <Typography fontWeight={500} sx={{ minWidth: 140 }}>
              Message
            </Typography>

            <Typography
              color="text.secondary"
              sx={{
                overflowWrap: "anywhere",
              }}
            >
              {jobMessage}
            </Typography>
          </Stack>

          <Box
            sx={{
              display: "flex",
              justifyContent: "flex-end",
            }}
          >
            {monitorActions}
          </Box>
        </Stack>
      </Paper>

      <Dialog
        open={cancellationState === "CONFIRMING"}
        onClose={onDismissCancellation}
        aria-labelledby="cancel-job-dialog-title"
      >
        <DialogTitle id="cancel-job-dialog-title">
          Cancel this running job?
        </DialogTitle>

        <DialogContent>
          <DialogContentText>
            The worker will be asked to stop processing the current job.
          </DialogContentText>
        </DialogContent>

        <DialogActions>
          <Button type="button" onClick={onDismissCancellation}>
            Keep running
          </Button>

          <Button
            type="button"
            color="error"
            variant="contained"
            onClick={onConfirmCancellation}
          >
            Cancel job
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
