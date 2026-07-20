// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Final editable screen before job submission.
 *
 * Shows the selected job type, uploaded inputs, and parameter values so the
 * user can review the immutable job request before submitting it.
 */

import {
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Button,
  Chip,
  Divider,
  Paper,
  Stack,
  Typography,
} from "@mui/material";

import type { SolvingMethod, SubmissionState } from "../WizardState";

export interface ReviewStepProps {
  jobType: string;
  matrixFile: File;
  rhsFile: File;
  solvingMethod: SolvingMethod;
  submissionState: SubmissionState;
  onDismissConfirmation: () => void;
  onConfirmSubmission: () => void;
}

interface SummaryValueProps {
  children: React.ReactNode;
}

function SummaryValue({ children }: Readonly<SummaryValueProps>) {
  return (
    <Typography
      variant="body1"
      sx={{
        overflowWrap: "anywhere",
      }}
    >
      {children}
    </Typography>
  );
}

export function ReviewStep({
  jobType,
  matrixFile,
  rhsFile,
  solvingMethod,
  submissionState,
  onDismissConfirmation,
  onConfirmSubmission,
}: Readonly<ReviewStepProps>) {
  return (
    <>
      <Paper
        component="section"
        variant="outlined"
        aria-labelledby="review-step-title"
        sx={{
          minHeight: 360,
          p: {
            xs: 2,
            sm: 3,
          },
        }}
      >
        <Stack spacing={3}>
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
            <Typography id="review-step-title" component="h2" variant="h6">
              Review Job Summary
            </Typography>

            <Typography variant="body2" color="text.secondary">
              Step 5 of 7
            </Typography>
          </Stack>

          <Divider />

          <Stack spacing={1}>
            <Typography
              component="h3"
              variant="subtitle2"
              color="text.secondary"
            >
              Job type
            </Typography>

            <SummaryValue>{jobType}</SummaryValue>
          </Stack>

          <Divider />

          <Stack spacing={2}>
            <Typography
              component="h3"
              variant="subtitle2"
              color="text.secondary"
            >
              Inputs
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
              spacing={2}
            >
              <Chip
                label="Uploaded"
                size="small"
                color="success"
                variant="outlined"
              />

              <Typography
                fontWeight={500}
                sx={{
                  width: {
                    xs: "auto",
                    sm: 80,
                  },
                }}
              >
                Matrix
              </Typography>

              <SummaryValue>{matrixFile.name}</SummaryValue>
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
              <Chip
                label="Uploaded"
                size="small"
                color="success"
                variant="outlined"
              />

              <Typography
                fontWeight={500}
                sx={{
                  width: {
                    xs: "auto",
                    sm: 80,
                  },
                }}
              >
                RHS
              </Typography>

              <SummaryValue>{rhsFile.name}</SummaryValue>
            </Stack>
          </Stack>

          <Divider />

          <Stack spacing={1}>
            <Typography
              component="h3"
              variant="subtitle2"
              color="text.secondary"
            >
              Parameters
            </Typography>

            <Stack
              direction={{
                xs: "column",
                sm: "row",
              }}
              spacing={2}
            >
              <Typography
                fontWeight={500}
                sx={{
                  width: {
                    xs: "auto",
                    sm: 150,
                  },
                }}
              >
                Solving method
              </Typography>

              <SummaryValue>{solvingMethod}</SummaryValue>
            </Stack>
          </Stack>
        </Stack>
      </Paper>

      <Dialog
        open={submissionState === "CONFIRMING"}
        onClose={onDismissConfirmation}
        aria-labelledby="submit-job-dialog-title"
      >
        <DialogTitle id="submit-job-dialog-title">Submit this job?</DialogTitle>

        <DialogContent>
          <DialogContentText>
            After submission, the input files and job parameters can no longer
            be changed.
          </DialogContentText>
        </DialogContent>

        <DialogActions>
          <Button type="button" onClick={onDismissConfirmation}>
            Continue editing
          </Button>

          <Button
            type="button"
            variant="contained"
            onClick={onConfirmSubmission}
          >
            Submit job
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
