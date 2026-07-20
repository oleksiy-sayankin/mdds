// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Navigation and submission rules for the wizard.
 *
 * This module decides when the user can move to the next step,
 * go back, submit a job, or open job outputs based on the
 * current wizard state.
 */

import type {
  SubmissionState,
  UploadManagerState,
  WizardState,
} from "./WizardState";

import { WizardStep } from "./WizardStep";

export function isSubmissionInProgress(
  submissionState: SubmissionState,
): boolean {
  return (
    submissionState === "CONFIRMING" ||
    submissionState === "REQUESTING" ||
    submissionState === "RECONCILING"
  );
}

export function canGoBackFromUpload(
  uploadManagerState: UploadManagerState,
): boolean {
  return (
    uploadManagerState === "IDLE" ||
    uploadManagerState === "COMPLETED" ||
    uploadManagerState === "FAILED" ||
    uploadManagerState === "STOPPED_BY_USER"
  );
}

export function getPreviousStep(state: WizardState): WizardStep | null {
  switch (state.activeStep) {
    case WizardStep.Inputs:
      return WizardStep.JobType;

    case WizardStep.Upload:
      return canGoBackFromUpload(state.uploadManagerState)
        ? WizardStep.Inputs
        : null;

    case WizardStep.Parameters:
      return state.parameterUpdateState !== "UPDATING"
        ? WizardStep.Upload
        : null;

    case WizardStep.Review:
      return state.submissionState === "IDLE" ||
        state.submissionState === "FAILED"
        ? WizardStep.Parameters
        : null;

    default:
      return null;
  }
}

export function isNextDisabled(state: WizardState): boolean {
  switch (state.activeStep) {
    case WizardStep.JobType:
      return !state.jobType;

    case WizardStep.Inputs:
      return !state.allRequiredFilesSelected;

    case WizardStep.Upload:
      return (
        state.uploadManagerState !== "COMPLETED" ||
        !state.allRequiredFilesUploaded
      );

    case WizardStep.Parameters:
      return state.parameterUpdateState !== "UPDATED";

    case WizardStep.Review:
      return (
        isSubmissionInProgress(state.submissionState) ||
        state.submissionState === "SUBMITTED"
      );

    case WizardStep.Monitor:
      return state.jobStatus !== "DONE" && state.jobStatus !== "CANCELLED";

    case WizardStep.Outputs:
      return false;
  }
}

export function getNextLabel(state: WizardState): string {
  switch (state.activeStep) {
    case WizardStep.Review:
      return state.submissionState === "UNCONFIRMED"
        ? "Check submission status"
        : "Submit job";

    case WizardStep.Outputs:
      return "Start new job";

    case WizardStep.Monitor:
      return state.jobStatus === "CANCELLED"
        ? "Start new job"
        : "View outputs >";

    default:
      return "Next >";
  }
}
