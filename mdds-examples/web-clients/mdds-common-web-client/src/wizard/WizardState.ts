// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * State model for the job wizard.
 *
 * Tracks the selected profile, upload session, draft job id, selected input
 * files, upload results, parameter values, submission state, runtime status,
 * output URLs, and user-visible errors.
 */
import type { WizardStep } from "./WizardStep";

export interface InputFiles {
  matrix: File | null;
  rhs: File | null;
}

export type UploadManagerState =
  | "IDLE"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "STOPPED_BY_USER";

export type InputSlotState =
  | "EMPTY"
  | "FILE_SELECTED"
  | "UPLOADING"
  | "UPLOADED"
  | "FAILED";

export type InputSlotStates = Record<keyof InputFiles, InputSlotState>;

export const SOLVING_METHODS = [
  "numpy_exact_solver",
  "numpy_lstsq_solver",
  "numpy_pinv_solver",
  "petsc_solver",
  "scipy_gmres_solver",
] as const;

export type SolvingMethod = (typeof SOLVING_METHODS)[number];

export type ParameterUpdateState =
  | "PENDING"
  | "UPDATING"
  | "UPDATED"
  | "FAILED";

export type SubmissionState =
  | "IDLE"
  | "CONFIRMING"
  | "REQUESTING"
  | "RECONCILING"
  | "UNCONFIRMED"
  | "SUBMITTED"
  | "FAILED";

export type PublicJobStatus =
  | "SUBMITTED"
  | "INPUTS_PREPARED"
  | "IN_PROGRESS"
  | "DONE"
  | "ERROR"
  | "CANCELLED";

export type MonitorState = "IDLE" | "RUNNING" | "COMPLETED" | "FAILED";

export type CancellationState =
  | "IDLE"
  | "CONFIRMING"
  | "REQUESTING"
  | "RECONCILING"
  | "ACCEPTED"
  | "FAILED";

export type DownloadState =
  | "IDLE"
  | "REQUESTING_URL"
  | "DOWNLOADING"
  | "DOWNLOADED"
  | "FAILED_DOWNLOAD"
  | "CANCELLED_DOWNLOAD";

export interface WizardState {
  activeStep: WizardStep;
  jobType: string;
  allRequiredFilesSelected: boolean;
  uploadManagerState: UploadManagerState;
  allRequiredFilesUploaded: boolean;
  parameterUpdateState: ParameterUpdateState;
  submissionState: SubmissionState;
  jobStatus: PublicJobStatus | null;
  monitorState: MonitorState;
  cancellationState: CancellationState;
  downloadState: DownloadState;
}
