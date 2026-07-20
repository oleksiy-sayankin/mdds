// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Owns wizard-scoped context and cross-step navigation.
 *
 * Delegates asynchronous operations to focused workflow hooks and passes
 * state and events to presentational step components.
 */

import { useState } from "react";
import { Box, Divider, Paper, Typography } from "@mui/material";

import type { MddsApiClient } from "@/api/MddsApiClient";
import type { ArtifactTransferClient } from "@/artifacts/ArtifactTransferClient";
import { WizardNavigation } from "@/components/WizardNavigation";
import { WizardStepper } from "@/components/WizardStepper";
import { InputsStep } from "./screens/InputsStep";
import { JobTypeStep } from "./screens/JobTypeStep";
import { ParametersStep } from "./screens/ParametersStep";
import { UploadStep } from "./screens/UploadStep";
import { ReviewStep } from "./screens/ReviewStep";
import { MonitorStep } from "./screens/MonitorStep";
import { OutputsStep } from "./screens/OutputsStep";

import type {
  DownloadState,
  InputFiles,
  ParameterUpdateState,
  SolvingMethod,
  SubmissionState,
  UploadManagerState,
  WizardState,
} from "./WizardState";

import {
  type ExecutionWorkflow,
  type OutputWorkflow,
  type ParameterWorkflow,
  type UploadWorkflow,
  useExecutionWorkflow,
  useOutputWorkflow,
  useParameterWorkflow,
  useUploadWorkflow,
} from "./useJobWizardWorkflows";
import {
  getNextLabel,
  getPreviousStep,
  isNextDisabled,
} from "@/wizard/WizardValidation";
import { WizardStep } from "./WizardStep";

export interface JobWizardProps {
  apiClient: MddsApiClient;
  artifactTransferClient: ArtifactTransferClient;
  onJobTypeSelected?: (jobType: string) => void;
  onInputsSelected?: (files: { matrix: File; rhs: File }) => void;
  onInputsUploaded?: () => void;
  onParametersSynchronized?: (solvingMethod: SolvingMethod) => void;
  onJobSubmitted?: () => void;
  onJobCancellationAccepted?: () => void;
  onOutputsRequested?: () => void;
  onOutputDownloaded?: (fileName: string) => void;
  onNewJobStarted?: () => void;
}

interface WizardFeedback {
  message: string | null;
  isError: boolean;
}

const EMPTY_FEEDBACK: WizardFeedback = {
  message: null,
  isError: false,
};

const UPLOAD_FEEDBACK: Record<UploadManagerState, string | null> = {
  IDLE: null,
  RUNNING: "Uploading input files...",
  COMPLETED: "All input files were uploaded successfully.",
  FAILED: "One or more input files could not be uploaded.",
  STOPPED_BY_USER: "Uploading was stopped by the user.",
};

const PARAMETER_FEEDBACK: Record<ParameterUpdateState, string | null> = {
  PENDING: "Job parameters have not been synchronized.",
  UPDATING: "Updating job parameters...",
  UPDATED: "Job parameters were synchronized successfully.",
  FAILED: "Network error while updating job parameters.",
};

const SUBMISSION_FEEDBACK: Record<SubmissionState, string | null> = {
  IDLE: null,
  CONFIRMING: null,
  REQUESTING: "Submitting the job...",
  RECONCILING: "Checking whether the job was submitted...",
  UNCONFIRMED:
    "The submission result is still unknown. Check the current job status before submitting again.",
  SUBMITTED: "The job was submitted successfully.",
  FAILED: "The job was not submitted.",
};

const DOWNLOAD_FEEDBACK: Record<DownloadState, string | null> = {
  IDLE: null,
  REQUESTING_URL: "Preparing the output download...",
  DOWNLOADING: "Downloading solution.csv...",
  DOWNLOADED: "solution.csv was downloaded successfully.",
  FAILED_DOWNLOAD: "The output file could not be downloaded.",
  CANCELLED_DOWNLOAD: "The output download was cancelled.",
};

function getMonitorFeedback(
  state: WizardState,
  monitorFeedbackMessage: string | null,
): WizardFeedback {
  switch (state.cancellationState) {
    case "REQUESTING":
      return {
        message: "Requesting job cancellation...",
        isError: false,
      };

    case "RECONCILING":
      return {
        message: "Checking whether cancellation was accepted...",
        isError: false,
      };

    case "FAILED":
      return {
        message: "Job cancellation could not be confirmed.",
        isError: true,
      };

    default:
      break;
  }

  if (state.monitorState === "FAILED") {
    return {
      message:
        monitorFeedbackMessage ?? "Unable to retrieve the current job status.",
      isError: true,
    };
  }

  if (
    state.cancellationState === "ACCEPTED" &&
    state.jobStatus !== "CANCELLED"
  ) {
    return {
      message:
        "Cancellation request was accepted. " + "Waiting for the job to stop.",
      isError: false,
    };
  }

  if (monitorFeedbackMessage !== null) {
    return {
      message: monitorFeedbackMessage,
      isError: false,
    };
  }

  return EMPTY_FEEDBACK;
}

function getWizardFeedback(
  state: WizardState,
  monitorFeedbackMessage: string | null,
): WizardFeedback {
  switch (state.activeStep) {
    case WizardStep.Upload:
      return {
        message: UPLOAD_FEEDBACK[state.uploadManagerState],
        isError: state.uploadManagerState === "FAILED",
      };

    case WizardStep.Parameters:
      return {
        message: PARAMETER_FEEDBACK[state.parameterUpdateState],
        isError: state.parameterUpdateState === "FAILED",
      };

    case WizardStep.Review:
      return {
        message: SUBMISSION_FEEDBACK[state.submissionState],
        isError:
          state.submissionState === "FAILED" ||
          state.submissionState === "UNCONFIRMED",
      };

    case WizardStep.Monitor:
      return getMonitorFeedback(state, monitorFeedbackMessage);

    case WizardStep.Outputs:
      return {
        message: DOWNLOAD_FEEDBACK[state.downloadState],
        isError: state.downloadState === "FAILED_DOWNLOAD",
      };

    default:
      return EMPTY_FEEDBACK;
  }
}

interface WizardStepContentProps {
  activeStep: WizardStep;
  jobType: string;
  inputFiles: InputFiles;
  upload: UploadWorkflow;
  parameters: ParameterWorkflow;
  execution: ExecutionWorkflow;
  outputs: OutputWorkflow;
  onJobTypeChange: (jobType: string) => void;
  onFileChange: (inputSlot: keyof InputFiles, file: File | null) => void;
  onConfirmSubmission: () => void;
  onStartNewJob: () => void;
}

function WizardStepContent({
  activeStep,
  jobType,
  inputFiles,
  upload,
  parameters,
  execution,
  outputs,
  onJobTypeChange,
  onFileChange,
  onConfirmSubmission,
  onStartNewJob,
}: Readonly<WizardStepContentProps>) {
  switch (activeStep) {
    case WizardStep.JobType:
      return (
        <JobTypeStep jobType={jobType} onJobTypeChange={onJobTypeChange} />
      );

    case WizardStep.Inputs:
      return <InputsStep files={inputFiles} onFileChange={onFileChange} />;

    case WizardStep.Upload:
      return (
        <UploadStep
          files={inputFiles}
          inputSlotStates={upload.inputSlotStates}
          uploadManagerState={upload.uploadManagerState}
          onStopUploading={upload.stopUploading}
          onRetry={upload.retryUpload}
        />
      );

    case WizardStep.Parameters:
      return (
        <ParametersStep
          solvingMethod={parameters.solvingMethod}
          parameterUpdateState={parameters.parameterUpdateState}
          onSolvingMethodChange={parameters.changeSolvingMethod}
          onRetryParameterUpdate={parameters.retryParameterUpdate}
        />
      );

    case WizardStep.Review:
      if (!inputFiles.matrix || !inputFiles.rhs) {
        return null;
      }

      return (
        <ReviewStep
          jobType={jobType}
          matrixFile={inputFiles.matrix}
          rhsFile={inputFiles.rhs}
          solvingMethod={parameters.solvingMethod}
          submissionState={execution.submissionState}
          onDismissConfirmation={execution.dismissSubmissionConfirmation}
          onConfirmSubmission={onConfirmSubmission}
        />
      );

    case WizardStep.Monitor:
      if (execution.jobStatus === null) {
        return null;
      }
      return (
        <MonitorStep
          jobStatus={execution.jobStatus}
          jobProgress={execution.jobProgress}
          jobMessage={execution.jobMessage}
          monitorState={execution.monitorState}
          cancellationState={execution.cancellationState}
          onRequestCancellation={execution.requestCancellation}
          onDismissCancellation={execution.dismissCancellation}
          onConfirmCancellation={() => {
            void execution.confirmCancellation();
          }}
          onRetryMonitoring={() => {
            void execution.retryMonitoring();
          }}
          onStartNewJob={onStartNewJob}
        />
      );

    case WizardStep.Outputs:
      return (
        <OutputsStep
          downloadState={outputs.downloadState}
          onDownload={() => {
            void outputs.startDownload();
          }}
          onCancelDownload={outputs.cancelDownload}
        />
      );

    default:
      return null;
  }
}

function createUploadSessionId(): string {
  return `upload-${globalThis.crypto.randomUUID()}`;
}

export function JobWizard({
  apiClient,
  artifactTransferClient,
  onJobTypeSelected,
  onInputsSelected,
  onInputsUploaded,
  onParametersSynchronized,
  onJobSubmitted,
  onJobCancellationAccepted,
  onOutputsRequested,
  onOutputDownloaded,
  onNewJobStarted,
}: Readonly<JobWizardProps>) {
  const [activeStep, setActiveStep] = useState<WizardStep>(WizardStep.JobType);
  const [jobType, setJobType] = useState("solving_slae");
  const [uploadSessionId, setUploadSessionId] = useState(createUploadSessionId);

  const [inputFiles, setInputFiles] = useState<InputFiles>({
    matrix: null,
    rhs: null,
  });

  const upload = useUploadWorkflow({
    apiClient,
    artifactTransferClient,
    jobType,
    uploadSessionId,
    files: inputFiles,
  });

  const parameters = useParameterWorkflow({
    apiClient,
    jobId: upload.jobId,
  });

  const execution = useExecutionWorkflow({
    apiClient,
    jobId: upload.jobId,
    onJobSubmitted,
    onJobCancellationAccepted,
  });

  const outputs = useOutputWorkflow({
    apiClient,
    artifactTransferClient,
    jobId: upload.jobId,
    onOutputDownloaded,
  });

  const allRequiredFilesSelected =
    inputFiles.matrix !== null && inputFiles.rhs !== null;

  const handleFileChange = (inputSlot: keyof InputFiles, file: File | null) => {
    setInputFiles((currentFiles) => ({
      ...currentFiles,
      [inputSlot]: file,
    }));

    upload.selectFile(inputSlot, file);
  };

  const resetWizard = () => {
    upload.reset();
    parameters.reset();
    execution.reset();
    outputs.reset();

    setActiveStep(WizardStep.JobType);
    setJobType("solving_slae");
    setUploadSessionId(createUploadSessionId());
    setInputFiles({
      matrix: null,
      rhs: null,
    });

    onNewJobStarted?.();
  };

  const handleSubmissionConfirmed = async () => {
    const submitted = await execution.submitJob();
    if (!submitted) {
      return;
    }

    setActiveStep(WizardStep.Monitor);
    void execution.startMonitoring();
  };

  const handleSubmissionReconciliationRetry = async () => {
    const submitted = await execution.retrySubmissionReconciliation();

    if (!submitted) {
      return;
    }

    setActiveStep(WizardStep.Monitor);
    void execution.startMonitoring();
  };

  const wizardState: WizardState = {
    activeStep,
    jobType,
    allRequiredFilesSelected,
    uploadManagerState: upload.uploadManagerState,
    allRequiredFilesUploaded: upload.allRequiredFilesUploaded,
    parameterUpdateState: parameters.parameterUpdateState,
    submissionState: execution.submissionState,
    jobStatus: execution.jobStatus,
    monitorState: execution.monitorState,
    cancellationState: execution.cancellationState,
    downloadState: outputs.downloadState,
  };

  const previousStep = getPreviousStep(wizardState);

  const handlePrevious = () => {
    if (previousStep !== null) {
      setActiveStep(previousStep);
    }
  };

  const handleNext = () => {
    switch (activeStep) {
      case WizardStep.JobType:
        onJobTypeSelected?.(jobType);
        setActiveStep(WizardStep.Inputs);
        return;

      case WizardStep.Inputs: {
        const { matrix, rhs } = inputFiles;
        if (!matrix || !rhs) {
          return;
        }

        onInputsSelected?.({
          matrix,
          rhs,
        });

        setActiveStep(WizardStep.Upload);

        if (!upload.allRequiredFilesUploaded) {
          void upload.startUpload();
        }
        return;
      }

      case WizardStep.Upload:
        onInputsUploaded?.();
        setActiveStep(WizardStep.Parameters);
        parameters.ensureParametersUpdated();
        return;

      case WizardStep.Parameters:
        onParametersSynchronized?.(parameters.solvingMethod);
        setActiveStep(WizardStep.Review);
        return;

      case WizardStep.Review:
        if (execution.submissionState === "UNCONFIRMED") {
          void handleSubmissionReconciliationRetry();
          return;
        }

        execution.requestSubmissionConfirmation();
        return;

      case WizardStep.Monitor:
        if (execution.jobStatus === "DONE") {
          onOutputsRequested?.();
          outputs.reset();
          setActiveStep(WizardStep.Outputs);
          return;
        }

        if (execution.jobStatus === "CANCELLED") {
          resetWizard();
        }
        return;

      case WizardStep.Outputs:
        resetWizard();
        return;

      default:
        return;
    }
  };

  const feedback = getWizardFeedback(
    wizardState,
    execution.monitorFeedbackMessage,
  );

  return (
    <Paper
      variant="outlined"
      sx={{
        overflow: "hidden",
        boxShadow: 1,
      }}
    >
      <Box
        component="header"
        sx={{
          px: {
            xs: 2,
            sm: 3,
          },
          py: 2.5,
        }}
      >
        <Typography component="h1" variant="h5" fontWeight={600} gutterBottom>
          MDDS Job Wizard
        </Typography>

        <Typography variant="body1" color="text.secondary">
          Create, configure, submit, and monitor an MDDS job
        </Typography>
      </Box>

      <Divider />

      <WizardStepper activeStep={activeStep} />

      <Divider />

      <Box
        sx={{
          p: {
            xs: 2,
            sm: 3,
          },
        }}
      >
        <WizardStepContent
          activeStep={activeStep}
          jobType={jobType}
          inputFiles={inputFiles}
          upload={upload}
          parameters={parameters}
          execution={execution}
          outputs={outputs}
          onJobTypeChange={setJobType}
          onFileChange={handleFileChange}
          onConfirmSubmission={() => {
            void handleSubmissionConfirmed();
          }}
          onStartNewJob={resetWizard}
        />
      </Box>

      <Divider />

      <Box
        component="section"
        role="status"
        aria-live="polite"
        aria-label="Wizard feedback"
        sx={{
          minHeight: 64,
          px: {
            xs: 2,
            sm: 3,
          },
          py: 2,
          bgcolor: "action.hover",
        }}
      >
        {feedback.message !== null && (
          <Typography
            variant="body2"
            color={feedback.isError ? "error.main" : "text.secondary"}
          >
            {feedback.message}
          </Typography>
        )}
      </Box>

      <Divider />

      <Box
        component="nav"
        aria-label="Wizard navigation"
        sx={{
          px: {
            xs: 2,
            sm: 3,
          },
          py: 2,
        }}
      >
        <WizardNavigation
          previousDisabled={previousStep === null}
          nextDisabled={isNextDisabled(wizardState)}
          nextLabel={getNextLabel(wizardState)}
          showPrevious={
            activeStep !== WizardStep.Monitor &&
            activeStep !== WizardStep.Outputs
          }
          showNext={
            !(
              activeStep === WizardStep.Monitor &&
              (execution.jobStatus === "ERROR" ||
                execution.monitorState === "FAILED")
            )
          }
          onPrevious={handlePrevious}
          onNext={handleNext}
        />
      </Box>
    </Paper>
  );
}
