// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MddsApiClient } from "../api/MddsApiClient";
import type { ArtifactTransferClient } from "../artifacts/ArtifactTransferClient";
import type { JobWizardProps } from "./JobWizard";
import { JobWizard } from "./JobWizard";
import type {
  ExecutionWorkflow,
  OutputWorkflow,
  ParameterWorkflow,
  UploadWorkflow,
} from "./useJobWizardWorkflows";
import {
  useExecutionWorkflow,
  useOutputWorkflow,
  useParameterWorkflow,
  useUploadWorkflow,
} from "./useJobWizardWorkflows";

vi.mock("@/components/WizardStepper", () => ({
  WizardStepper: ({ activeStep }: { activeStep: string }) => (
    <div data-testid="wizard-stepper">{activeStep}</div>
  ),
}));

vi.mock("@/components/WizardNavigation", () => ({
  WizardNavigation: ({
    previousDisabled,
    nextDisabled,
    nextLabel,
    showPrevious,
    onPrevious,
    onNext,
  }: {
    previousDisabled: boolean;
    nextDisabled: boolean;
    nextLabel: string;
    showPrevious: boolean;
    onPrevious: () => void;
    onNext: () => void;
  }) => (
    <div aria-label="Wizard navigation">
      {showPrevious && (
        <button type="button" disabled={previousDisabled} onClick={onPrevious}>
          Previous
        </button>
      )}

      <button type="button" disabled={nextDisabled} onClick={onNext}>
        {nextLabel}
      </button>
    </div>
  ),
}));

vi.mock("./screens/JobTypeStep", () => ({
  JobTypeStep: ({ jobType }: { jobType: string }) => (
    <section data-testid="job-type-screen">
      <h2>JobType screen</h2>
      <span>{jobType}</span>
    </section>
  ),
}));

vi.mock("./screens/InputsStep", () => ({
  InputsStep: ({
    onFileChange,
  }: {
    onFileChange: (inputSlot: "matrix" | "rhs", file: File) => void;
  }) => (
    <section data-testid="inputs-screen">
      <h2>Inputs screen</h2>

      <button
        type="button"
        onClick={() => {
          onFileChange(
            "matrix",
            new File(["1,0\n0,1"], "matrix.csv", {
              type: "text/csv",
            }),
          );
        }}
      >
        Select matrix
      </button>

      <button
        type="button"
        onClick={() => {
          onFileChange(
            "rhs",
            new File(["1\n2"], "rhs.csv", {
              type: "text/csv",
            }),
          );
        }}
      >
        Select RHS
      </button>
    </section>
  ),
}));

vi.mock("./screens/UploadStep", () => ({
  UploadStep: ({ onStopUploading }: { onStopUploading: () => void }) => (
    <section data-testid="upload-screen">
      <h2>Upload screen</h2>

      <button type="button" onClick={onStopUploading}>
        Stop upload
      </button>
    </section>
  ),
}));

vi.mock("./screens/ParametersStep", () => ({
  ParametersStep: () => (
    <section data-testid="parameters-screen">
      <h2>Parameters screen</h2>
    </section>
  ),
}));

vi.mock("./screens/ReviewStep", () => ({
  ReviewStep: ({
    submissionState,
    onConfirmSubmission,
  }: {
    submissionState: string;
    onConfirmSubmission: () => void;
  }) => (
    <section data-testid="review-screen">
      <h2>Review screen</h2>
      <span>{submissionState}</span>

      {submissionState === "CONFIRMING" && (
        <button type="button" onClick={onConfirmSubmission}>
          Confirm submission
        </button>
      )}
    </section>
  ),
}));

vi.mock("./screens/MonitorStep", () => ({
  MonitorStep: ({
    jobStatus,
    monitorState,
    onRetryMonitoring,
    onStartNewJob,
  }: {
    jobStatus: string;
    monitorState: string;
    onRetryMonitoring: () => void;
    onStartNewJob: () => void;
  }) => (
    <section data-testid="monitor-screen">
      <h2>Monitor screen</h2>

      {jobStatus === "ERROR" && (
        <button type="button" onClick={onStartNewJob}>
          Start new job
        </button>
      )}

      {jobStatus !== "ERROR" && monitorState === "FAILED" && (
        <>
          <button type="button" onClick={onRetryMonitoring}>
            Retry monitoring
          </button>

          <button type="button" onClick={onStartNewJob}>
            Start new job
          </button>
        </>
      )}
    </section>
  ),
}));

vi.mock("./screens/OutputsStep", () => ({
  OutputsStep: () => (
    <section data-testid="outputs-screen">
      <h2>Outputs screen</h2>
    </section>
  ),
}));

vi.mock("./useJobWizardWorkflows", () => ({
  useUploadWorkflow: vi.fn(),
  useParameterWorkflow: vi.fn(),
  useExecutionWorkflow: vi.fn(),
  useOutputWorkflow: vi.fn(),
}));

function createUploadWorkflow(
  overrides: Partial<UploadWorkflow> = {},
): UploadWorkflow {
  return {
    jobId: null,
    uploadManagerState: "IDLE",
    inputSlotStates: {
      matrix: "EMPTY",
      rhs: "EMPTY",
    },
    allRequiredFilesUploaded: false,
    selectFile: vi.fn(),
    startUpload: vi.fn(async () => undefined),
    stopUploading: vi.fn(),
    retryUpload: vi.fn(),
    reset: vi.fn(),
    ...overrides,
  };
}

function createParameterWorkflow(
  overrides: Partial<ParameterWorkflow> = {},
): ParameterWorkflow {
  return {
    solvingMethod: "numpy_exact_solver",
    parameterUpdateState: "PENDING",
    changeSolvingMethod: vi.fn(),
    ensureParametersUpdated: vi.fn(),
    retryParameterUpdate: vi.fn(),
    reset: vi.fn(),
    ...overrides,
  };
}

function createExecutionWorkflow(
  overrides: Partial<ExecutionWorkflow> = {},
): ExecutionWorkflow {
  return {
    submissionState: "IDLE",
    jobStatus: null,
    jobProgress: 0,
    jobMessage: "The job has not started yet.",
    monitorState: "IDLE",
    monitorFeedbackMessage: null,
    cancellationState: "IDLE",
    requestSubmissionConfirmation: vi.fn(),
    dismissSubmissionConfirmation: vi.fn(),
    submitJob: vi.fn(async () => true),
    startMonitoring: vi.fn(async () => undefined),
    requestCancellation: vi.fn(),
    dismissCancellation: vi.fn(),
    confirmCancellation: vi.fn(async () => undefined),
    reset: vi.fn(),
    retryMonitoring: vi.fn(async () => undefined),
    retrySubmissionReconciliation: vi.fn(async () => false),
    ...overrides,
  };
}

function createOutputWorkflow(
  overrides: Partial<OutputWorkflow> = {},
): OutputWorkflow {
  return {
    downloadState: "IDLE",
    startDownload: vi.fn(async () => undefined),
    cancelDownload: vi.fn(),
    reset: vi.fn(),
    ...overrides,
  };
}

let upload: UploadWorkflow;
let parameters: ParameterWorkflow;
let execution: ExecutionWorkflow;
let outputs: OutputWorkflow;

beforeEach(() => {
  upload = createUploadWorkflow();
  parameters = createParameterWorkflow();
  execution = createExecutionWorkflow();
  outputs = createOutputWorkflow();

  vi.mocked(useUploadWorkflow).mockReturnValue(upload);
  vi.mocked(useParameterWorkflow).mockReturnValue(parameters);
  vi.mocked(useExecutionWorkflow).mockReturnValue(execution);
  vi.mocked(useOutputWorkflow).mockReturnValue(outputs);
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

const apiClient = {} as MddsApiClient;
const artifactTransferClient = {} as ArtifactTransferClient;

type JobWizardCallbackProps = Omit<
  JobWizardProps,
  "apiClient" | "artifactTransferClient"
>;

function renderWizard(props: JobWizardCallbackProps = {}) {
  const result = render(
    <JobWizard
      apiClient={apiClient}
      artifactTransferClient={artifactTransferClient}
      {...props}
    />,
  );

  return {
    ...result,
    rerenderWizard: () => {
      result.rerender(
        <JobWizard
          apiClient={apiClient}
          artifactTransferClient={artifactTransferClient}
          {...props}
        />,
      );
    },
  };
}

function clickNext(label = "Next >") {
  fireEvent.click(
    screen.getByRole("button", {
      name: label,
    }),
  );
}

function mockSuccessfulSubmission() {
  vi.mocked(execution.submitJob).mockImplementation(async () => {
    execution.submissionState = "SUBMITTED";
    execution.jobStatus = "SUBMITTED";
    return true;
  });
}

function openInputs() {
  clickNext();

  expect(screen.getByTestId("inputs-screen")).toBeTruthy();
}

function selectRequiredFiles() {
  fireEvent.click(
    screen.getByRole("button", {
      name: "Select matrix",
    }),
  );

  fireEvent.click(
    screen.getByRole("button", {
      name: "Select RHS",
    }),
  );

  const selectFileMock = vi.mocked(upload.selectFile);

  const matrixFile = selectFileMock.mock.calls.find(
    ([inputSlot]) => inputSlot === "matrix",
  )?.[1];

  const rhsFile = selectFileMock.mock.calls.find(
    ([inputSlot]) => inputSlot === "rhs",
  )?.[1];

  if (!matrixFile || !rhsFile) {
    throw new Error("Required files were not passed to the upload workflow.");
  }

  return {
    matrixFile,
    rhsFile,
  };
}

function openUpload() {
  openInputs();
  const files = selectRequiredFiles();
  clickNext();

  expect(screen.getByTestId("upload-screen")).toBeTruthy();

  return files;
}

function openParameters(rerenderWizard: () => void) {
  const files = openUpload();

  upload.uploadManagerState = "COMPLETED";
  upload.allRequiredFilesUploaded = true;
  rerenderWizard();

  clickNext();

  expect(screen.getByTestId("parameters-screen")).toBeTruthy();

  return files;
}

function openReview(
  rerenderWizard: () => void,
  solvingMethod: ParameterWorkflow["solvingMethod"] = "numpy_exact_solver",
) {
  const files = openParameters(rerenderWizard);

  parameters.parameterUpdateState = "UPDATED";
  parameters.solvingMethod = solvingMethod;
  rerenderWizard();

  clickNext();

  expect(screen.getByTestId("review-screen")).toBeTruthy();

  return files;
}

async function openMonitor(rerenderWizard: () => void) {
  openReview(rerenderWizard);

  clickNext("Submit job");

  execution.submissionState = "CONFIRMING";
  rerenderWizard();

  mockSuccessfulSubmission();

  fireEvent.click(
    screen.getByRole("button", {
      name: "Confirm submission",
    }),
  );

  await waitFor(() => {
    expect(screen.getByTestId("monitor-screen")).toBeTruthy();
  });
}

describe("JobWizard", () => {
  it("shows the initial JobType screen and opens Inputs", () => {
    const onJobTypeSelected = vi.fn();

    renderWizard({
      onJobTypeSelected,
    });

    expect(
      screen.getByRole("heading", {
        name: "MDDS Job Wizard",
      }),
    ).toBeTruthy();

    expect(screen.getByTestId("job-type-screen")).toBeTruthy();

    expect(screen.getByText("solving_slae")).toBeTruthy();

    const previousButton = screen.getByRole("button", {
      name: "Previous",
    }) as HTMLButtonElement;

    expect(previousButton.disabled).toBe(true);

    const nextButton = screen.getByRole("button", {
      name: "Next >",
    }) as HTMLButtonElement;

    expect(nextButton.disabled).toBe(false);

    fireEvent.click(nextButton);

    expect(onJobTypeSelected).toHaveBeenCalledOnce();

    expect(onJobTypeSelected).toHaveBeenCalledWith("solving_slae");

    expect(screen.getByTestId("inputs-screen")).toBeTruthy();
  });

  it("stays on Upload after the user stops uploading", () => {
    const { rerenderWizard } = renderWizard();

    openUpload();

    upload.uploadManagerState = "RUNNING";
    rerenderWizard();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Stop upload",
      }),
    );

    expect(upload.stopUploading).toHaveBeenCalledOnce();

    upload.uploadManagerState = "STOPPED_BY_USER";
    rerenderWizard();

    expect(screen.getByTestId("upload-screen")).toBeTruthy();
    expect(screen.queryByTestId("inputs-screen")).toBeNull();
    expect(screen.queryByTestId("parameters-screen")).toBeNull();

    const previousButton = screen.getByRole("button", {
      name: "Previous",
    }) as HTMLButtonElement;

    expect(previousButton.disabled).toBe(false);
  });

  it("selects required files and starts their upload", () => {
    const onInputsSelected = vi.fn();

    renderWizard({
      onInputsSelected,
    });

    openInputs();

    const disabledNextButton = screen.getByRole("button", {
      name: "Next >",
    }) as HTMLButtonElement;

    expect(disabledNextButton.disabled).toBe(true);

    const { matrixFile, rhsFile } = selectRequiredFiles();

    expect(upload.selectFile).toHaveBeenCalledWith("matrix", matrixFile);

    expect(upload.selectFile).toHaveBeenCalledWith("rhs", rhsFile);

    const enabledNextButton = screen.getByRole("button", {
      name: "Next >",
    }) as HTMLButtonElement;

    expect(enabledNextButton.disabled).toBe(false);

    fireEvent.click(enabledNextButton);

    expect(onInputsSelected).toHaveBeenCalledWith({
      matrix: matrixFile,
      rhs: rhsFile,
    });

    expect(upload.startUpload).toHaveBeenCalledOnce();

    expect(screen.getByTestId("upload-screen")).toBeTruthy();
  });

  it("does not upload files that are already uploaded", () => {
    const { rerenderWizard } = renderWizard();

    openInputs();
    selectRequiredFiles();

    upload.allRequiredFilesUploaded = true;
    rerenderWizard();

    clickNext();

    expect(upload.startUpload).not.toHaveBeenCalled();

    expect(screen.getByTestId("upload-screen")).toBeTruthy();
  });

  it("opens Parameters after completed upload", () => {
    const onInputsUploaded = vi.fn();

    const { rerenderWizard } = renderWizard({
      onInputsUploaded,
    });

    openUpload();

    upload.uploadManagerState = "COMPLETED";
    upload.allRequiredFilesUploaded = true;
    rerenderWizard();

    clickNext();

    expect(onInputsUploaded).toHaveBeenCalledOnce();

    expect(parameters.ensureParametersUpdated).toHaveBeenCalledOnce();

    expect(screen.getByTestId("parameters-screen")).toBeTruthy();
  });

  it("opens Review after synchronized parameters", () => {
    const onParametersSynchronized = vi.fn();

    const { rerenderWizard } = renderWizard({
      onParametersSynchronized,
    });

    openParameters(rerenderWizard);

    parameters.parameterUpdateState = "UPDATED";
    parameters.solvingMethod = "numpy_lstsq_solver";
    rerenderWizard();

    clickNext();

    expect(onParametersSynchronized).toHaveBeenCalledWith("numpy_lstsq_solver");

    expect(screen.getByTestId("review-screen")).toBeTruthy();
  });

  it("requests confirmation before submitting", () => {
    const { rerenderWizard } = renderWizard();

    openReview(rerenderWizard);

    clickNext("Submit job");

    expect(execution.requestSubmissionConfirmation).toHaveBeenCalledOnce();

    expect(execution.submitJob).not.toHaveBeenCalled();

    expect(execution.startMonitoring).not.toHaveBeenCalled();

    expect(screen.getByTestId("review-screen")).toBeTruthy();

    expect(screen.queryByTestId("monitor-screen")).toBeNull();
  });

  it("opens Monitor after successful submission", async () => {
    const { rerenderWizard } = renderWizard();

    mockSuccessfulSubmission();

    openReview(rerenderWizard);
    clickNext("Submit job");

    execution.submissionState = "CONFIRMING";
    rerenderWizard();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Confirm submission",
      }),
    );

    await waitFor(() => {
      expect(execution.submitJob).toHaveBeenCalledOnce();

      expect(execution.startMonitoring).toHaveBeenCalledOnce();

      expect(screen.getByTestId("monitor-screen")).toBeTruthy();
    });
  });

  it("stays on Review after failed submission", async () => {
    const { rerenderWizard } = renderWizard();

    vi.mocked(execution.submitJob).mockResolvedValue(false);

    openReview(rerenderWizard);
    clickNext("Submit job");

    execution.submissionState = "CONFIRMING";
    rerenderWizard();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Confirm submission",
      }),
    );

    await waitFor(() => {
      expect(execution.submitJob).toHaveBeenCalledOnce();
    });

    expect(execution.startMonitoring).not.toHaveBeenCalled();

    expect(screen.getByTestId("review-screen")).toBeTruthy();

    expect(screen.queryByTestId("monitor-screen")).toBeNull();
  });

  it("opens Outputs for a completed job", async () => {
    const onOutputsRequested = vi.fn();

    const { rerenderWizard } = renderWizard({
      onOutputsRequested,
    });

    await openMonitor(rerenderWizard);

    execution.jobStatus = "DONE";
    rerenderWizard();

    clickNext("View outputs >");

    expect(onOutputsRequested).toHaveBeenCalledOnce();

    expect(outputs.reset).toHaveBeenCalledOnce();

    expect(screen.getByTestId("outputs-screen")).toBeTruthy();
  });

  it("starts a new job after cancellation", async () => {
    const onNewJobStarted = vi.fn();

    const { rerenderWizard } = renderWizard({
      onNewJobStarted,
    });

    await openMonitor(rerenderWizard);

    execution.jobStatus = "CANCELLED";
    rerenderWizard();

    clickNext("Start new job");

    expect(upload.reset).toHaveBeenCalledOnce();
    expect(parameters.reset).toHaveBeenCalledOnce();
    expect(execution.reset).toHaveBeenCalledOnce();
    expect(outputs.reset).toHaveBeenCalledOnce();
    expect(onNewJobStarted).toHaveBeenCalledOnce();

    expect(screen.getByTestId("job-type-screen")).toBeTruthy();
  });

  it("starts a new job from Outputs", async () => {
    const onNewJobStarted = vi.fn();

    const { rerenderWizard } = renderWizard({
      onNewJobStarted,
    });

    await openMonitor(rerenderWizard);

    execution.jobStatus = "DONE";
    rerenderWizard();

    clickNext("View outputs >");

    vi.clearAllMocks();

    clickNext("Start new job");

    expect(upload.reset).toHaveBeenCalledOnce();
    expect(parameters.reset).toHaveBeenCalledOnce();
    expect(execution.reset).toHaveBeenCalledOnce();
    expect(outputs.reset).toHaveBeenCalledOnce();
    expect(onNewJobStarted).toHaveBeenCalledOnce();

    expect(screen.getByTestId("job-type-screen")).toBeTruthy();
  });

  it("returns from Inputs to JobType", () => {
    renderWizard();

    openInputs();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Previous",
      }),
    );

    expect(screen.getByTestId("job-type-screen")).toBeTruthy();
  });

  it("returns from Parameters to Upload", () => {
    const { rerenderWizard } = renderWizard();

    openParameters(rerenderWizard);

    fireEvent.click(
      screen.getByRole("button", {
        name: "Previous",
      }),
    );

    expect(screen.getByTestId("upload-screen")).toBeTruthy();
  });

  it("shows monitoring failure after cancellation was accepted", async () => {
    const { rerenderWizard } = renderWizard();

    await openMonitor(rerenderWizard);

    execution.jobStatus = "IN_PROGRESS";
    execution.jobMessage = "Worker is processing the job.";
    execution.monitorState = "FAILED";
    execution.monitorFeedbackMessage = "Status endpoint unavailable.";
    execution.cancellationState = "ACCEPTED";

    rerenderWizard();

    expect(screen.getByText("Status endpoint unavailable.")).toBeTruthy();

    expect(
      screen.queryByText(
        "Cancellation request was accepted. Waiting for the job to stop.",
      ),
    ).toBeNull();
  });

  it("starts a new job after execution error", async () => {
    const onNewJobStarted = vi.fn();

    const { rerenderWizard } = renderWizard({
      onNewJobStarted,
    });

    await openMonitor(rerenderWizard);

    execution.jobStatus = "ERROR";
    execution.monitorState = "COMPLETED";
    rerenderWizard();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Start new job",
      }),
    );

    expect(upload.reset).toHaveBeenCalledOnce();
    expect(parameters.reset).toHaveBeenCalledOnce();
    expect(execution.reset).toHaveBeenCalledOnce();
    expect(outputs.reset).toHaveBeenCalledOnce();
    expect(onNewJobStarted).toHaveBeenCalledOnce();

    expect(screen.getByTestId("job-type-screen")).toBeTruthy();
  });

  it("retries failed job monitoring", async () => {
    const { rerenderWizard } = renderWizard();

    await openMonitor(rerenderWizard);

    execution.jobStatus = "IN_PROGRESS";
    execution.monitorState = "FAILED";
    rerenderWizard();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Retry monitoring",
      }),
    );

    expect(execution.retryMonitoring).toHaveBeenCalledOnce();

    expect(upload.reset).not.toHaveBeenCalled();
    expect(parameters.reset).not.toHaveBeenCalled();
    expect(execution.reset).not.toHaveBeenCalled();
    expect(outputs.reset).not.toHaveBeenCalled();

    expect(screen.getByTestId("monitor-screen")).toBeTruthy();
  });

  it("starts a new job after monitoring failure", async () => {
    const onNewJobStarted = vi.fn();

    const { rerenderWizard } = renderWizard({
      onNewJobStarted,
    });

    await openMonitor(rerenderWizard);

    execution.jobStatus = "IN_PROGRESS";
    execution.monitorState = "FAILED";
    rerenderWizard();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Start new job",
      }),
    );

    expect(upload.reset).toHaveBeenCalledOnce();
    expect(parameters.reset).toHaveBeenCalledOnce();
    expect(execution.reset).toHaveBeenCalledOnce();
    expect(outputs.reset).toHaveBeenCalledOnce();
    expect(onNewJobStarted).toHaveBeenCalledOnce();

    expect(screen.getByTestId("job-type-screen")).toBeTruthy();
  });
});
