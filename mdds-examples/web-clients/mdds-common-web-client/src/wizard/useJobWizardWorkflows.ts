/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import { useCallback, useEffect, useRef, useState } from "react";

import type { MddsApiClient } from "@/api/MddsApiClient";
import {
  isAmbiguousCancellationFailure,
  isAmbiguousMddsApiFailure,
} from "@/api/MddsApiFailure";
import type { JobStatus, JobStatusResponseDTO } from "@/api/MddsRestTypes";
import type { ArtifactTransferClient } from "@/artifacts/ArtifactTransferClient";
import type {
  CancellationState,
  DownloadState,
  InputFiles,
  InputSlotState,
  InputSlotStates,
  MonitorState,
  ParameterUpdateState,
  PublicJobStatus,
  SolvingMethod,
  SubmissionState,
  UploadManagerState,
} from "./WizardState";

const DEFAULT_SOLVING_METHOD: SolvingMethod = "numpy_exact_solver";
const INITIAL_JOB_MESSAGE = "The job has not started yet.";
const SOLUTION_FILE_NAME = "solution.csv";
const SOLUTION_OUTPUT_SLOT = "solution";
const MONITOR_POLL_INTERVAL_MS = 700;

interface PendingDelay {
  timerId: number;
  resolve: (completed: boolean) => void;
}

function useCancellableDelay() {
  const pendingDelaysRef = useRef<PendingDelay[]>([]);

  const removePendingDelay = useCallback((timerId: number) => {
    pendingDelaysRef.current = pendingDelaysRef.current.filter(
      (pendingDelay) => pendingDelay.timerId !== timerId,
    );
  }, []);

  const wait = useCallback(
    (delayMs: number) =>
      new Promise<boolean>((resolve) => {
        const timerId = window.setTimeout(() => {
          removePendingDelay(timerId);
          resolve(true);
        }, delayMs);

        pendingDelaysRef.current.push({
          timerId,
          resolve,
        });
      }),
    [removePendingDelay],
  );

  const cancelAll = useCallback(() => {
    const pendingDelays = pendingDelaysRef.current;
    pendingDelaysRef.current = [];

    for (const pendingDelay of pendingDelays) {
      window.clearTimeout(pendingDelay.timerId);
      pendingDelay.resolve(false);
    }
  }, []);

  useEffect(() => cancelAll, [cancelAll]);

  return {
    wait,
    cancelAll,
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message.trim();
  }

  return fallback;
}

interface UploadWorkflowProps {
  apiClient: MddsApiClient;
  artifactTransferClient: ArtifactTransferClient;
  jobType: string;
  uploadSessionId: string;
  files: InputFiles;
}

export interface UploadWorkflow {
  jobId: string | null;
  uploadManagerState: UploadManagerState;
  inputSlotStates: InputSlotStates;
  allRequiredFilesUploaded: boolean;
  selectFile: (inputSlot: keyof InputFiles, file: File | null) => void;
  startUpload: () => Promise<void>;
  stopUploading: () => void;
  retryUpload: () => void;
  reset: () => void;
}

export function useUploadWorkflow({
  apiClient,
  artifactTransferClient,
  jobType,
  uploadSessionId,
  files,
}: Readonly<UploadWorkflowProps>): UploadWorkflow {
  const [jobId, setJobId] = useState<string | null>(null);

  const [uploadManagerState, setUploadManagerState] =
    useState<UploadManagerState>("IDLE");

  const [inputSlotStates, setInputSlotStates] = useState<InputSlotStates>({
    matrix: "EMPTY",
    rhs: "EMPTY",
  });

  const abortControllerRef = useRef<AbortController | null>(null);

  const abortActiveUpload = useCallback(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
  }, []);

  useEffect(() => abortActiveUpload, [abortActiveUpload]);

  const setInputSlotState = useCallback(
    (inputSlot: keyof InputFiles, state: InputSlotState) => {
      setInputSlotStates((currentStates) => ({
        ...currentStates,
        [inputSlot]: state,
      }));
    },
    [],
  );

  type UploadSlotResult = "UPLOADED" | "FAILED";

  const uploadSlot = useCallback(
    async (
      currentJobId: string,
      inputSlot: keyof InputFiles,
      file: File,
      signal: AbortSignal,
    ): Promise<UploadSlotResult> => {
      setInputSlotState(inputSlot, "UPLOADING");

      try {
        const response = await apiClient.requestInputUploadUrl(
          currentJobId,
          inputSlot,
        );

        await artifactTransferClient.upload(response.uploadUrl, file, signal);

        setInputSlotState(inputSlot, "UPLOADED");
        return "UPLOADED";
      } catch (error) {
        if (isAbortError(error)) {
          throw error;
        }

        setInputSlotState(inputSlot, "FAILED");
        return "FAILED";
      }
    },
    [apiClient, artifactTransferClient, setInputSlotState],
  );

  const startUpload = useCallback(async () => {
    const matrixFile = files.matrix;
    const rhsFile = files.rhs;

    if (!matrixFile || !rhsFile) {
      setUploadManagerState("FAILED");
      return;
    }

    abortActiveUpload();
    const abortController = new AbortController();
    abortControllerRef.current = abortController;
    setUploadManagerState("RUNNING");

    try {
      const draft = await apiClient.createOrReuseDraftJob(
        jobType,
        uploadSessionId,
      );

      if (abortController.signal.aborted) {
        return;
      }

      setJobId(draft.jobId);
      const startingStates = inputSlotStates;

      let hasUploadFailures = false;

      if (startingStates.matrix !== "UPLOADED") {
        const result = await uploadSlot(
          draft.jobId,
          "matrix",
          matrixFile,
          abortController.signal,
        );

        hasUploadFailures ||= result === "FAILED";
      }

      if (abortController.signal.aborted) {
        return;
      }

      if (startingStates.rhs !== "UPLOADED") {
        const result = await uploadSlot(
          draft.jobId,
          "rhs",
          rhsFile,
          abortController.signal,
        );

        hasUploadFailures ||= result === "FAILED";
      }

      if (!abortController.signal.aborted) {
        setUploadManagerState(hasUploadFailures ? "FAILED" : "COMPLETED");
      }
    } catch (error) {
      if (!isAbortError(error)) {
        setUploadManagerState("FAILED");
      }
    } finally {
      if (abortControllerRef.current === abortController) {
        abortControllerRef.current = null;
      }
    }
  }, [
    abortActiveUpload,
    apiClient,
    files.matrix,
    files.rhs,
    inputSlotStates,
    jobType,
    uploadSessionId,
    uploadSlot,
  ]);

  const selectFile = useCallback(
    (inputSlot: keyof InputFiles, file: File | null) => {
      abortActiveUpload();
      setInputSlotState(inputSlot, file ? "FILE_SELECTED" : "EMPTY");
      setUploadManagerState("IDLE");
    },
    [abortActiveUpload, setInputSlotState],
  );

  const stopUploading = useCallback(() => {
    abortActiveUpload();
    setUploadManagerState("STOPPED_BY_USER");
  }, [abortActiveUpload]);

  const retryFailedUploads = useCallback(() => {
    void startUpload();
  }, [startUpload]);

  const reset = useCallback(() => {
    abortActiveUpload();
    setJobId(null);
    setUploadManagerState("IDLE");
    setInputSlotStates({
      matrix: "EMPTY",
      rhs: "EMPTY",
    });
  }, [abortActiveUpload]);

  const allRequiredFilesUploaded =
    inputSlotStates.matrix === "UPLOADED" && inputSlotStates.rhs === "UPLOADED";

  return {
    jobId,
    uploadManagerState,
    inputSlotStates,
    allRequiredFilesUploaded,
    selectFile,
    startUpload,
    stopUploading,
    retryUpload: retryFailedUploads,
    reset,
  };
}

interface ParameterWorkflowProps {
  apiClient: MddsApiClient;
  jobId: string | null;
}

export interface ParameterWorkflow {
  solvingMethod: SolvingMethod;
  parameterUpdateState: ParameterUpdateState;
  changeSolvingMethod: (method: SolvingMethod) => void;
  ensureParametersUpdated: () => void;
  retryParameterUpdate: () => void;
  reset: () => void;
}

export function useParameterWorkflow({
  apiClient,
  jobId,
}: Readonly<ParameterWorkflowProps>): ParameterWorkflow {
  const [solvingMethod, setSolvingMethod] = useState<SolvingMethod>(
    DEFAULT_SOLVING_METHOD,
  );

  const [parameterUpdateState, setParameterUpdateState] =
    useState<ParameterUpdateState>("PENDING");

  const operationIdRef = useRef(0);

  const updateParameters = useCallback(
    async (method: SolvingMethod) => {
      const operationId = ++operationIdRef.current;
      setParameterUpdateState("UPDATING");

      if (!jobId) {
        setParameterUpdateState("FAILED");
        return;
      }

      try {
        await apiClient.patchJobParams(jobId, {
          solvingMethod: method,
        });

        if (operationId === operationIdRef.current) {
          setParameterUpdateState("UPDATED");
        }
      } catch {
        if (operationId === operationIdRef.current) {
          setParameterUpdateState("FAILED");
        }
      }
    },
    [apiClient, jobId],
  );

  const changeSolvingMethod = useCallback(
    (method: SolvingMethod) => {
      setSolvingMethod(method);
      setParameterUpdateState("PENDING");
      void updateParameters(method);
    },
    [updateParameters],
  );

  const ensureParametersUpdated = useCallback(() => {
    if (parameterUpdateState !== "UPDATED") {
      void updateParameters(solvingMethod);
    }
  }, [parameterUpdateState, solvingMethod, updateParameters]);

  const retryParameterUpdate = useCallback(() => {
    void updateParameters(solvingMethod);
  }, [solvingMethod, updateParameters]);

  const reset = useCallback(() => {
    operationIdRef.current += 1;
    setSolvingMethod(DEFAULT_SOLVING_METHOD);
    setParameterUpdateState("PENDING");
  }, []);

  return {
    solvingMethod,
    parameterUpdateState,
    changeSolvingMethod,
    ensureParametersUpdated,
    retryParameterUpdate,
    reset,
  };
}

interface ExecutionWorkflowProps {
  apiClient: MddsApiClient;
  jobId: string | null;
  onJobSubmitted?: () => void;
  onJobCancellationAccepted?: () => void;
}

export interface ExecutionWorkflow {
  submissionState: SubmissionState;
  jobStatus: PublicJobStatus | null;
  jobProgress: number;
  jobMessage: string;
  monitorState: MonitorState;
  monitorFeedbackMessage: string | null;
  cancellationState: CancellationState;

  requestSubmissionConfirmation: () => void;
  dismissSubmissionConfirmation: () => void;
  submitJob: () => Promise<boolean>;
  retrySubmissionReconciliation: () => Promise<boolean>;

  startMonitoring: () => Promise<void>;
  retryMonitoring: () => Promise<void>;
  requestCancellation: () => void;
  dismissCancellation: () => void;
  confirmCancellation: () => Promise<void>;
  reset: () => void;
}

export function useExecutionWorkflow({
  apiClient,
  jobId,
  onJobSubmitted,
  onJobCancellationAccepted,
}: Readonly<ExecutionWorkflowProps>): ExecutionWorkflow {
  const [submissionState, setSubmissionState] =
    useState<SubmissionState>("IDLE");

  const [jobStatus, setJobStatus] = useState<PublicJobStatus | null>(null);

  const [jobProgress, setJobProgress] = useState(0);
  const [jobMessage, setJobMessage] = useState(INITIAL_JOB_MESSAGE);
  const [monitorFeedbackMessage, setMonitorFeedbackMessage] = useState<
    string | null
  >(null);
  const [monitorState, setMonitorState] = useState<MonitorState>("IDLE");

  const [cancellationState, setCancellationState] =
    useState<CancellationState>("IDLE");

  const cancellationStateRef = useRef<CancellationState>("IDLE");

  const updateCancellationState = useCallback(
    (nextState: CancellationState) => {
      cancellationStateRef.current = nextState;
      setCancellationState(nextState);
    },
    [],
  );

  const submissionOperationIdRef = useRef(0);
  const cancellationOperationIdRef = useRef(0);
  const monitorRunIdRef = useRef(0);
  const { wait: waitForNextPoll, cancelAll: cancelPolling } =
    useCancellableDelay();

  const requestSubmissionConfirmation = useCallback(() => {
    if (submissionState === "IDLE" || submissionState === "FAILED") {
      setSubmissionState("CONFIRMING");
    }
  }, [submissionState]);

  const dismissSubmissionConfirmation = useCallback(() => {
    if (submissionState === "CONFIRMING") {
      setSubmissionState("IDLE");
    }
  }, [submissionState]);

  const isCurrentSubmissionOperation = useCallback(
    (operationId: number): boolean =>
      operationId === submissionOperationIdRef.current,
    [],
  );

  const confirmSubmittedJob = useCallback(
    (status: PublicJobStatus, progress: number, message: string | null) => {
      setSubmissionState("SUBMITTED");
      setJobStatus(status);
      setJobProgress(progress);

      if (message !== null) {
        setJobMessage(message);
      }

      onJobSubmitted?.();
    },
    [onJobSubmitted],
  );

  const reconcileSubmission = useCallback(
    async (currentJobId: string, operationId: number): Promise<boolean> => {
      setSubmissionState("RECONCILING");

      try {
        const response = await apiClient.getJobStatus(currentJobId);

        if (!isCurrentSubmissionOperation(operationId)) {
          return false;
        }

        if (response.status === "DRAFT") {
          setSubmissionState("FAILED");
          return false;
        }

        confirmSubmittedJob(
          toPublicJobStatus(response.status) ?? "SUBMITTED",
          response.progress,
          response.message,
        );

        return true;
      } catch {
        if (isCurrentSubmissionOperation(operationId)) {
          setSubmissionState("UNCONFIRMED");
        }

        return false;
      }
    },
    [apiClient, confirmSubmittedJob, isCurrentSubmissionOperation],
  );

  const retrySubmissionReconciliation =
    useCallback(async (): Promise<boolean> => {
      if (submissionState !== "UNCONFIRMED" || !jobId) {
        return false;
      }

      const operationId = ++submissionOperationIdRef.current;

      return reconcileSubmission(jobId, operationId);
    }, [jobId, reconcileSubmission, submissionState]);

  const submitJob = useCallback(async (): Promise<boolean> => {
    if (submissionState !== "CONFIRMING" || !jobId) {
      setSubmissionState("FAILED");
      return false;
    }

    const operationId = ++submissionOperationIdRef.current;

    setSubmissionState("REQUESTING");

    try {
      const response = await apiClient.submitJob(jobId);

      if (!isCurrentSubmissionOperation(operationId)) {
        return false;
      }

      confirmSubmittedJob(response.status, 0, null);

      return true;
    } catch (error) {
      if (!isCurrentSubmissionOperation(operationId)) {
        return false;
      }

      if (!isAmbiguousMddsApiFailure(error)) {
        setSubmissionState("FAILED");
        return false;
      }

      return reconcileSubmission(jobId, operationId);
    }
  }, [
    apiClient,
    confirmSubmittedJob,
    isCurrentSubmissionOperation,
    jobId,
    reconcileSubmission,
    submissionState,
  ]);

  const resolveCancellationReconciliation = useCallback(
    (status: JobStatus) => {
      if (cancellationStateRef.current !== "RECONCILING") {
        return;
      }

      switch (status) {
        case "CANCEL_REQUESTED":
        case "CANCELLED":
          updateCancellationState("ACCEPTED");
          onJobCancellationAccepted?.();
          return;

        case "IN_PROGRESS":
        case "DRAFT":
          /*
           * The status request did not confirm that
           * cancellation was accepted. The user may
           * initiate another cancellation attempt.
           */
          updateCancellationState("FAILED");
          return;

        case "DONE":
        case "ERROR":
          /*
           * The job reached another terminal state.
           * Cancellation is no longer relevant.
           */
          updateCancellationState("IDLE");
          return;

        case "SUBMITTED":
        case "INPUTS_PREPARED":
          /*
           * These are earlier lifecycle states and may
           * be stale responses. Keep reconciling through
           * the next monitor poll.
           */
          return;
      }
    },
    [onJobCancellationAccepted, updateCancellationState],
  );

  const applyMonitorResponse = useCallback(
    (
      response: JobStatusResponseDTO,
      reconcilesCancellation: boolean,
    ): boolean => {
      const publicStatus = toPublicJobStatus(response.status);

      if (publicStatus !== null) {
        setJobStatus(publicStatus);
      }

      setJobProgress(response.progress);

      if (response.message !== null) {
        setJobMessage(response.message);
      }

      setMonitorFeedbackMessage(null);

      if (reconcilesCancellation) {
        resolveCancellationReconciliation(response.status);
      }

      if (!isTerminalJobStatus(response.status)) {
        return true;
      }

      setMonitorState("COMPLETED");
      return false;
    },
    [resolveCancellationReconciliation],
  );

  const failMonitoring = useCallback(
    (error: unknown, reconcilesCancellation: boolean) => {
      if (reconcilesCancellation) {
        updateCancellationState("FAILED");
      }

      setMonitorState("FAILED");

      setMonitorFeedbackMessage(
        getErrorMessage(error, "Unable to retrieve the current job status."),
      );
    },
    [updateCancellationState],
  );

  const pollJobStatus = useCallback(
    async (currentJobId: string, runId: number): Promise<boolean> => {
      /*
       * Only a status request started after entering
       * RECONCILING may resolve cancellation ambiguity.
       */
      const reconcilesCancellation =
        cancellationStateRef.current === "RECONCILING";

      try {
        const response = await apiClient.getJobStatus(currentJobId);

        if (runId !== monitorRunIdRef.current) {
          return false;
        }

        return applyMonitorResponse(response, reconcilesCancellation);
      } catch (error) {
        if (runId !== monitorRunIdRef.current) {
          return false;
        }

        failMonitoring(error, reconcilesCancellation);

        return false;
      }
    },
    [apiClient, applyMonitorResponse, failMonitoring],
  );

  const monitorUntilTerminal = useCallback(
    async (runId: number) => {
      if (!jobId) {
        setMonitorState("FAILED");
        setMonitorFeedbackMessage("Cannot monitor a job without a job ID.");
        return;
      }

      while (runId === monitorRunIdRef.current) {
        const shouldContinue = await pollJobStatus(jobId, runId);

        if (!shouldContinue) {
          return;
        }

        const delayCompleted = await waitForNextPoll(MONITOR_POLL_INTERVAL_MS);

        if (!delayCompleted) {
          return;
        }
      }
    },
    [jobId, pollJobStatus, waitForNextPoll],
  );

  const startMonitoring = useCallback(async () => {
    cancelPolling();

    const runId = ++monitorRunIdRef.current;

    setMonitorState("RUNNING");
    setMonitorFeedbackMessage(null);
    updateCancellationState("IDLE");

    await monitorUntilTerminal(runId);
  }, [cancelPolling, monitorUntilTerminal, updateCancellationState]);

  const retryMonitoring = useCallback(async () => {
    if (
      monitorState !== "FAILED" ||
      jobStatus === null ||
      jobStatus === "DONE" ||
      jobStatus === "ERROR" ||
      jobStatus === "CANCELLED"
    ) {
      return;
    }

    cancelPolling();
    const runId = ++monitorRunIdRef.current;
    setMonitorState("RUNNING");
    setMonitorFeedbackMessage("Retrying job monitoring.");
    await monitorUntilTerminal(runId);
  }, [cancelPolling, jobStatus, monitorState, monitorUntilTerminal]);

  const requestCancellation = useCallback(() => {
    const currentState = cancellationStateRef.current;

    if (
      jobStatus === "IN_PROGRESS" &&
      monitorState === "RUNNING" &&
      (currentState === "IDLE" || currentState === "FAILED")
    ) {
      updateCancellationState("CONFIRMING");
    }
  }, [jobStatus, monitorState, updateCancellationState]);

  const dismissCancellation = useCallback(() => {
    if (cancellationStateRef.current === "CONFIRMING") {
      updateCancellationState("IDLE");
    }
  }, [updateCancellationState]);

  const confirmCancellation = useCallback(async () => {
    if (
      cancellationStateRef.current !== "CONFIRMING" ||
      jobStatus !== "IN_PROGRESS" ||
      !jobId
    ) {
      updateCancellationState("FAILED");
      return;
    }

    const operationId = ++cancellationOperationIdRef.current;

    updateCancellationState("REQUESTING");

    try {
      const response = await apiClient.cancelJob(jobId);

      if (operationId !== cancellationOperationIdRef.current) {
        return;
      }

      if (response.status !== "CANCEL_REQUESTED") {
        updateCancellationState("FAILED");
        return;
      }

      updateCancellationState("ACCEPTED");

      onJobCancellationAccepted?.();
    } catch (error) {
      if (operationId !== cancellationOperationIdRef.current) {
        return;
      }

      updateCancellationState(
        isAmbiguousCancellationFailure(error) ? "RECONCILING" : "FAILED",
      );
    }
  }, [
    apiClient,
    jobId,
    jobStatus,
    onJobCancellationAccepted,
    updateCancellationState,
  ]);

  const reset = useCallback(() => {
    cancelPolling();
    submissionOperationIdRef.current += 1;
    cancellationOperationIdRef.current += 1;
    monitorRunIdRef.current += 1;

    setSubmissionState("IDLE");
    setJobStatus(null);
    setJobProgress(0);
    setJobMessage(INITIAL_JOB_MESSAGE);
    setMonitorState("IDLE");
    setMonitorFeedbackMessage(null);
    updateCancellationState("IDLE");
  }, [cancelPolling]);

  return {
    submissionState,
    jobStatus,
    jobProgress,
    jobMessage,
    monitorState,
    monitorFeedbackMessage,
    cancellationState,
    requestSubmissionConfirmation,
    dismissSubmissionConfirmation,
    submitJob,
    retrySubmissionReconciliation,
    startMonitoring,
    retryMonitoring,
    requestCancellation,
    dismissCancellation,
    confirmCancellation,
    reset,
  };
}

function toPublicJobStatus(status: JobStatus): PublicJobStatus | null {
  switch (status) {
    case "SUBMITTED":
    case "INPUTS_PREPARED":
    case "IN_PROGRESS":
    case "DONE":
    case "ERROR":
    case "CANCELLED":
      return status;

    case "DRAFT":
    case "CANCEL_REQUESTED":
      return null;
  }
}

function isTerminalJobStatus(status: JobStatus): boolean {
  return status === "DONE" || status === "ERROR" || status === "CANCELLED";
}

interface OutputWorkflowProps {
  apiClient: MddsApiClient;
  artifactTransferClient: ArtifactTransferClient;
  jobId: string | null;
  onOutputDownloaded?: (fileName: string) => void;
}

export interface OutputWorkflow {
  downloadState: DownloadState;
  startDownload: () => Promise<void>;
  cancelDownload: () => void;
  reset: () => void;
}

function triggerFileDownload(blob: Blob, fileName: string): void {
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");

  anchor.href = objectUrl;
  anchor.download = fileName;
  anchor.style.display = "none";

  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();

  window.setTimeout(() => {
    URL.revokeObjectURL(objectUrl);
  }, 0);
}

export function useOutputWorkflow({
  apiClient,
  artifactTransferClient,
  jobId,
  onOutputDownloaded,
}: Readonly<OutputWorkflowProps>): OutputWorkflow {
  const [downloadState, setDownloadState] = useState<DownloadState>("IDLE");

  const abortControllerRef = useRef<AbortController | null>(null);

  const abortActiveDownload = useCallback(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
  }, []);

  useEffect(() => abortActiveDownload, [abortActiveDownload]);

  const startDownload = useCallback(async () => {
    abortActiveDownload();

    if (!jobId) {
      setDownloadState("FAILED_DOWNLOAD");
      return;
    }

    const abortController = new AbortController();
    abortControllerRef.current = abortController;
    setDownloadState("REQUESTING_URL");

    try {
      const response = await apiClient.requestOutputDownloadUrl(
        jobId,
        SOLUTION_OUTPUT_SLOT,
      );

      if (abortController.signal.aborted) {
        return;
      }

      setDownloadState("DOWNLOADING");

      const blob = await artifactTransferClient.download(
        response.downloadUrl,
        abortController.signal,
      );

      if (abortController.signal.aborted) {
        return;
      }

      triggerFileDownload(blob, SOLUTION_FILE_NAME);
      setDownloadState("DOWNLOADED");
      onOutputDownloaded?.(SOLUTION_FILE_NAME);
    } catch (error) {
      if (!isAbortError(error)) {
        setDownloadState("FAILED_DOWNLOAD");
      }
    } finally {
      if (abortControllerRef.current === abortController) {
        abortControllerRef.current = null;
      }
    }
  }, [
    abortActiveDownload,
    apiClient,
    artifactTransferClient,
    jobId,
    onOutputDownloaded,
  ]);

  const cancelDownload = useCallback(() => {
    if (downloadState !== "REQUESTING_URL" && downloadState !== "DOWNLOADING") {
      return;
    }

    abortActiveDownload();
    setDownloadState("CANCELLED_DOWNLOAD");
  }, [abortActiveDownload, downloadState]);

  const reset = useCallback(() => {
    abortActiveDownload();
    setDownloadState("IDLE");
  }, [abortActiveDownload]);

  return {
    downloadState,
    startDownload,
    cancelDownload,
    reset,
  };
}
