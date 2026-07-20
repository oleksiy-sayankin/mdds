/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

/** @vitest-environment jsdom */

import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MddsApiClient } from "@/api/MddsApiClient";
import type { ArtifactTransferClient } from "@/artifacts/ArtifactTransferClient";
import {
  useExecutionWorkflow,
  useOutputWorkflow,
  useParameterWorkflow,
  useUploadWorkflow,
} from "./useJobWizardWorkflows";

import type {
  CancelJobResponseDTO,
  JobStatusResponseDTO,
  SubmitJobResponseDTO,
} from "@/api/MddsRestTypes";

import { HttpError } from "@/api/HttpError";

const files = {
  matrix: new File(["1,0\n0,1"], "matrix.csv", {
    type: "text/csv",
  }),
  rhs: new File(["1\n2"], "rhs.csv", {
    type: "text/csv",
  }),
};

function createApiClient(
  overrides: Partial<MddsApiClient> = {},
): MddsApiClient {
  return {
    createOrReuseDraftJob: vi.fn(async () => ({
      jobId: "job-1",
    })),
    requestInputUploadUrl: vi.fn(async (_jobId, inputSlot) => ({
      jobId: "job-1",
      uploadUrl: `mock://upload/${inputSlot}`,
      expiresAt: "2026-07-19T12:15:00.000Z",
    })),
    patchJobParams: vi.fn(async () => undefined),
    submitJob: vi.fn(
      async (_jobId: string): Promise<SubmitJobResponseDTO> => ({
        jobId: "job-1",
        status: "SUBMITTED",
      }),
    ),
    getJobStatus: vi.fn(async () => createStatusResponse("SUBMITTED", 0)),
    cancelJob: vi.fn(
      async (_jobId: string): Promise<CancelJobResponseDTO> => ({
        jobId: "job-1",
        status: "CANCEL_REQUESTED",
      }),
    ),
    requestOutputDownloadUrl: vi.fn(async () => ({
      jobId: "job-1",
      downloadUrl: "mock://download/solution",
      expiresAt: "2026-07-19T12:15:00.000Z",
    })),
    ...overrides,
  };
}

function createArtifactTransferClient(
  overrides: Partial<ArtifactTransferClient> = {},
): ArtifactTransferClient {
  return {
    upload: vi.fn(async () => undefined),
    download: vi.fn(async () => new Blob(["solution"])),
    ...overrides,
  };
}

function createStatusResponse(
  status: JobStatusResponseDTO["status"],
  progress: number,
  message = `Status is ${status}.`,
): JobStatusResponseDTO {
  return {
    jobId: "job-1",
    jobType: "solving_slae",
    status,
    progress,
    message,
    createdAt: "2026-07-19T12:00:00.000Z",
    submittedAt: "2026-07-19T12:01:00.000Z",
    startedAt:
      status === "IN_PROGRESS" || status === "DONE"
        ? "2026-07-19T12:02:00.000Z"
        : null,
    finishedAt:
      status === "DONE" || status === "ERROR" || status === "CANCELLED"
        ? "2026-07-19T12:03:00.000Z"
        : null,
  };
}

function createHttpError(
  status: number,
  statusText: string,
  operation = "submit",
): HttpError {
  return new HttpError({
    method: "POST",
    url: `http://localhost/api/v1/jobs/job-1/` + operation,
    status,
    statusText,
    responseBody: null,
    message:
      `POST /api/v1/jobs/job-1/${operation} ` +
      `failed: HTTP ${status} ${statusText}`,
  });
}

async function flushPromises() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;

  let reject!: (reason?: unknown) => void;

  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

async function advanceTimers(milliseconds: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(milliseconds);
  });
}

describe("useUploadWorkflow", () => {
  it("creates a draft and uploads selected input files", async () => {
    const apiClient = createApiClient();
    const artifactTransferClient = createArtifactTransferClient();

    const { result } = renderHook(() =>
      useUploadWorkflow({
        apiClient,
        artifactTransferClient,
        jobType: "solving_slae",
        uploadSessionId: "session-1",
        files,
      }),
    );

    act(() => {
      result.current.selectFile("matrix", files.matrix);
      result.current.selectFile("rhs", files.rhs);
    });

    await act(async () => {
      await result.current.startUpload();
    });

    expect(apiClient.createOrReuseDraftJob).toHaveBeenCalledWith(
      "solving_slae",
      "session-1",
    );

    expect(apiClient.requestInputUploadUrl).toHaveBeenCalledTimes(2);
    expect(artifactTransferClient.upload).toHaveBeenCalledTimes(2);
    expect(result.current.jobId).toBe("job-1");
    expect(result.current.inputSlotStates).toEqual({
      matrix: "UPLOADED",
      rhs: "UPLOADED",
    });
    expect(result.current.allRequiredFilesUploaded).toBe(true);
    expect(result.current.uploadManagerState).toBe("COMPLETED");
  });

  it("stops an active artifact upload", async () => {
    const apiClient = createApiClient();

    const artifactTransferClient = createArtifactTransferClient({
      upload: vi.fn(
        (_url, _file, signal) =>
          new Promise<void>((_resolve, reject) => {
            signal?.addEventListener(
              "abort",
              () => {
                reject(
                  new DOMException("The operation was aborted.", "AbortError"),
                );
              },
              { once: true },
            );
          }),
      ),
    });

    const { result } = renderHook(() =>
      useUploadWorkflow({
        apiClient,
        artifactTransferClient,
        jobType: "solving_slae",
        uploadSessionId: "session-1",
        files,
      }),
    );

    let uploadPromise!: Promise<void>;

    act(() => {
      uploadPromise = result.current.startUpload();
    });

    await flushPromises();

    act(() => {
      result.current.stopUploading();
    });

    await act(async () => {
      await uploadPromise;
    });

    expect(result.current.uploadManagerState).toBe("STOPPED_BY_USER");
    expect(result.current.allRequiredFilesUploaded).toBe(false);
  });

  it("resets draft and selected input state", async () => {
    const { result } = renderHook(() =>
      useUploadWorkflow({
        apiClient: createApiClient(),
        artifactTransferClient: createArtifactTransferClient(),
        jobType: "solving_slae",
        uploadSessionId: "session-1",
        files,
      }),
    );

    await act(async () => {
      await result.current.startUpload();
    });

    act(() => {
      result.current.reset();
    });

    expect(result.current.jobId).toBeNull();
    expect(result.current.uploadManagerState).toBe("IDLE");
    expect(result.current.inputSlotStates).toEqual({
      matrix: "EMPTY",
      rhs: "EMPTY",
    });
  });
});

describe("useParameterWorkflow", () => {
  it("updates the default parameters through the API", async () => {
    const apiClient = createApiClient();

    const { result } = renderHook(() =>
      useParameterWorkflow({
        apiClient,
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.ensureParametersUpdated();
    });

    expect(result.current.parameterUpdateState).toBe("UPDATING");
    await flushPromises();

    expect(apiClient.patchJobParams).toHaveBeenCalledWith("job-1", {
      solvingMethod: "numpy_exact_solver",
    });
    expect(result.current.parameterUpdateState).toBe("UPDATED");
  });

  it("updates a changed solving method and resets it", async () => {
    const apiClient = createApiClient();

    const { result } = renderHook(() =>
      useParameterWorkflow({
        apiClient,
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.changeSolvingMethod("numpy_lstsq_solver");
    });

    await flushPromises();

    expect(apiClient.patchJobParams).toHaveBeenCalledWith("job-1", {
      solvingMethod: "numpy_lstsq_solver",
    });
    expect(result.current.parameterUpdateState).toBe("UPDATED");

    act(() => {
      result.current.reset();
    });

    expect(result.current.solvingMethod).toBe("numpy_exact_solver");
    expect(result.current.parameterUpdateState).toBe("PENDING");
  });
});

describe("useExecutionWorkflow", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it("reconciles an ambiguous cancellation through monitor polling", async () => {
    const onJobCancellationAccepted = vi.fn();

    const cancelJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi
      .fn()
      .mockResolvedValueOnce(
        createStatusResponse(
          "IN_PROGRESS",
          20,
          "Worker is processing the job.",
        ),
      )
      .mockResolvedValueOnce(
        createStatusResponse(
          "CANCEL_REQUESTED",
          20,
          "Cancellation request was accepted.",
        ),
      )
      .mockResolvedValueOnce(
        createStatusResponse("CANCELLED", 20, "Job was cancelled."),
      );

    const apiClient = createApiClient({
      cancelJob,
      getJobStatus,
    });

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient,
        jobId: "job-1",
        onJobCancellationAccepted,
      }),
    );

    let monitoringPromise!: Promise<void>;

    act(() => {
      monitoringPromise = result.current.startMonitoring();
    });

    await flushPromises();

    expect(result.current.jobStatus).toBe("IN_PROGRESS");

    act(() => {
      result.current.requestCancellation();
    });

    await act(async () => {
      await result.current.confirmCancellation();
    });

    expect(result.current.cancellationState).toBe("RECONCILING");

    /*
     * confirmCancellation itself must not perform
     * an additional status request.
     */
    expect(getJobStatus).toHaveBeenCalledOnce();

    expect(cancelJob).toHaveBeenCalledOnce();

    /*
     * The existing monitor owns the reconciliation
     * GET and observes CANCEL_REQUESTED.
     */
    await advanceTimers(700);

    expect(result.current.cancellationState).toBe("ACCEPTED");

    expect(result.current.jobStatus).toBe("IN_PROGRESS");

    expect(onJobCancellationAccepted).toHaveBeenCalledOnce();

    /*
     * Monitoring continues until terminal CANCELLED.
     */
    await advanceTimers(700);
    await monitoringPromise;

    expect(result.current.jobStatus).toBe("CANCELLED");

    expect(result.current.monitorState).toBe("COMPLETED");

    expect(cancelJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledTimes(3);
  });

  it("reconciles HTTP 409 cancellation through monitor polling", async () => {
    const onJobCancellationAccepted = vi.fn();

    const cancelJob = vi.fn(async () => {
      throw createHttpError(409, "Conflict", "cancel");
    });

    const getJobStatus = vi
      .fn()
      /*
       * Initial monitoring confirms that cancellation
       * is currently allowed.
       */
      .mockResolvedValueOnce(
        createStatusResponse(
          "IN_PROGRESS",
          20,
          "Worker is processing the job.",
        ),
      )
      /*
       * The next existing monitoring poll reconciles
       * the ambiguous HTTP 409 result.
       */
      .mockResolvedValueOnce(
        createStatusResponse(
          "CANCEL_REQUESTED",
          20,
          "Cancellation request was accepted.",
        ),
      )
      /*
       * Monitoring continues until the worker publishes
       * the terminal cancellation status.
       */
      .mockResolvedValueOnce(
        createStatusResponse("CANCELLED", 20, "Job was cancelled."),
      );

    const apiClient = createApiClient({
      cancelJob,
      getJobStatus,
    });

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient,
        jobId: "job-1",
        onJobCancellationAccepted,
      }),
    );

    let monitoringPromise!: Promise<void>;

    act(() => {
      monitoringPromise = result.current.startMonitoring();
    });

    await flushPromises();

    expect(result.current.jobStatus).toBe("IN_PROGRESS");
    expect(getJobStatus).toHaveBeenCalledOnce();

    act(() => {
      result.current.requestCancellation();
    });

    await act(async () => {
      await result.current.confirmCancellation();
    });

    /*
     * HTTP 409 is not treated as a definite cancellation
     * failure because the job state may have changed
     * concurrently on the server.
     */
    expect(result.current.cancellationState).toBe("RECONCILING");

    expect(cancelJob).toHaveBeenCalledOnce();
    expect(cancelJob).toHaveBeenCalledWith("job-1");

    /*
     * confirmCancellation must not repeat POST /cancel
     * or initiate a parallel status request. The existing
     * monitor owns reconciliation.
     */
    expect(getJobStatus).toHaveBeenCalledOnce();

    await advanceTimers(700);

    expect(result.current.cancellationState).toBe("ACCEPTED");
    expect(result.current.jobStatus).toBe("IN_PROGRESS");
    expect(onJobCancellationAccepted).toHaveBeenCalledOnce();

    expect(cancelJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledTimes(2);

    await advanceTimers(700);
    await monitoringPromise;

    expect(result.current.jobStatus).toBe("CANCELLED");
    expect(result.current.monitorState).toBe("COMPLETED");

    /*
     * The cancellation POST is never retried automatically.
     */
    expect(cancelJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledTimes(3);
  });

  it("fails cancellation directly after a definite HTTP error", async () => {
    const cancelJob = vi.fn(async () => {
      throw createHttpError(422, "Unprocessable Entity", "cancel");
    });

    const getJobStatus = vi
      .fn()
      .mockResolvedValueOnce(createStatusResponse("IN_PROGRESS", 20))
      .mockResolvedValueOnce(createStatusResponse("DONE", 100));

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          cancelJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    let monitoringPromise!: Promise<void>;

    act(() => {
      monitoringPromise = result.current.startMonitoring();
    });

    await flushPromises();

    act(() => {
      result.current.requestCancellation();
    });

    await act(async () => {
      await result.current.confirmCancellation();
    });

    expect(result.current.cancellationState).toBe("FAILED");

    expect(cancelJob).toHaveBeenCalledOnce();

    /*
     * No reconciliation GET was initiated by
     * confirmCancellation.
     */
    expect(getJobStatus).toHaveBeenCalledOnce();

    await advanceTimers(700);
    await monitoringPromise;
  });

  it("confirms and submits a job through the API", async () => {
    const onJobSubmitted = vi.fn();
    const apiClient = createApiClient();

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient,
        jobId: "job-1",
        onJobSubmitted,
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    let submitted = false;

    await act(async () => {
      submitted = await result.current.submitJob();
    });

    expect(submitted).toBe(true);
    expect(apiClient.submitJob).toHaveBeenCalledWith("job-1");
    expect(result.current.submissionState).toBe("SUBMITTED");
    expect(result.current.jobStatus).toBe("SUBMITTED");
    expect(onJobSubmitted).toHaveBeenCalledOnce();
  });

  it("monitors API statuses until a terminal status", async () => {
    const getJobStatus = vi
      .fn()
      .mockResolvedValueOnce(createStatusResponse("SUBMITTED", 0))
      .mockResolvedValueOnce(createStatusResponse("IN_PROGRESS", 40))
      .mockResolvedValueOnce(
        createStatusResponse("DONE", 100, "Job completed successfully."),
      );

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({ getJobStatus }),
        jobId: "job-1",
      }),
    );

    let monitoringPromise!: Promise<void>;

    act(() => {
      monitoringPromise = result.current.startMonitoring();
    });

    await flushPromises();
    expect(result.current.jobStatus).toBe("SUBMITTED");

    await advanceTimers(700);
    expect(result.current.jobStatus).toBe("IN_PROGRESS");
    expect(result.current.jobProgress).toBe(40);

    await advanceTimers(700);
    await monitoringPromise;

    expect(result.current.jobStatus).toBe("DONE");
    expect(result.current.monitorState).toBe("COMPLETED");
    expect(result.current.jobMessage).toBe("Job completed successfully.");
  });

  it("keeps monitoring until CANCELLED is returned by status API", async () => {
    const onJobCancellationAccepted = vi.fn();

    const getJobStatus = vi
      .fn()
      .mockResolvedValueOnce(createStatusResponse("SUBMITTED", 0))
      .mockResolvedValueOnce(createStatusResponse("IN_PROGRESS", 20))
      .mockResolvedValueOnce(
        createStatusResponse("CANCELLED", 20, "Job was cancelled."),
      );

    const apiClient = createApiClient({ getJobStatus });

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient,
        jobId: "job-1",
        onJobCancellationAccepted,
      }),
    );

    let monitoringPromise!: Promise<void>;

    act(() => {
      monitoringPromise = result.current.startMonitoring();
    });

    await flushPromises();
    await advanceTimers(700);

    expect(result.current.jobStatus).toBe("IN_PROGRESS");

    act(() => {
      result.current.requestCancellation();
    });

    await act(async () => {
      await result.current.confirmCancellation();
    });

    expect(result.current.cancellationState).toBe("ACCEPTED");
    expect(result.current.jobStatus).toBe("IN_PROGRESS");
    expect(result.current.monitorState).toBe("RUNNING");
    expect(onJobCancellationAccepted).toHaveBeenCalledOnce();

    await advanceTimers(700);
    await monitoringPromise;

    expect(result.current.jobStatus).toBe("CANCELLED");
    expect(result.current.monitorState).toBe("COMPLETED");
  });

  it("moves monitoring to FAILED and supports retry", async () => {
    const getJobStatus = vi
      .fn()
      .mockResolvedValueOnce(
        createStatusResponse("SUBMITTED", 0, "Job is waiting for a worker."),
      )
      .mockRejectedValueOnce(new Error("Status endpoint unavailable."))
      .mockResolvedValueOnce(
        createStatusResponse(
          "IN_PROGRESS",
          50,
          "Worker is processing the job.",
        ),
      )
      .mockResolvedValueOnce(
        createStatusResponse("DONE", 100, "Job completed successfully."),
      );

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    let monitoringPromise!: Promise<void>;

    act(() => {
      monitoringPromise = result.current.startMonitoring();
    });

    /*
     * The first status request succeeds.
     */
    await flushPromises();

    expect(result.current.monitorState).toBe("RUNNING");

    expect(result.current.jobStatus).toBe("SUBMITTED");

    /*
     * The next poll fails.
     */
    await advanceTimers(700);
    await monitoringPromise;

    expect(result.current.monitorState).toBe("FAILED");

    expect(result.current.jobStatus).toBe("SUBMITTED");

    expect(result.current.jobMessage).toBe("Job is waiting for a worker.");

    expect(result.current.monitorFeedbackMessage).toBe(
      "Status endpoint unavailable.",
    );

    /*
     * The user retries monitoring of the same job.
     */
    let retryPromise!: Promise<void>;

    act(() => {
      retryPromise = result.current.retryMonitoring();
    });

    await flushPromises();

    expect(result.current.monitorState).toBe("RUNNING");

    expect(result.current.jobStatus).toBe("IN_PROGRESS");

    expect(result.current.jobMessage).toBe("Worker is processing the job.");

    expect(result.current.monitorFeedbackMessage).toBeNull();

    expect(result.current.jobProgress).toBe(50);

    /*
     * The following poll observes the terminal
     * public status.
     */
    await advanceTimers(700);
    await retryPromise;

    expect(result.current.jobStatus).toBe("DONE");

    expect(result.current.monitorState).toBe("COMPLETED");

    expect(getJobStatus).toHaveBeenCalledTimes(4);
  });

  it("rejects cancellation outside confirmation of a running job", async () => {
    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient(),
        jobId: "job-1",
      }),
    );

    await act(async () => {
      await result.current.confirmCancellation();
    });

    expect(result.current.cancellationState).toBe("FAILED");
  });

  it("resets execution state", async () => {
    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient(),
        jobId: "job-1",
      }),
    );

    act(() => {
      void result.current.startMonitoring();
      result.current.reset();
    });

    expect(result.current.submissionState).toBe("IDLE");
    expect(result.current.jobStatus).toBeNull();
    expect(result.current.jobProgress).toBe(0);
    expect(result.current.monitorState).toBe("IDLE");
    expect(result.current.cancellationState).toBe("IDLE");
  });

  it("fails submission without reconciliation after a 4xx response", async () => {
    const submitJob = vi.fn(async () => {
      throw createHttpError(422, "Unprocessable Entity");
    });

    const getJobStatus = vi.fn(async () => createStatusResponse("DRAFT", 0));

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    let submitted = true;

    await act(async () => {
      submitted = await result.current.submitJob();
    });

    expect(submitted).toBe(false);
    expect(result.current.submissionState).toBe("FAILED");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).not.toHaveBeenCalled();
  });

  it("reconciles an ambiguous submission failure", async () => {
    const statusDeferred = createDeferred<JobStatusResponseDTO>();

    const submitJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi.fn(() => statusDeferred.promise);

    const onJobSubmitted = vi.fn();

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
        onJobSubmitted,
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    let submissionPromise!: Promise<boolean>;

    act(() => {
      submissionPromise = result.current.submitJob();
    });

    await flushPromises();

    expect(result.current.submissionState).toBe("RECONCILING");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledOnce();

    statusDeferred.resolve(
      createStatusResponse("IN_PROGRESS", 40, "Worker is processing the job."),
    );

    let submitted = false;

    await act(async () => {
      submitted = await submissionPromise;
    });

    expect(submitted).toBe(true);
    expect(result.current.submissionState).toBe("SUBMITTED");
    expect(result.current.jobStatus).toBe("IN_PROGRESS");
    expect(result.current.jobProgress).toBe(40);
    expect(onJobSubmitted).toHaveBeenCalledOnce();

    /*
     * The original POST must not be retried.
     */
    expect(submitJob).toHaveBeenCalledOnce();
  });

  it("fails submission when reconciliation confirms DRAFT", async () => {
    const submitJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi.fn(async () =>
      createStatusResponse("DRAFT", 0, "Draft job was not submitted."),
    );

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    let submitted = true;

    await act(async () => {
      submitted = await result.current.submitJob();
    });

    expect(submitted).toBe(false);
    expect(result.current.submissionState).toBe("FAILED");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledOnce();
  });

  it("reconciles submission after a 5xx response", async () => {
    const submitJob = vi.fn(async () => {
      throw createHttpError(503, "Service Unavailable");
    });

    const getJobStatus = vi.fn(async () =>
      createStatusResponse("SUBMITTED", 0, "Job was submitted."),
    );

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    let submitted = false;

    await act(async () => {
      submitted = await result.current.submitJob();
    });

    expect(submitted).toBe(true);
    expect(result.current.submissionState).toBe("SUBMITTED");
    expect(result.current.jobStatus).toBe("SUBMITTED");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledOnce();
  });

  it("moves submission to UNCONFIRMED when reconciliation cannot retrieve status", async () => {
    const submitJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi.fn(async () => {
      throw new TypeError("Status endpoint is unavailable");
    });

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    let submitted = true;

    await act(async () => {
      submitted = await result.current.submitJob();
    });

    expect(submitted).toBe(false);
    expect(result.current.submissionState).toBe("UNCONFIRMED");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledOnce();
  });

  it("retries submission reconciliation without repeating POST and confirms submission", async () => {
    const submitJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi
      .fn()
      .mockRejectedValueOnce(new TypeError("Status endpoint is unavailable"))
      .mockResolvedValueOnce(
        createStatusResponse(
          "IN_PROGRESS",
          40,
          "Worker is processing the job.",
        ),
      );

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    await act(async () => {
      await result.current.submitJob();
    });

    expect(result.current.submissionState).toBe("UNCONFIRMED");

    let submitted = false;

    await act(async () => {
      submitted = await result.current.retrySubmissionReconciliation();
    });

    expect(submitted).toBe(true);
    expect(result.current.submissionState).toBe("SUBMITTED");
    expect(result.current.jobStatus).toBe("IN_PROGRESS");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledTimes(2);
  });

  it("moves submission from UNCONFIRMED to FAILED when retry confirms DRAFT", async () => {
    const submitJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi
      .fn()
      .mockRejectedValueOnce(new TypeError("Status endpoint is unavailable"))
      .mockResolvedValueOnce(
        createStatusResponse("DRAFT", 0, "Draft job was not submitted."),
      );

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    await act(async () => {
      await result.current.submitJob();
    });

    expect(result.current.submissionState).toBe("UNCONFIRMED");

    await act(async () => {
      await result.current.retrySubmissionReconciliation();
    });

    expect(result.current.submissionState).toBe("FAILED");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledTimes(2);
  });

  it("keeps submission UNCONFIRMED when reconciliation retry also fails", async () => {
    const submitJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi.fn(async () => {
      throw new TypeError("Status endpoint is unavailable");
    });

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    await act(async () => {
      await result.current.submitJob();
    });

    await act(async () => {
      await result.current.retrySubmissionReconciliation();
    });

    expect(result.current.submissionState).toBe("UNCONFIRMED");

    expect(submitJob).toHaveBeenCalledOnce();
    expect(getJobStatus).toHaveBeenCalledTimes(2);
  });

  it("starts without a confirmed public job status", () => {
    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient(),
        jobId: "job-1",
      }),
    );

    expect(result.current.jobStatus).toBeNull();

    expect(result.current.monitorState).toBe("IDLE");
  });

  it("publishes INPUTS_PREPARED as the current public job status", async () => {
    const getJobStatus = vi
      .fn()
      .mockResolvedValueOnce(
        createStatusResponse(
          "INPUTS_PREPARED",
          0,
          "Worker prepared the job inputs.",
        ),
      )
      .mockResolvedValueOnce(
        createStatusResponse("DONE", 100, "Job completed successfully."),
      );

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          getJobStatus,
        }),
        jobId: "job-1",
      }),
    );

    let monitoringPromise!: Promise<void>;

    act(() => {
      monitoringPromise = result.current.startMonitoring();
    });

    /*
     * The first request does not require advancing
     * the polling timer.
     */
    await flushPromises();

    expect(result.current.jobStatus).toBe("INPUTS_PREPARED");

    expect(result.current.jobProgress).toBe(0);

    expect(result.current.jobMessage).toBe("Worker prepared the job inputs.");

    expect(result.current.monitorState).toBe("RUNNING");

    /*
     * Allow the next polling iteration to observe
     * a terminal status and finish the workflow.
     */
    await advanceTimers(700);
    await monitoringPromise;

    expect(result.current.jobStatus).toBe("DONE");

    expect(result.current.monitorState).toBe("COMPLETED");

    expect(getJobStatus).toHaveBeenCalledTimes(2);
  });

  it("accepts INPUTS_PREPARED during submission reconciliation", async () => {
    const submitJob = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });

    const getJobStatus = vi.fn(async () =>
      createStatusResponse(
        "INPUTS_PREPARED",
        0,
        "Worker prepared the job inputs.",
      ),
    );

    const onJobSubmitted = vi.fn();

    const { result } = renderHook(() =>
      useExecutionWorkflow({
        apiClient: createApiClient({
          submitJob,
          getJobStatus,
        }),
        jobId: "job-1",
        onJobSubmitted,
      }),
    );

    act(() => {
      result.current.requestSubmissionConfirmation();
    });

    expect(result.current.submissionState).toBe("CONFIRMING");

    let submitted = false;

    await act(async () => {
      submitted = await result.current.submitJob();
    });

    expect(submitted).toBe(true);

    expect(result.current.submissionState).toBe("SUBMITTED");

    expect(result.current.jobStatus).toBe("INPUTS_PREPARED");

    expect(result.current.jobProgress).toBe(0);

    expect(result.current.jobMessage).toBe("Worker prepared the job inputs.");

    expect(submitJob).toHaveBeenCalledOnce();

    expect(getJobStatus).toHaveBeenCalledOnce();

    expect(onJobSubmitted).toHaveBeenCalledOnce();
  });
});

describe("useOutputWorkflow", () => {
  beforeEach(() => {
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      value: vi.fn(() => "blob:solution"),
    });

    Object.defineProperty(URL, "revokeObjectURL", {
      configurable: true,
      value: vi.fn(),
    });

    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(
      () => undefined,
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("requests an output URL and downloads the artifact", async () => {
    const onOutputDownloaded = vi.fn();
    const apiClient = createApiClient();
    const artifactTransferClient = createArtifactTransferClient();

    const { result } = renderHook(() =>
      useOutputWorkflow({
        apiClient,
        artifactTransferClient,
        jobId: "job-1",
        onOutputDownloaded,
      }),
    );

    await act(async () => {
      await result.current.startDownload();
    });

    expect(apiClient.requestOutputDownloadUrl).toHaveBeenCalledWith(
      "job-1",
      "solution",
    );
    expect(artifactTransferClient.download).toHaveBeenCalledWith(
      "mock://download/solution",
      expect.any(AbortSignal),
    );
    expect(result.current.downloadState).toBe("DOWNLOADED");
    expect(onOutputDownloaded).toHaveBeenCalledWith("solution.csv");
  });

  it("cancels an active artifact download", async () => {
    const artifactTransferClient = createArtifactTransferClient({
      download: vi.fn(
        (_url, signal) =>
          new Promise<Blob>((_resolve, reject) => {
            signal?.addEventListener(
              "abort",
              () => {
                reject(
                  new DOMException("The operation was aborted.", "AbortError"),
                );
              },
              { once: true },
            );
          }),
      ),
    });

    const { result } = renderHook(() =>
      useOutputWorkflow({
        apiClient: createApiClient(),
        artifactTransferClient,
        jobId: "job-1",
      }),
    );

    let downloadPromise!: Promise<void>;

    act(() => {
      downloadPromise = result.current.startDownload();
    });

    await flushPromises();

    act(() => {
      result.current.cancelDownload();
    });

    await act(async () => {
      await downloadPromise;
    });

    expect(result.current.downloadState).toBe("CANCELLED_DOWNLOAD");
  });
});
