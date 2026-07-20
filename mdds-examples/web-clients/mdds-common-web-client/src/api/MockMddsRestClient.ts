/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import type { MddsApiClient } from "./MddsApiClient";
import type {
  CancelJobResponseDTO,
  CreateJobResponseDTO,
  JobOutputResponseDTO,
  JobParameterValue,
  JobStatus,
  JobStatusResponseDTO,
  JobUploadUrlResponseDTO,
  PatchJobParamsRequest,
  SubmitJobResponseDTO,
} from "./MddsRestTypes";

const DEFAULT_PROGRESS_SEQUENCE = [10, 20, 40, 60, 80, 100] as const;
const URL_TTL_MS = 15 * 60 * 1000;

export type MockMddsApiOperation =
  | "createOrReuseDraftJob"
  | "requestInputUploadUrl"
  | "patchJobParams"
  | "submitJob"
  | "getJobStatus"
  | "cancelJob"
  | "requestOutputDownloadUrl";

export interface MockMddsRestClientConfig {
  delayMs?: number;
  now?: () => Date;
  jobIdFactory?: () => string;
  progressSequence?: readonly number[];
}

interface MockJob {
  jobId: string;
  jobType: string;
  status: JobStatus;
  progress: number;
  message: string | null;
  createdAt: string;
  submittedAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  params: Record<string, JobParameterValue>;
  statusReadCount: number;
  cancellationRequested: boolean;
}

/**
 * Stateful in-memory implementation of the MDDS REST API contract.
 *
 * It models server-owned job lifecycle transitions and intentionally keeps
 * those transitions outside React workflow hooks.
 */
export class MockMddsRestClient implements MddsApiClient {
  private readonly delayMs: number;
  private readonly now: () => Date;
  private readonly jobIdFactory: () => string;
  private readonly progressSequence: readonly number[];
  private readonly jobs = new Map<string, MockJob>();
  private readonly jobIdsByUploadSession = new Map<string, string>();
  private readonly pendingFailures = new Map<MockMddsApiOperation, Error[]>();

  constructor(config: MockMddsRestClientConfig = {}) {
    this.delayMs = config.delayMs ?? 350;
    this.now = config.now ?? (() => new Date());
    this.jobIdFactory = config.jobIdFactory ?? createMockJobId;
    this.progressSequence =
      config.progressSequence ?? DEFAULT_PROGRESS_SEQUENCE;
  }

  failNext(
    operation: MockMddsApiOperation,
    error: Error = new Error(`Mock ${operation} failure.`),
  ): void {
    const failures = this.pendingFailures.get(operation) ?? [];
    failures.push(error);
    this.pendingFailures.set(operation, failures);
  }

  async createOrReuseDraftJob(
    jobType: string,
    uploadSessionId: string,
  ): Promise<CreateJobResponseDTO> {
    await this.beforeOperation("createOrReuseDraftJob");

    const existingJobId = this.jobIdsByUploadSession.get(uploadSessionId);

    if (existingJobId) {
      return { jobId: existingJobId };
    }

    const createdAt = this.now().toISOString();
    const jobId = this.jobIdFactory();

    this.jobs.set(jobId, {
      jobId,
      jobType,
      status: "DRAFT",
      progress: 0,
      message: "Draft job was created.",
      createdAt,
      submittedAt: null,
      startedAt: null,
      finishedAt: null,
      params: {},
      statusReadCount: 0,
      cancellationRequested: false,
    });

    this.jobIdsByUploadSession.set(uploadSessionId, jobId);
    return { jobId };
  }

  async requestInputUploadUrl(
    jobId: string,
    inputSlot: string,
  ): Promise<JobUploadUrlResponseDTO> {
    await this.beforeOperation("requestInputUploadUrl");

    const job = this.requireJob(jobId);
    this.requireEditableJob(job);

    return {
      jobId,
      uploadUrl:
        `mock://mdds/uploads/` +
        `${encodeURIComponent(jobId)}/` +
        `${encodeURIComponent(inputSlot)}`,
      expiresAt: this.expiresAt(),
    };
  }

  async patchJobParams(
    jobId: string,
    params: PatchJobParamsRequest,
  ): Promise<void> {
    await this.beforeOperation("patchJobParams");

    const job = this.requireJob(jobId);
    this.requireEditableJob(job);
    job.params = {
      ...job.params,
      ...params,
    };
  }

  async submitJob(jobId: string): Promise<SubmitJobResponseDTO> {
    await this.beforeOperation("submitJob");

    const job = this.requireJob(jobId);
    this.requireEditableJob(job);

    job.status = "SUBMITTED";
    job.progress = 0;
    job.message = "Job was submitted and is waiting to start.";
    job.submittedAt = this.now().toISOString();
    job.statusReadCount = 0;

    return {
      jobId,
      status: "SUBMITTED",
    };
  }

  async getJobStatus(jobId: string): Promise<JobStatusResponseDTO> {
    await this.beforeOperation("getJobStatus");

    const job = this.requireJob(jobId);
    this.advanceLifecycle(job);
    return this.toStatusResponse(job);
  }

  async cancelJob(jobId: string): Promise<CancelJobResponseDTO> {
    await this.beforeOperation("cancelJob");

    const job = this.requireJob(jobId);

    if (job.status !== "IN_PROGRESS") {
      throw new Error(`Cannot cancel job ${jobId} from status ${job.status}.`);
    }

    job.cancellationRequested = true;
    job.message =
      "Cancellation request was accepted. Waiting for the job to stop.";

    return {
      jobId,
      status: "CANCEL_REQUESTED",
    };
  }

  async requestOutputDownloadUrl(
    jobId: string,
    outputSlot: string,
  ): Promise<JobOutputResponseDTO> {
    await this.beforeOperation("requestOutputDownloadUrl");

    const job = this.requireJob(jobId);

    if (job.status !== "DONE") {
      throw new Error(
        `Cannot download outputs for job ${jobId} from status ` +
          `${job.status}.`,
      );
    }

    return {
      jobId,
      downloadUrl: `mock://mdds/downloads/${encodeURIComponent(
        jobId,
      )}/${encodeURIComponent(outputSlot)}`,
      expiresAt: this.expiresAt(),
    };
  }

  private async beforeOperation(
    operation: MockMddsApiOperation,
  ): Promise<void> {
    await delay(this.delayMs);

    const failures = this.pendingFailures.get(operation);
    const failure = failures?.shift();

    if (failures?.length === 0) {
      this.pendingFailures.delete(operation);
    }

    if (failure) {
      throw failure;
    }
  }

  private requireJob(jobId: string): MockJob {
    const job = this.jobs.get(jobId);

    if (!job) {
      throw new Error(`Mock job ${jobId} does not exist.`);
    }

    return job;
  }

  private requireEditableJob(job: MockJob): void {
    if (job.status !== "DRAFT") {
      throw new Error(
        `Job ${job.jobId} is not editable ` + `in status ${job.status}.`,
      );
    }
  }

  private advanceLifecycle(job: MockJob): void {
    if (job.cancellationRequested) {
      job.cancellationRequested = false;
      job.status = "CANCELLED";
      job.message = "Job was cancelled.";
      job.finishedAt = this.now().toISOString();
      return;
    }

    if (job.status === "SUBMITTED") {
      if (job.statusReadCount === 0) {
        job.statusReadCount += 1;
        return;
      }

      job.status = "INPUTS_PREPARED";
      job.progress = 0;
      job.message = "Worker prepared the job inputs.";
      job.statusReadCount = 0;
      return;
    }

    if (job.status === "INPUTS_PREPARED") {
      job.status = "IN_PROGRESS";
      job.progress = this.progressSequence[0] ?? 10;
      job.message = "Worker is processing the job.";
      job.startedAt = this.now().toISOString();
      job.statusReadCount = 1;
      return;
    }

    if (job.status !== "IN_PROGRESS") {
      return;
    }

    const nextProgress = this.progressSequence[job.statusReadCount] ?? 100;

    if (nextProgress >= 100) {
      job.status = "DONE";
      job.progress = 100;
      job.message = "Job completed successfully.";
      job.finishedAt = this.now().toISOString();
      return;
    }

    job.progress = nextProgress;
    job.message = "Worker is processing the job.";
    job.statusReadCount += 1;
  }

  private toStatusResponse(job: MockJob): JobStatusResponseDTO {
    return {
      jobId: job.jobId,
      jobType: job.jobType,
      status: job.status,
      progress: job.progress,
      message: job.message,
      createdAt: job.createdAt,
      submittedAt: job.submittedAt,
      startedAt: job.startedAt,
      finishedAt: job.finishedAt,
    };
  }

  private expiresAt(): string {
    return new Date(this.now().getTime() + URL_TTL_MS).toISOString();
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, milliseconds);
  });
}

function createMockJobId(): string {
  return `mock-job-${globalThis.crypto.randomUUID()}`;
}
