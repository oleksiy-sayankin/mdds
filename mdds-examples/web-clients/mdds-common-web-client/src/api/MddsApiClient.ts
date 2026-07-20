/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import type {
  CancelJobResponseDTO,
  CreateJobResponseDTO,
  JobOutputResponseDTO,
  JobStatusResponseDTO,
  JobUploadUrlResponseDTO,
  PatchJobParamsRequest,
  SubmitJobResponseDTO,
} from "./MddsRestTypes";

/**
 * Port used by the Web Client workflows to access the MDDS REST API.
 *
 * Production and mock implementations must expose the same DTO-level
 * contract so workflow state machines do not depend on the transport.
 */
export interface MddsApiClient {
  createOrReuseDraftJob(
    jobType: string,
    uploadSessionId: string,
  ): Promise<CreateJobResponseDTO>;

  requestInputUploadUrl(
    jobId: string,
    inputSlot: string,
  ): Promise<JobUploadUrlResponseDTO>;

  patchJobParams(jobId: string, params: PatchJobParamsRequest): Promise<void>;

  submitJob(jobId: string): Promise<SubmitJobResponseDTO>;

  getJobStatus(jobId: string): Promise<JobStatusResponseDTO>;

  cancelJob(jobId: string): Promise<CancelJobResponseDTO>;

  requestOutputDownloadUrl(
    jobId: string,
    outputSlot: string,
  ): Promise<JobOutputResponseDTO>;
}
