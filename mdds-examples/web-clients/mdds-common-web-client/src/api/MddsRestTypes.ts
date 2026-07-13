/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

/**
 * TypeScript DTOs for MDDS REST API v1.
 *
 * These interfaces mirror Java DTOs from com.mdds.dto.rest.v1.
 * They describe only the external Web Server <-> Web Client contract.
 */

export type JobStatus =
  | "DRAFT"
  | "SUBMITTED"
  | "INPUTS_PREPARED"
  | "IN_PROGRESS"
  | "CANCEL_REQUESTED"
  | "DONE"
  | "ERROR"
  | "CANCELLED";

export type JobParameterValue = string | number | boolean | null;

export type PatchJobParamsRequest = Record<string, JobParameterValue>;

export interface CancelJobResponseDTO {
  jobId: string;
  status: "CANCEL_REQUESTED";
}

export interface CreateJobRequestDTO {
  jobType: string;
}

export interface CreateJobResponseDTO {
  jobId: string;
}

export interface ErrorResponseDTO {
  message: string;
}

export interface JobOutputResponseDTO {
  jobId: string;
  downloadUrl: string;
  expiresAt: string;
}

export interface JobStatusResponseDTO {
  jobId: string;
  jobType: string;
  status: JobStatus;
  progress: number;
  message: string | null;
  createdAt: string;
  submittedAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface JobUploadUrlRequestDTO {
  inputSlot: string;
}

export interface JobUploadUrlResponseDTO {
  jobId: string;
  uploadUrl: string;
  expiresAt: string;
}

export interface SubmitJobResponseDTO {
  jobId: string;
  status: "SUBMITTED";
}
