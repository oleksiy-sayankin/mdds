/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import type { MddsApiClient } from "./MddsApiClient";
import type {
  CancelJobResponseDTO,
  CreateJobRequestDTO,
  CreateJobResponseDTO,
  JobOutputResponseDTO,
  JobStatusResponseDTO,
  JobUploadUrlRequestDTO,
  JobUploadUrlResponseDTO,
  PatchJobParamsRequest,
  SubmitJobResponseDTO,
} from "./MddsRestTypes";
import { HttpError } from "@/api/HttpError";

/**
 * Client-side wrapper around the MDDS REST v1 API.
 *
 * All Web Server REST calls used by the wizard should go through this module:
 * job creation, input upload URL requests, parameter updates, job submission,
 * status reads, cancellation requests, and output URL requests.
 *
 * Direct object-storage uploads and downloads through pre-signed URLs are not
 * part of the Web Server REST API and should be handled separately.
 */
export class MddsRestClient implements MddsApiClient {
  private readonly baseUrl: string;
  private readonly userLogin: string;
  private readonly fetchFn: typeof fetch;

  constructor(config: MddsRestClientConfig) {
    this.baseUrl = normalizeBaseUrl(config.baseUrl ?? "");
    this.userLogin = config.userLogin;
    this.fetchFn = config.fetchFn ?? getDefaultFetch();
  }

  createOrReuseDraftJob(
    jobType: string,
    uploadSessionId: string,
  ): Promise<CreateJobResponseDTO> {
    const request: CreateJobRequestDTO = { jobType };

    return this.requestJson<CreateJobResponseDTO>({
      method: "POST",
      path: "/jobs",
      headers: {
        "X-MDDS-User-Login": this.userLogin,
        "X-MDDS-Upload-Session-Id": uploadSessionId,
        "Content-Type": "application/json",
      },
      body: request,
    });
  }

  requestInputUploadUrl(
    jobId: string,
    inputSlot: string,
  ): Promise<JobUploadUrlResponseDTO> {
    const request: JobUploadUrlRequestDTO = { inputSlot };

    return this.requestJson<JobUploadUrlResponseDTO>({
      method: "POST",
      path: `/jobs/${encodeURIComponent(jobId)}/inputs`,
      headers: {
        "X-MDDS-User-Login": this.userLogin,
        "Content-Type": "application/json",
      },
      body: request,
    });
  }

  async patchJobParams(
    jobId: string,
    params: PatchJobParamsRequest,
  ): Promise<void> {
    await this.requestJson<void>({
      method: "PATCH",
      path: `/jobs/${encodeURIComponent(jobId)}/params`,
      headers: {
        "X-MDDS-User-Login": this.userLogin,
        "Content-Type": "application/merge-patch+json",
      },
      body: params,
    });
  }

  submitJob(jobId: string): Promise<SubmitJobResponseDTO> {
    return this.requestJson<SubmitJobResponseDTO>({
      method: "POST",
      path: `/jobs/${encodeURIComponent(jobId)}/submit`,
      headers: {
        "X-MDDS-User-Login": this.userLogin,
      },
    });
  }

  getJobStatus(jobId: string): Promise<JobStatusResponseDTO> {
    return this.requestJson<JobStatusResponseDTO>({
      method: "GET",
      path: `/jobs/${encodeURIComponent(jobId)}/status`,
      headers: {
        "X-MDDS-User-Login": this.userLogin,
      },
    });
  }

  cancelJob(jobId: string): Promise<CancelJobResponseDTO> {
    return this.requestJson<CancelJobResponseDTO>({
      method: "POST",
      path: `/jobs/${encodeURIComponent(jobId)}/cancel`,
      headers: {
        "X-MDDS-User-Login": this.userLogin,
      },
    });
  }

  requestOutputDownloadUrl(
    jobId: string,
    outputSlot: string,
  ): Promise<JobOutputResponseDTO> {
    const query = new URLSearchParams({ outputSlot });

    return this.requestJson<JobOutputResponseDTO>({
      method: "GET",
      path: `/jobs/${encodeURIComponent(jobId)}/outputs?${query.toString()}`,
      headers: {
        "X-MDDS-User-Login": this.userLogin,
      },
    });
  }

  private async requestJson<T>(request: RestRequest): Promise<T> {
    const url = `${this.baseUrl}${request.path}`;
    const init: RequestInit = {
      method: request.method,
      headers: request.headers,
    };

    if (request.body !== undefined) {
      init.body = JSON.stringify(request.body);
    }

    const response = await this.fetchFn(url, init);
    const responseBody = await readResponseBody(response);

    if (!response.ok) {
      throw new HttpError({
        method: request.method,
        url,
        status: response.status,
        statusText: response.statusText,
        responseBody,
        message: buildHttpErrorMessage(response, responseBody),
      });
    }

    return responseBody as T;
  }
}

export interface MddsRestClientConfig {
  baseUrl?: string;
  userLogin: string;
  fetchFn?: typeof fetch;
}

interface RestRequest {
  method: string;
  path: string;
  headers: Record<string, string>;
  body?: unknown;
}

const FORWARD_SLASH = "/";

function normalizeBaseUrl(baseUrl: string): string {
  let endIndex = baseUrl.length;

  while (endIndex > 0 && baseUrl[endIndex - 1] === FORWARD_SLASH) {
    endIndex -= 1;
  }

  return baseUrl.slice(0, endIndex);
}

async function readResponseBody(response: Response): Promise<unknown> {
  const text = await response.text();

  if (text.trim() === "") {
    return undefined;
  }

  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function buildHttpErrorMessage(
  response: Response,
  responseBody: unknown,
): string {
  if (isObjectWithNonBlankString(responseBody, "message")) {
    return responseBody.message.trim();
  }

  if (isObjectWithNonBlankString(responseBody, "error")) {
    return responseBody.error.trim();
  }

  if (typeof responseBody === "string" && responseBody.trim() !== "") {
    return responseBody.trim();
  }

  return `HTTP ${response.status} ${response.statusText}`.trim();
}

function isObjectWithNonBlankString(
  value: unknown,
  key: string,
): value is Record<string, string> {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const fieldValue = (value as Record<string, unknown>)[key];

  return typeof fieldValue === "string" && fieldValue.trim() !== "";
}

function getDefaultFetch(): typeof fetch {
  if (typeof globalThis.fetch !== "function") {
    throw new TypeError("The Fetch API is not available.");
  }

  return globalThis.fetch.bind(globalThis);
}
