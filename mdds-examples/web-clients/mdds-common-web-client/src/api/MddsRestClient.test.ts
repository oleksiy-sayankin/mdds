/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import { describe, expect, it, vi } from "vitest";

import { MddsRestClient } from "./MddsRestClient";
import { HttpError } from "@/api/HttpError";

/**
 * These tests exercise MddsRestClient as a thin REST adapter.
 *
 * They do not start the real MDDS Web Server. Instead, each test injects a
 * mocked fetch implementation that returns a prepared Response object. This
 * allows the tests to verify that the client builds the correct REST v1 URL,
 * HTTP method, headers, and request body, and that it correctly parses
 * successful responses or converts non-2xx responses into HttpError.
 */
describe("MddsRestClient", () => {
  it("creates or reuses a draft job", async () => {
    const fetchFn = createFetchMock(
      jsonResponse({ jobId: "job-1" }, { status: 201 }),
    );
    const client = createClient(fetchFn);

    await expect(
      client.createOrReuseDraftJob("solving_slae", "session-1"),
    ).resolves.toEqual({ jobId: "job-1" });

    expect(fetchFn).toHaveBeenCalledWith("http://localhost:8000/jobs", {
      method: "POST",
      headers: {
        "X-MDDS-User-Login": "guest",
        "X-MDDS-Upload-Session-Id": "session-1",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ jobType: "solving_slae" }),
    });
  });

  it("requests an input upload URL", async () => {
    const responseBody = {
      jobId: "job-1",
      uploadUrl: "http://minio/upload-url",
      expiresAt: "2026-03-20T14:30:00Z",
    };
    const fetchFn = createFetchMock(jsonResponse(responseBody));
    const client = createClient(fetchFn);

    await expect(
      client.requestInputUploadUrl("job-1", "matrix"),
    ).resolves.toEqual(responseBody);

    expect(fetchFn).toHaveBeenCalledWith(
      "http://localhost:8000/jobs/job-1/inputs",
      {
        method: "POST",
        headers: {
          "X-MDDS-User-Login": "guest",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ inputSlot: "matrix" }),
      },
    );
  });

  it("encodes job id in path parameters", async () => {
    const responseBody = {
      jobId: "job/1",
      uploadUrl: "http://minio/upload-url",
      expiresAt: "2026-03-20T14:30:00Z",
    };
    const fetchFn = createFetchMock(jsonResponse(responseBody));
    const client = createClient(fetchFn);

    await client.requestInputUploadUrl("job/1", "matrix");

    expect(fetchFn).toHaveBeenCalledWith(
      "http://localhost:8000/jobs/job%2F1/inputs",
      expect.objectContaining({
        method: "POST",
      }),
    );
  });

  it("patches job params using JSON merge patch", async () => {
    const fetchFn = createFetchMock(emptyResponse());
    const client = createClient(fetchFn);

    await expect(
      client.patchJobParams("job-1", {
        solvingMethod: "numpy_exact_solver",
        tolerance: 1e-9,
        optionalValue: null,
      }),
    ).resolves.toBeUndefined();

    expect(fetchFn).toHaveBeenCalledWith(
      "http://localhost:8000/jobs/job-1/params",
      {
        method: "PATCH",
        headers: {
          "X-MDDS-User-Login": "guest",
          "Content-Type": "application/merge-patch+json",
        },
        body: JSON.stringify({
          solvingMethod: "numpy_exact_solver",
          tolerance: 1e-9,
          optionalValue: null,
        }),
      },
    );
  });

  it("submits a job without a request body", async () => {
    const responseBody = {
      jobId: "job-1",
      status: "SUBMITTED",
    };
    const fetchFn = createFetchMock(
      jsonResponse(responseBody, { status: 202 }),
    );
    const client = createClient(fetchFn);

    await expect(client.submitJob("job-1")).resolves.toEqual(responseBody);

    expect(fetchFn).toHaveBeenCalledWith(
      "http://localhost:8000/jobs/job-1/submit",
      {
        method: "POST",
        headers: {
          "X-MDDS-User-Login": "guest",
        },
      },
    );
  });

  it("gets job status", async () => {
    const responseBody = {
      jobId: "job-1",
      jobType: "solving_slae",
      status: "IN_PROGRESS",
      progress: 42,
      message: null,
      createdAt: "2026-03-20T14:00:00Z",
      submittedAt: "2026-03-20T14:05:00Z",
      startedAt: "2026-03-20T14:10:00Z",
      finishedAt: null,
    };
    const fetchFn = createFetchMock(jsonResponse(responseBody));
    const client = createClient(fetchFn);

    await expect(client.getJobStatus("job-1")).resolves.toEqual(responseBody);

    expect(fetchFn).toHaveBeenCalledWith(
      "http://localhost:8000/jobs/job-1/status",
      {
        method: "GET",
        headers: {
          "X-MDDS-User-Login": "guest",
        },
      },
    );
  });

  it("requests job cancellation without a request body", async () => {
    const responseBody = {
      jobId: "job-1",
      status: "CANCEL_REQUESTED",
    };
    const fetchFn = createFetchMock(
      jsonResponse(responseBody, { status: 202 }),
    );
    const client = createClient(fetchFn);

    await expect(client.cancelJob("job-1")).resolves.toEqual(responseBody);

    expect(fetchFn).toHaveBeenCalledWith(
      "http://localhost:8000/jobs/job-1/cancel",
      {
        method: "POST",
        headers: {
          "X-MDDS-User-Login": "guest",
        },
      },
    );
  });

  it("requests an output download URL", async () => {
    const responseBody = {
      jobId: "job-1",
      downloadUrl: "http://minio/download-url",
      expiresAt: "2026-03-20T14:30:00Z",
    };
    const fetchFn = createFetchMock(jsonResponse(responseBody));
    const client = createClient(fetchFn);

    await expect(
      client.requestOutputDownloadUrl("job-1", "solution"),
    ).resolves.toEqual(responseBody);

    expect(fetchFn).toHaveBeenCalledWith(
      "http://localhost:8000/jobs/job-1/outputs?outputSlot=solution",
      {
        method: "GET",
        headers: {
          "X-MDDS-User-Login": "guest",
        },
      },
    );
  });

  it("throws HttpError for JSON error responses", async () => {
    const fetchFn = createFetchMock(
      jsonResponse(
        { message: " Job is not in DRAFT status. " },
        { status: 409, statusText: "Conflict" },
      ),
    );
    const client = createClient(fetchFn);

    const error = await client
      .submitJob("job-1")
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(HttpError);
    expect(error).toMatchObject({
      name: "HttpError",
      method: "POST",
      url: "http://localhost:8000/jobs/job-1/submit",
      status: 409,
      statusText: "Conflict",
      responseBody: { message: " Job is not in DRAFT status. " },
      message: "Job is not in DRAFT status.",
    });
  });

  it("uses plain text response body as HttpError message", async () => {
    const fetchFn = createFetchMock(
      textResponse("Plain text failure", {
        status: 500,
        statusText: "Internal Server Error",
      }),
    );
    const client = createClient(fetchFn);

    const error = await client
      .getJobStatus("job-1")
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(HttpError);
    expect(error).toMatchObject({
      method: "GET",
      url: "http://localhost:8000/jobs/job-1/status",
      status: 500,
      statusText: "Internal Server Error",
      responseBody: "Plain text failure",
      message: "Plain text failure",
    });
  });

  it("uses HTTP status as fallback HttpError message for empty responses", async () => {
    const fetchFn = createFetchMock(
      emptyResponse({ status: 404, statusText: "Not Found" }),
    );
    const client = createClient(fetchFn);

    const error = await client
      .getJobStatus("missing-job")
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(HttpError);
    expect(error).toMatchObject({
      method: "GET",
      url: "http://localhost:8000/jobs/missing-job/status",
      status: 404,
      statusText: "Not Found",
      responseBody: undefined,
      message: "HTTP 404 Not Found",
    });
  });
});

function createClient(fetchFn: typeof fetch): MddsRestClient {
  return new MddsRestClient({
    baseUrl: "http://localhost:8000/",
    userLogin: "guest",
    fetchFn,
  });
}

function createFetchMock(response: Response): typeof fetch {
  return vi.fn(async () => response) as unknown as typeof fetch;
}

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: {
      "Content-Type": "application/json",
    },
    ...init,
  });
}

function textResponse(body: string, init: ResponseInit = {}): Response {
  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "text/plain",
    },
    ...init,
  });
}

function emptyResponse(init: ResponseInit = {}): Response {
  return new Response(null, {
    status: 200,
    ...init,
  });
}
