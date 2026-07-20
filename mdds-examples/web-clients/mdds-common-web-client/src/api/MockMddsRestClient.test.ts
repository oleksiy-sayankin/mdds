/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import { describe, expect, it } from "vitest";
import { MockMddsRestClient } from "./MockMddsRestClient";

function createClient() {
  let nextJobId = 1;

  return new MockMddsRestClient({
    delayMs: 0,
    jobIdFactory: () => `job-${nextJobId++}`,
    now: () => new Date("2026-07-19T12:00:00.000Z"),
  });
}

async function createDraftJob(client: MockMddsRestClient) {
  const draft = await client.createOrReuseDraftJob(
    "solving_slae",
    "upload-session-1",
  );

  return draft.jobId;
}

describe("MockMddsRestClient", () => {
  it("reuses a draft job for the same upload session", async () => {
    const client = createClient();

    const first = await client.createOrReuseDraftJob(
      "solving_slae",
      "session-1",
    );

    const second = await client.createOrReuseDraftJob(
      "solving_slae",
      "session-1",
    );

    expect(second.jobId).toBe(first.jobId);
  });

  it("submits a draft without modelling object-storage state", async () => {
    const client = createClient();

    const draft = await client.createOrReuseDraftJob(
      "solving_slae",
      "session-1",
    );

    await client.patchJobParams(draft.jobId, {
      solvingMethod: "numpy_exact_solver",
    });

    await expect(client.submitJob(draft.jobId)).resolves.toEqual({
      jobId: draft.jobId,
      status: "SUBMITTED",
    });
  });

  it("publishes CANCELLED only through a later status read", async () => {
    const client = createClient();
    const jobId = await createDraftJob(client);

    await client.submitJob(jobId);

    await expect(client.getJobStatus(jobId)).resolves.toMatchObject({
      status: "SUBMITTED",
    });

    await expect(client.getJobStatus(jobId)).resolves.toMatchObject({
      status: "INPUTS_PREPARED",
    });

    await expect(client.getJobStatus(jobId)).resolves.toMatchObject({
      status: "IN_PROGRESS",
    });

    await expect(client.cancelJob(jobId)).resolves.toEqual({
      jobId,
      status: "CANCEL_REQUESTED",
    });

    await expect(client.getJobStatus(jobId)).resolves.toMatchObject({
      jobId,
      status: "CANCELLED",
    });
  });

  it("can fail the next selected operation", async () => {
    const client = createClient();
    client.failNext(
      "createOrReuseDraftJob",
      new Error("Draft service unavailable."),
    );

    await expect(
      client.createOrReuseDraftJob("solving_slae", "session-1"),
    ).rejects.toThrow("Draft service unavailable.");
  });

  it("keeps a job in DRAFT while upload URLs are requested", async () => {
    const client = createClient();

    const draft = await client.createOrReuseDraftJob(
      "solving_slae",
      "session-1",
    );

    await client.requestInputUploadUrl(draft.jobId, "matrix");

    await client.requestInputUploadUrl(draft.jobId, "rhs");

    await expect(client.getJobStatus(draft.jobId)).resolves.toMatchObject({
      jobId: draft.jobId,
      status: "DRAFT",
    });

    await expect(
      client.patchJobParams(draft.jobId, {
        solvingMethod: "numpy_exact_solver",
      }),
    ).resolves.toBeUndefined();
  });
});
