/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import { describe, expect, it } from "vitest";
import { MockArtifactTransferClient } from "./MockArtifactTransferClient";

describe("MockArtifactTransferClient", () => {
  it("stores uploaded files in memory", async () => {
    const client = new MockArtifactTransferClient({ delayMs: 0 });
    const file = new File(["matrix"], "matrix.csv");
    const url = "mock://mdds/uploads/job-1/matrix";

    await client.upload(url, file);

    expect(client.getUploadedFile(url)).toBe(file);
  });

  it("returns the configured download", async () => {
    const client = new MockArtifactTransferClient({ delayMs: 0 });
    const url = "mock://mdds/downloads/job-1/solution";
    client.setDownload(url, new Blob(["custom solution"]));

    const result = await client.download(url);
    expect(await result.text()).toBe("custom solution");
  });

  it("supports controlled transfer failures", async () => {
    const client = new MockArtifactTransferClient({ delayMs: 0 });
    client.failNextUpload(new Error("Upload failed."));
    client.failNextDownload(new Error("Download failed."));

    await expect(
      client.upload("mock://upload", new File(["matrix"], "matrix.csv")),
    ).rejects.toThrow("Upload failed.");

    await expect(client.download("mock://download")).rejects.toThrow(
      "Download failed.",
    );
  });

  it("honors an already aborted operation", async () => {
    const client = new MockArtifactTransferClient({ delayMs: 100 });
    const abortController = new AbortController();
    abortController.abort();

    await expect(
      client.download("mock://download", abortController.signal),
    ).rejects.toMatchObject({
      name: "AbortError",
    });
  });
});
