/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import { describe, expect, it, vi } from "vitest";
import { FetchArtifactTransferClient } from "./FetchArtifactTransferClient";

describe("FetchArtifactTransferClient", () => {
  it("uploads a file with PUT", async () => {
    const fetchFn = vi.fn(async () => new Response(null, { status: 200 }));
    const client = new FetchArtifactTransferClient(fetchFn as typeof fetch);
    const file = new File(["matrix"], "matrix.csv");

    await client.upload("https://storage/upload", file);

    expect(fetchFn).toHaveBeenCalledWith("https://storage/upload", {
      method: "PUT",
      body: file,
      signal: undefined,
    });
  });

  it("downloads a blob with GET", async () => {
    const expected = new Blob(["solution"]);
    const fetchFn = vi.fn(async () => new Response(expected, { status: 200 }));

    const client = new FetchArtifactTransferClient(fetchFn as typeof fetch);
    const result = await client.download("https://storage/download");

    expect(await result.text()).toBe("solution");
    expect(fetchFn).toHaveBeenCalledWith("https://storage/download", {
      method: "GET",
      signal: undefined,
    });
  });

  it("reports a failed upload", async () => {
    const fetchFn = vi.fn(
      async () =>
        new Response(null, {
          status: 403,
          statusText: "Forbidden",
        }),
    );

    const client = new FetchArtifactTransferClient(fetchFn as typeof fetch);

    await expect(
      client.upload(
        "https://storage/upload",
        new File(["matrix"], "matrix.csv"),
      ),
    ).rejects.toThrow("Artifact upload failed: HTTP 403 Forbidden");
  });

  it("reports a failed download", async () => {
    const fetchFn = vi.fn(
      async () =>
        new Response(null, {
          status: 404,
          statusText: "Not Found",
        }),
    );

    const client = new FetchArtifactTransferClient(fetchFn as typeof fetch);

    await expect(client.download("https://storage/download")).rejects.toThrow(
      "Artifact download failed: HTTP 404 Not Found",
    );
  });
});
