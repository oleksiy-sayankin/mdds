/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import type { ArtifactTransferClient } from "./ArtifactTransferClient";

/** Performs real PUT and GET transfers against pre-signed object URLs. */
export class FetchArtifactTransferClient implements ArtifactTransferClient {
  private readonly fetchFn: typeof fetch;

  constructor(fetchFn: typeof fetch = fetch) {
    this.fetchFn = fetchFn;
  }

  async upload(url: string, file: File, signal?: AbortSignal): Promise<void> {
    const response = await this.fetchFn(url, {
      method: "PUT",
      body: file,
      signal,
    });

    if (!response.ok) {
      throw buildTransferError("upload", response);
    }
  }

  async download(url: string, signal?: AbortSignal): Promise<Blob> {
    const response = await this.fetchFn(url, {
      method: "GET",
      signal,
    });

    if (!response.ok) {
      throw buildTransferError("download", response);
    }

    return response.blob();
  }
}

function buildTransferError(
  operation: "upload" | "download",
  response: Response,
): Error {
  return new Error(
    `Artifact ${operation} failed: HTTP ${response.status} ` +
      response.statusText,
  );
}
