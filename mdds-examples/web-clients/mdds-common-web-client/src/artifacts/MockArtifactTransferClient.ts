/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import type { ArtifactTransferClient } from "./ArtifactTransferClient";

const DEFAULT_SOLUTION_CSV = [
  "index,value",
  "0,1.0",
  "1,2.0",
  "2,3.0",
  "",
].join("\n");

export interface MockArtifactTransferClientConfig {
  delayMs?: number;
  defaultDownload?: Blob;
}

/** In-memory artifact transfer adapter for UI development and tests. */
export class MockArtifactTransferClient implements ArtifactTransferClient {
  private readonly delayMs: number;
  private readonly defaultDownload: Blob;
  private readonly uploadedFiles = new Map<string, File>();
  private readonly downloads = new Map<string, Blob>();
  private nextUploadError: Error | null = null;
  private nextDownloadError: Error | null = null;

  constructor(config: MockArtifactTransferClientConfig = {}) {
    this.delayMs = config.delayMs ?? 500;
    this.defaultDownload =
      config.defaultDownload ??
      new Blob([DEFAULT_SOLUTION_CSV], {
        type: "text/csv;charset=utf-8",
      });
  }

  failNextUpload(
    error: Error = new Error("Mock artifact upload failure."),
  ): void {
    this.nextUploadError = error;
  }

  failNextDownload(
    error: Error = new Error("Mock artifact download failure."),
  ): void {
    this.nextDownloadError = error;
  }

  setDownload(url: string, blob: Blob): void {
    this.downloads.set(url, blob);
  }

  getUploadedFile(url: string): File | undefined {
    return this.uploadedFiles.get(url);
  }

  async upload(url: string, file: File, signal?: AbortSignal): Promise<void> {
    await abortableDelay(this.delayMs, signal);

    if (this.nextUploadError) {
      const error = this.nextUploadError;
      this.nextUploadError = null;
      throw error;
    }

    this.uploadedFiles.set(url, file);
  }

  async download(url: string, signal?: AbortSignal): Promise<Blob> {
    await abortableDelay(this.delayMs, signal);

    if (this.nextDownloadError) {
      const error = this.nextDownloadError;
      this.nextDownloadError = null;
      throw error;
    }

    const blob = this.downloads.get(url) ?? this.defaultDownload;
    return blob.slice(0, blob.size, blob.type);
  }
}

function abortableDelay(
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(createAbortError());
      return;
    }

    const timerId = globalThis.setTimeout(() => {
      signal?.removeEventListener("abort", handleAbort);
      resolve();
    }, milliseconds);

    const handleAbort = () => {
      globalThis.clearTimeout(timerId);
      signal?.removeEventListener("abort", handleAbort);
      reject(createAbortError());
    };

    signal?.addEventListener("abort", handleAbort, {
      once: true,
    });
  });
}

function createAbortError(): DOMException {
  return new DOMException("The operation was aborted.", "AbortError");
}
