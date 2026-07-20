/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

/**
 * Port for direct object-storage transfers through pre-signed URLs.
 *
 * These operations deliberately stay outside MddsApiClient because their
 * HTTP requests target object storage rather than the MDDS Web Server.
 */
export interface ArtifactTransferClient {
  upload(url: string, file: File, signal?: AbortSignal): Promise<void>;

  download(url: string, signal?: AbortSignal): Promise<Blob>;
}
