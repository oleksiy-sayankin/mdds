// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Represents a failed HTTP response from the MDDS REST API.
 *
 * Keeps the response status, status text, and parsed body so UI components
 * can show useful diagnostics instead of a generic network error.
 */

export class HttpError extends Error {
  readonly method: string;
  readonly url: string;
  readonly status: number;
  readonly statusText: string;
  readonly responseBody: unknown;

  constructor(args: {
    method: string;
    url: string;
    status: number;
    statusText: string;
    responseBody: unknown;
    message: string;
  }) {
    super(args.message);
    this.name = "HttpError";
    this.method = args.method;
    this.url = args.url;
    this.status = args.status;
    this.statusText = args.statusText;
    this.responseBody = args.responseBody;
  }
}
