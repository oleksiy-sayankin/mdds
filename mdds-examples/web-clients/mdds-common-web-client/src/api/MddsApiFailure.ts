/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

import { HttpError } from "@/api/HttpError";

const AMBIGUOUS_HTTP_STATUSES = new Set([408, 429, 500, 502, 503, 504]);

/**
 * Returns true when an API operation may have reached
 * the server but its outcome was not confirmed.
 */
export function isAmbiguousMddsApiFailure(error: unknown): boolean {
  if (!(error instanceof HttpError)) {
    return true;
  }

  return AMBIGUOUS_HTTP_STATUSES.has(error.status);
}

/**
 * Cancellation additionally reconciles HTTP 409 because
 * the job lifecycle may have changed concurrently with
 * the cancellation request.
 */
export function isAmbiguousCancellationFailure(error: unknown): boolean {
  if (error instanceof HttpError && error.status === 409) {
    return true;
  }

  return isAmbiguousMddsApiFailure(error);
}
