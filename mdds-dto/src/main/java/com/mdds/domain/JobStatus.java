/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Enumeration of Job statuses. */
public enum JobStatus {
  DRAFT,
  SUBMITTED,
  IN_PROGRESS,
  DONE,
  ERROR,
  CANCEL_REQUESTED,
  CANCELLED
}
