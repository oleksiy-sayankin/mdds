/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

import java.util.Arrays;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Enumeration of Job statuses. */
@RequiredArgsConstructor
public enum JobStatus {
  DRAFT("DRAFT"),
  SUBMITTED("SUBMITTED"),
  VALIDATION_FAILED("VALIDATION_FAILED"),
  IN_PROGRESS("IN_PROGRESS"),
  DONE("DONE"),
  ERROR("ERROR"),
  CANCEL_REQUESTED("CANCEL_REQUESTED"),
  CANCELLED("CANCELLED");

  @Getter private final String code;

  public static JobStatus from(String raw) {
    if (raw == null || raw.isBlank()) {
      throw new UnknownJobStatusException("Job status must not be null or blank.");
    }
    return Arrays.stream(values())
        .filter(t -> t.code.equalsIgnoreCase(raw.trim()))
        .findFirst()
        .orElseThrow(
            () ->
                new UnknownJobStatusException("Unknown or unsupported job status: '" + raw + "'."));
  }
}
