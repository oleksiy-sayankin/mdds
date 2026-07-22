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
  INPUTS_PREPARED("INPUTS_PREPARED"),
  IN_PROGRESS("IN_PROGRESS"),
  DONE("DONE"),
  ERROR("ERROR"),
  CANCEL_REQUESTED("CANCEL_REQUESTED"),
  CANCELLED("CANCELLED");

  @Getter private final String code;

  public boolean canSwitchTo(JobStatus newStatus) {
    return switch (this) {
      case DRAFT -> newStatus == SUBMITTED;
      case SUBMITTED -> newStatus == INPUTS_PREPARED || newStatus == ERROR;
      case INPUTS_PREPARED -> newStatus == IN_PROGRESS || newStatus == ERROR;
      case IN_PROGRESS ->
          newStatus == IN_PROGRESS
              || newStatus == DONE
              || newStatus == ERROR
              || newStatus == CANCEL_REQUESTED;
      case CANCEL_REQUESTED -> newStatus == CANCELLED || newStatus == DONE || newStatus == ERROR;
      case DONE, ERROR, CANCELLED -> false;
    };
  }

  public boolean isTerminal() {
    return switch (this) {
      case DONE, ERROR, CANCELLED -> true;
      default -> false;
    };
  }

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
