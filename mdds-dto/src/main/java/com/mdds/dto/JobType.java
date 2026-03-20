/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import java.util.Arrays;

/** List of possible job types. */
public enum JobType {
  SOLVING_SLAE("solving_slae"),
  SOLVING_SLAE_PARALLEL("solving_slae_parallel");

  private final String value;

  JobType(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static JobType from(String raw) {
    if (raw == null || raw.isBlank()) {
      throw new UnknownJobTypeException("Job type must not be null or blank.");
    }
    return Arrays.stream(values())
        .filter(t -> t.value.equalsIgnoreCase(raw.trim()))
        .findFirst()
        .orElseThrow(
            () -> new UnknownJobTypeException("Unknown or unsupported job type: " + raw + "."));
  }
}
