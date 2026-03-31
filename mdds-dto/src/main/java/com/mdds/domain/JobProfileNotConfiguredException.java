/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Indicates that for certain job type job profile was not configured for a job type. */
public class JobProfileNotConfiguredException extends RuntimeException {
  public JobProfileNotConfiguredException(String message) {
    super(message);
  }
}
