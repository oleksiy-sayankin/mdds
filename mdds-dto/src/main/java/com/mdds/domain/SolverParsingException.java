/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Indicates incorrect solver. */
public class SolverParsingException extends RuntimeException {
  public SolverParsingException(String message) {
    super(message);
  }
}
