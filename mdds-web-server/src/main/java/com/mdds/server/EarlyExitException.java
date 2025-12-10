/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Stub exception to perform early exit from set of operations. */
public class EarlyExitException extends RuntimeException {
  public EarlyExitException(String message) {
    super(message);
  }
}
