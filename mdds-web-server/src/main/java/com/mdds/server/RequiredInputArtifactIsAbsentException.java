/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Required input artifact is absent in storage. */
public class RequiredInputArtifactIsAbsentException extends RuntimeException {
  public RequiredInputArtifactIsAbsentException(String message) {
    super(message);
  }
}
