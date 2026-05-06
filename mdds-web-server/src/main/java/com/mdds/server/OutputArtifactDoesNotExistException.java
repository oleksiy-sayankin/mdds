/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that a DONE job has no expected output artifact in object storage. */
public class OutputArtifactDoesNotExistException extends RuntimeException {
  public OutputArtifactDoesNotExistException(String message) {
    super(message);
  }
}
