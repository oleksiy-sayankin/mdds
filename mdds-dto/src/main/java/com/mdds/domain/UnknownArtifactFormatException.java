/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Indication that requested artifact format is unknown or unsupported. */
public class UnknownArtifactFormatException extends RuntimeException {
  public UnknownArtifactFormatException(String message) {
    super(message);
  }
}
