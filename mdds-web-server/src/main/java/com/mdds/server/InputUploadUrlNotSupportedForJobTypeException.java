/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/**
 * We use this exception type to show that some job types are not supported yet, so can not upload
 * data for it in input slots.
 */
public class InputUploadUrlNotSupportedForJobTypeException extends RuntimeException {
  public InputUploadUrlNotSupportedForJobTypeException(String message) {
    super(message);
  }
}
