/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/**
 * Indicates that item in profile storage has inconsistency. Say, certain job type is used for key,
 * but the job profile (which is value for that key) has another job type.
 */
public class InvalidProfileKeyException extends RuntimeException {
  public InvalidProfileKeyException(String message) {
    super(message);
  }
}
