/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/** Indicates that we can not cancel task because we do not know its cancel queue name. */
@Getter
public class CanNotCancelTaskException extends RuntimeException {
  private final HttpStatus httpStatus;

  public CanNotCancelTaskException(HttpStatus httpStatus, String message) {
    super(message);
    this.httpStatus = httpStatus;
  }
}
