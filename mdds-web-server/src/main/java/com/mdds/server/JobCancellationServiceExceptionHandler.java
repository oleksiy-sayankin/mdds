/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.dto.rest.v1.ErrorResponseDTO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class JobCancellationServiceExceptionHandler {

  @ExceptionHandler(JobIsInTerminalStateException.class)
  public ResponseEntity<ErrorResponseDTO> handleIsInTerminalState(
      JobIsInTerminalStateException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(JobIsNotRunningException.class)
  public ResponseEntity<ErrorResponseDTO> handleIsNotRunning(JobIsNotRunningException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(JobHasNoWorkerAssignedException.class)
  public ResponseEntity<ErrorResponseDTO> handleNoWorkerAssigned(
      JobHasNoWorkerAssignedException ex) {
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(new ErrorResponseDTO("Internal Server Error"));
  }
}
