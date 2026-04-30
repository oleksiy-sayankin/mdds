/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.dto.ErrorResponseDTO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class JobSubmissionServiceExceptionHandler {

  @ExceptionHandler(RequiredParameterIsAbsentException.class)
  public ResponseEntity<ErrorResponseDTO> handleRequiredParameterIsAbsent(
      RequiredParameterIsAbsentException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(RequiredInputArtifactIsAbsentException.class)
  public ResponseEntity<ErrorResponseDTO> handleRequiredInputArtifactIsAbsent(
      RequiredInputArtifactIsAbsentException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }
}
