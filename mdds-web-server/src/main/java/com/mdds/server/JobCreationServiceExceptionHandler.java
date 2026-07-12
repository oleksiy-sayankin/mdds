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
public class JobCreationServiceExceptionHandler {

  @ExceptionHandler(UnknownOrUnsupportedJobTypeException.class)
  public ResponseEntity<ErrorResponseDTO> handleUnknownJobType(
      UnknownOrUnsupportedJobTypeException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(JobTypeConflictException.class)
  public ResponseEntity<ErrorResponseDTO> handleJobTypeConflict(JobTypeConflictException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(UploadSessionIdIsNullOrBlankException.class)
  public ResponseEntity<ErrorResponseDTO> handleUploadSessionIdIsNullOrBlank(
      UploadSessionIdIsNullOrBlankException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }
}
