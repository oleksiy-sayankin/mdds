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
public class GlobalExceptionHandler {
  @ExceptionHandler(EarlyExitException.class)
  public ResponseEntity<ErrorResponseDTO> handleEarlyExit(EarlyExitException ex) {
    return ResponseEntity.badRequest().body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(NoResultFoundException.class)
  public ResponseEntity<ErrorResponseDTO> handleNoResultFound(NoResultFoundException ex) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(CanNotCancelJobException.class)
  public ResponseEntity<ErrorResponseDTO> handleCanNotCancelJob(CanNotCancelJobException ex) {
    return ResponseEntity.status(ex.getHttpStatus()).body(new ErrorResponseDTO(ex.getMessage()));
  }
}
