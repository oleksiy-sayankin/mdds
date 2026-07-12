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
public class JobParamsServiceExceptionHandler {
  @ExceptionHandler(UnknownOrUnsupportedJobParameterException.class)
  public ResponseEntity<ErrorResponseDTO> handleUnknownOrUnsupportedJobParameter(
      UnknownOrUnsupportedJobParameterException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(JobParameterIsNullOrBlankException.class)
  public ResponseEntity<ErrorResponseDTO> handleNullOrBlankJobParameter(
      JobParameterIsNullOrBlankException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(InvalidJobParameterTypeException.class)
  public ResponseEntity<ErrorResponseDTO> handleInvalidJobParameterType(
      InvalidJobParameterTypeException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(InvalidJobParameterValueException.class)
  public ResponseEntity<ErrorResponseDTO> handleInvalidJobParameterValue(
      InvalidJobParameterValueException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }
}
