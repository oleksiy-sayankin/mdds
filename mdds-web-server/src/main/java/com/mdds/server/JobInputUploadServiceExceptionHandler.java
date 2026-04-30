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
public class JobInputUploadServiceExceptionHandler {

  @ExceptionHandler(InputSlotIsNullOrBlankException.class)
  public ResponseEntity<ErrorResponseDTO> handleInputSlotIsNullOrBlank(
      InputSlotIsNullOrBlankException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(InputUploadUrlNotSupportedForJobTypeException.class)
  public ResponseEntity<ErrorResponseDTO> handleInputUploadUrlNotSupported(
      InputUploadUrlNotSupportedForJobTypeException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(UnknownOrUnsupportedInputSlotException.class)
  public ResponseEntity<ErrorResponseDTO> handleUnknownOrUnsupportedInputSlot(
      UnknownOrUnsupportedInputSlotException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }
}
