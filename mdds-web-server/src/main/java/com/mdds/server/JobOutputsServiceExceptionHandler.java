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
public class JobOutputsServiceExceptionHandler {

  @ExceptionHandler(OutputSlotIsNullOrBlankException.class)
  public ResponseEntity<ErrorResponseDTO> handleOutputSlotIsNullOrBlank(
      OutputSlotIsNullOrBlankException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(UnknownOrUnsupportedOutputSlotException.class)
  public ResponseEntity<ErrorResponseDTO> handleUnknownOrUnsupportedOutputSlot(
      UnknownOrUnsupportedOutputSlotException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(JobIsNotDoneException.class)
  public ResponseEntity<ErrorResponseDTO> handleJobIsNotDoneException(JobIsNotDoneException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(OutputArtifactDoesNotExistException.class)
  public ResponseEntity<ErrorResponseDTO> handleOutputArtifactDoesNotExist(
      OutputArtifactDoesNotExistException ex) {
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(new ErrorResponseDTO("Internal Server Error"));
  }
}
