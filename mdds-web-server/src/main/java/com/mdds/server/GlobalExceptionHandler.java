/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.UnknownJobTypeException;
import com.mdds.dto.ErrorResponseDTO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
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

  @ExceptionHandler(UnknownUserException.class)
  public ResponseEntity<ErrorResponseDTO> handleUnknownUser(UnknownUserException ex) {
    return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(UnknownJobTypeException.class)
  public ResponseEntity<ErrorResponseDTO> handleUnknownJobType(UnknownJobTypeException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(JobTypeConflictException.class)
  public ResponseEntity<ErrorResponseDTO> handleJobTypeConflict(JobTypeConflictException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(MissingRequestHeaderException.class)
  public ResponseEntity<ErrorResponseDTO> handleMissingRequestHeader(
      MissingRequestHeaderException ex) {
    var errorMessage = "Required request header '" + ex.getHeaderName() + "' is missing.";
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponseDTO(errorMessage));
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ErrorResponseDTO> handleValidation(MethodArgumentNotValidException ex) {
    // Extract all validation errors from the exception
    var sb = new StringBuilder();
    var br = ex.getBindingResult();
    if (br.hasErrors()) {
      var first = true;
      for (var error : ex.getBindingResult().getFieldErrors()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(error.getField());
        sb.append(": ");
        sb.append(error.getDefaultMessage());
        first = false;
      }
    }
    if (sb.isEmpty()) {
      sb.append("Request validation failed.");
    }
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponseDTO(sb.toString()));
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<ErrorResponseDTO> handleHttpMessageNotReadable(
      HttpMessageNotReadableException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO("Request body is missing or malformed."));
  }

  @ExceptionHandler(JobIsNotDraftException.class)
  public ResponseEntity<ErrorResponseDTO> handleJobIsNotDraft(JobIsNotDraftException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(UserIsNullOrBlankException.class)
  public ResponseEntity<ErrorResponseDTO> handleUserIsNullOrBlank(UserIsNullOrBlankException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

  @ExceptionHandler(UploadSessionIdIsNullOrBlankException.class)
  public ResponseEntity<ErrorResponseDTO> handleUploadSessionIdIsNullOrBlank(
      UploadSessionIdIsNullOrBlankException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }

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

  @ExceptionHandler(JobDoesNotExistException.class)
  public ResponseEntity<ErrorResponseDTO> handleJobDoesNotExist(JobDoesNotExistException ex) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponseDTO(ex.getMessage()));
  }

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

  @ExceptionHandler(MergePatchDocumentMustBeJsonObjectException.class)
  public ResponseEntity<ErrorResponseDTO> handleMergePatchDocumentMustBeJsonObject(
      MergePatchDocumentMustBeJsonObjectException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ErrorResponseDTO(ex.getMessage()));
  }
}
