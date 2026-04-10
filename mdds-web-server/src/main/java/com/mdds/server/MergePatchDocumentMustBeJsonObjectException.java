/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that input JSON patch with job parameters is not a JSON object. */
public class MergePatchDocumentMustBeJsonObjectException extends RuntimeException {
  public MergePatchDocumentMustBeJsonObjectException(String message) {
    super(message);
  }
}
