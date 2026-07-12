/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto.rest.v1;

import jakarta.validation.constraints.NotBlank;

/**
 * Request for url for uploading input slot.
 *
 * @param inputSlot input slot that will be uploaded via url.
 */
public record JobUploadUrlRequestDTO(
    @NotBlank(message = "must not be null or blank.") String inputSlot) {}
