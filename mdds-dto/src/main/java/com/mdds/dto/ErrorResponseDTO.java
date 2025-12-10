/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

/**
 * Used in cases when any error should be returned.
 *
 * @param message Text message of the error.
 */
public record ErrorResponseDTO(String message) {}
