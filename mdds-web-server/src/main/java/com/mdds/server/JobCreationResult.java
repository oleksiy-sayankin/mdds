/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/**
 * Container for Job creation result indicating whether Job was created or used existing one.
 *
 * @param jobId job identifier.
 * @param created true if Job was created and false otherwise.
 */
public record JobCreationResult(String jobId, boolean created) {}
