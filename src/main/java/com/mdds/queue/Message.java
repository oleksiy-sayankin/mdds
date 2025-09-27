/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.Map;

/**
 * Represents common message for all types of queues. Contains message body and metadata.
 *
 * @param payload Content of the message
 * @param headers key-value map for metadata
 * @param timestamp date/time of creation
 * @param <T> type of payload
 */
public record Message<T>(
    @Nonnull T payload, @Nonnull Map<String, Object> headers, @Nonnull Instant timestamp) {}
