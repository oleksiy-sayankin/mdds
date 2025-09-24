/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

/**
 * Base class for Redis connection properties.
 *
 * @param host Redis host
 * @param port Redis port
 */
public record RedisConf(String host, int port) {}
