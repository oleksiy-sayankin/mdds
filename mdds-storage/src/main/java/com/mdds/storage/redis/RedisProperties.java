/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Base class for Redis connection properties. */
@ConfigurationProperties(prefix = "mdds.redis")
@Getter
@Setter
public class RedisProperties {
  private String host;
  private int port;
}
