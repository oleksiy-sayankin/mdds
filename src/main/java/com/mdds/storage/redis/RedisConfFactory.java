/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

/** Factory for creating Redis configurations. */
public final class RedisConfFactory {
  private RedisConfFactory() {}

  public static final String FILE_NAME = "redis.properties";

  public static RedisConf fromEnvOrDefaultProperties() {
    var props = readPropertiesOrEmpty(FILE_NAME);
    var host = resolveString("redis.host", "REDIS_HOST", props, "localhost");
    var port = resolveInt("redis.port", "REDIS_PORT", props, 6379);
    return new RedisConf(host, port);
  }
}
