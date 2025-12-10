/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage;

import com.mdds.storage.redis.RedisDataStorage;
import com.mdds.storage.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(RedisProperties.class)
public class DataStorageConfig {

  @Bean(name = "redis")
  DataStorage redis(RedisProperties redisProperties) {
    return new RedisDataStorage(redisProperties);
  }
}
