/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.JobStatus;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.embedded.RedisServer;

class TestRedisDataStorage {
  private static final String DEFAULT_HOST = "localhost";
  private static final int REDIS_SERVER_PORT = findFreePort();
  private static RedisServer redisServer;

  @BeforeAll
  static void startServer() throws IOException {
    redisServer = new RedisServer(REDIS_SERVER_PORT);
    redisServer.start();
  }

  @AfterAll
  static void stopServer() throws IOException {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  void testPut() {
    var result = new ResultDTO();
    var jobId = "test";
    result.setJobId(jobId);
    result.setDateTimeJobStarted(Instant.now());
    result.setDateTimeJobEnded(Instant.now());
    result.setJobStatus(JobStatus.DONE);
    result.setProgress(100);
    result.setSolution(new double[] {9.3, 6.278, 6.783, 3.874});
    result.setErrorMessage("");
    try (var dataStorage = new RedisDataStorage(DEFAULT_HOST, REDIS_SERVER_PORT)) {
      assertThatCode(() -> dataStorage.put(jobId, result)).doesNotThrowAnyException();
    }
  }

  @Test
  void testGet() {
    var expectedResult = new ResultDTO();
    var jobId = "test";
    expectedResult.setJobId(jobId);
    expectedResult.setDateTimeJobStarted(Instant.now());
    expectedResult.setDateTimeJobEnded(Instant.now());
    expectedResult.setJobStatus(JobStatus.DONE);
    expectedResult.setProgress(100);
    expectedResult.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});
    expectedResult.setErrorMessage("");
    try (var dataStorage = new RedisDataStorage(DEFAULT_HOST, REDIS_SERVER_PORT)) {
      assertThatCode(() -> dataStorage.put(jobId, expectedResult)).doesNotThrowAnyException();
      var actualResult = dataStorage.get(jobId, ResultDTO.class);
      assertThat(actualResult.isPresent() ? actualResult.get() : actualResult)
          .isEqualTo(expectedResult);
    }
  }

  @Test
  void testClose() {
    try (var ignore = new RedisDataStorage(DEFAULT_HOST, REDIS_SERVER_PORT)) {
      assertThatCode(() -> {}).doesNotThrowAnyException();
    }
  }

  @Test
  void testNoConfFileExists() {
    var randomHost = "random.host";
    var randomPort = 89798;
    var oneSecond = Duration.ofSeconds(1);
    assertThatThrownBy(
            () -> {
              try (var ignore = new RedisDataStorage(randomHost, randomPort, oneSecond)) {
                // Do nothing.
              }
            })
        .isInstanceOf(RedisConnectionException.class)
        .hasMessageContaining("Failed to connect to redis://");
  }

  @Test
  void testWrongConfFile() {
    var oneSecond = Duration.ofSeconds(1);
    assertThatThrownBy(
            () -> {
              try (var ignore = new RedisDataStorage("wrong.host", 3234, oneSecond)) {
                // Do nothing.
              }
            })
        .isInstanceOf(RedisConnectionException.class)
        .hasMessageContaining("Failed to connect to redis://");
  }
}
