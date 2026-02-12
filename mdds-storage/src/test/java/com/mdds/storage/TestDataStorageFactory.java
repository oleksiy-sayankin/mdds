/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.JobStatus;
import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import redis.embedded.RedisServer;

@SpringBootTest(classes = DataStorageConfig.class)
class TestDataStorageFactory {
  private static final String DEFAULT_HOST = "localhost";
  private static final int REDIS_SERVER_PORT = findFreePort();
  private static RedisServer redisServer;

  @Autowired
  @Qualifier("redis")
  private DataStorage dataStorage;

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

  @DynamicPropertySource
  static void redisProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.redis.host", () -> DEFAULT_HOST);
    registry.add("mdds.redis.port", () -> REDIS_SERVER_PORT);
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
    result.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    result.setErrorMessage("");
    assertThatCode(() -> dataStorage.put(jobId, result)).doesNotThrowAnyException();
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
    expectedResult.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    expectedResult.setErrorMessage("");
    assertThatCode(() -> dataStorage.put(jobId, expectedResult)).doesNotThrowAnyException();
    var actualResult = dataStorage.get(jobId, ResultDTO.class);
    assertThat(actualResult.isPresent() ? actualResult.get() : actualResult)
        .isEqualTo(expectedResult);
  }

  @Test
  void testGetNull() {
    assertThat(dataStorage.get("random key", ResultDTO.class)).isEmpty();
  }
}
