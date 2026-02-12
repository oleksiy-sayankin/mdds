/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.queue.Message;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import redis.embedded.RedisServer;

@Slf4j
@SpringBootTest
class TestResultConsumerApplicationService {
  private static final String HOST = "localhost";
  private static final int REDIS_PORT = findFreePort();
  private static final RedisServer redisServer;
  private static final RabbitMQContainer rabbitMq;
  @Autowired private DataStorage storage;
  @Autowired private ResultConsumerService resultConsumerApplication;
  @Autowired private CommonProperties commonProperties;

  @Autowired
  @Qualifier("resultQueue")
  private Queue queue;

  static {
    rabbitMq =
        new RabbitMQContainer("rabbitmq:3.12-management")
            .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
            .withExposedPorts(5672, 15672)
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(Duration.ofSeconds(30));
    rabbitMq.start();
    log.info("RabbitMq container is ready {}:{}", rabbitMq.getHost(), rabbitMq.getAmqpPort());
    try {
      redisServer = new RedisServer(REDIS_PORT);
      redisServer.start();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    log.info("Embedded Redis Server is ready {}:{}", HOST, REDIS_PORT);
  }

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
    registry.add("mdds.redis.host", () -> HOST);
    registry.add("mdds.redis.port", () -> String.valueOf(REDIS_PORT));
  }

  @AfterAll
  static void stopServer() throws IOException {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  void testResultConsumerService() {
    // Prepare and put data to result queue
    var jobId = UUID.randomUUID().toString();
    var expectedResult =
        new ResultDTO(
            jobId,
            Instant.now(),
            Instant.now(),
            JobStatus.DONE,
            "cancel.queue-executor-0001",
            100,
            new double[] {1.1, 2.2, 3.3, 4.4},
            "");
    var message = new Message<>(expectedResult, new HashMap<>(), Instant.now());
    queue.publish(commonProperties.getResultQueueName(), message);
    // Read result from Data Storage
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              var actualResult = storage.get(jobId, ResultDTO.class);
              assertThat(actualResult).isPresent();
              actualResult.ifPresent(ar -> assertThat(expectedResult).isEqualTo(ar));
            });
  }
}
