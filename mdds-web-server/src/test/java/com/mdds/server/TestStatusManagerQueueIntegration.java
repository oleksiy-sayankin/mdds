/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.domain.JobStatus.IN_PROGRESS;
import static com.mdds.domain.JobStatus.SUBMITTED;
import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.CommonProperties;
import com.mdds.dto.JobStatusUpdateDTO;
import com.mdds.queue.Message;
import com.mdds.queue.QueueClient;
import com.mdds.server.jpa.JobsRepository;
import com.mdds.server.support.JobTestFixture;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@SpringBootTest(properties = "mdds.common.status-queue-name=status.queue.test.${random.uuid}")
@Testcontainers
@Import(JobTestFixture.class)
class TestStatusManagerQueueIntegration {

  private static final Instant BASE_EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @Container
  private static final RabbitMQContainer RABBIT_MQ =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672)
          .waitingFor(Wait.forListeningPort())
          .withStartupTimeout(Duration.ofSeconds(30));

  @Autowired private JobCreationService jobCreationService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobTestFixture jobFixture;
  @Autowired private JobsRepository jobsRepository;
  @Autowired private StatusManagerService statusManagerService;
  @Autowired private CommonProperties commonProperties;

  @Autowired
  @Qualifier("statusQueueClient")
  private QueueClient statusQueueClient;

  @DynamicPropertySource
  static void registerProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", RABBIT_MQ::getHost);
    registry.add("mdds.rabbitmq.port", RABBIT_MQ::getAmqpPort);
    registry.add("mdds.rabbitmq.user", RABBIT_MQ::getAdminUsername);
    registry.add("mdds.rabbitmq.password", RABBIT_MQ::getAdminPassword);
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testProcessStatusUpdateMessage(String login) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var workerId = newWorkerId();
    var eventTime = BASE_EVENT_TIME;
    var progress = 10;
    var queueName = commonProperties.getStatusQueueName();
    var message = "Started processing";
    assertThat(statusManagerService).isNotNull();
    statusQueueClient.publish(
        queueName,
        new Message<>(
            new JobStatusUpdateDTO(
                jobId, workerId, IN_PROGRESS.getCode(), progress, message, eventTime),
            Map.of(),
            eventTime));
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              var job = jobsRepository.findById(jobId).orElseThrow();
              assertThat(job.getStatus()).isEqualTo(IN_PROGRESS);
            });

    var job = jobsRepository.findById(jobId).orElseThrow();
    assertThat(job.getWorkerId()).isEqualTo(workerId);
    assertThat(job.getProgress()).isEqualTo(progress);
    assertThat(job.getMessage()).isEqualTo(message);
    var before = eventTime.minusMillis(10);
    var after = eventTime.plusMillis(10);
    assertThat(job.getStartedAt()).isBetween(before, after);
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private JobCreationResult createOrReuseDraftJob(long userId, String sessionId, String jobType) {
    return jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType);
  }

  private static String newWorkerId() {
    return "worker-" + UUID.randomUUID();
  }
}
