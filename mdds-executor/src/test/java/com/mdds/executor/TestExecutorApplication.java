/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.queue.Message;
import com.mdds.queue.Queue;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Slf4j
@SpringBootTest(
    classes = ExecutorApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class TestExecutorApplication {
  @Autowired
  @Qualifier("taskQueue")
  private Queue taskQueue;

  @Autowired
  @Qualifier("resultQueue")
  private Queue resultQueue;

  @Autowired private CommonProperties commonProperties;

  @Autowired private ExecutorService executorService;

  private static final String MDDS_EXECUTOR_HOST = "localhost";
  private static final int MDDS_EXECUTOR_PORT = findFreePort();
  private static final String MDDS_GRPC_CORE = "mdds_grpc_core";
  private static final String MDDS_HOME = File.separator + "opt" + File.separator + "mdds";
  private static final String WORK_DIR = MDDS_HOME + File.separator + MDDS_GRPC_CORE;
  private static final String GRPC_SERVER_HOST = "0.0.0.0";
  private static final int GRPC_SERVER_PORT = 50051;
  private static final RabbitMQContainer rabbitMq;
  private static final GenericContainer<?> grpcServer;

  static {
    rabbitMq =
        new RabbitMQContainer("rabbitmq:3.12-management")
            .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
            .withExposedPorts(5672, 15672)
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(Duration.ofSeconds(30));

    grpcServer =
        new GenericContainer<>(DockerImageName.parse("mddsproject/grpc-server:0.1.0"))
            .withEnv("MDDS_EXECUTOR_GRPC_SERVER_HOST", GRPC_SERVER_HOST)
            .withEnv("MDDS_EXECUTOR_GRPC_SERVER_PORT", String.valueOf(GRPC_SERVER_PORT))
            .withWorkingDirectory(WORK_DIR)
            .withExposedPorts(GRPC_SERVER_PORT)
            .waitingFor(
                Wait.forSuccessfulCommand(
                    "grpc_health_probe -addr=" + GRPC_SERVER_HOST + ":" + GRPC_SERVER_PORT));

    rabbitMq.start();
    log.info("RabbitMq container is ready {}:{}", rabbitMq.getHost(), rabbitMq.getAmqpPort());

    grpcServer.start();
    log.info(
        "gRPC container is ready {}:{}",
        grpcServer.getHost(),
        grpcServer.getMappedPort(GRPC_SERVER_PORT));
  }

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
    registry.add("mdds.executor.grpc.server.host", grpcServer::getHost);
    registry.add(
        "mdds.executor.grpc.server.port",
        () -> String.valueOf(grpcServer.getMappedPort(GRPC_SERVER_PORT)));
    registry.add("mdds.executor.host", () -> MDDS_EXECUTOR_HOST);
    registry.add("mdds.executor.port", () -> String.valueOf(MDDS_EXECUTOR_PORT));
  }

  @AfterAll
  static void tearDown() {
    grpcServer.stop();
    rabbitMq.stop();
  }

  @Test
  void testHealthReturnsStatusOk() throws IOException, InterruptedException {
    HttpResponse<Void> response;
    try (var client = HttpClient.newHttpClient()) {
      var request =
          HttpRequest.newBuilder()
              .uri(
                  URI.create("http://" + MDDS_EXECUTOR_HOST + ":" + MDDS_EXECUTOR_PORT + "/health"))
              .GET()
              .build();
      response = client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding());
    }
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_OK);
  }

  @Test
  void testExecutor() {
    // Prepare and put data to task queue
    var taskId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var task =
        new TaskDTO(
            taskId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4},
              {51, 24.2, 33.3, 34.24},
              {31.1, 232.2, 43.3, 4.4},
              {62.1, 78.2, 92.3, 122.4}
            },
            new double[] {4.3, 3.23, 5.324, 4.553},
            SlaeSolver.NUMPY_EXACT_SOLVER);

    var taskMessage = new Message<>(task, new HashMap<>(), Instant.now());
    taskQueue.publish(commonProperties.getTaskQueueName(), taskMessage);
    log.info(
        "Published taskMessage = {} to '{}', {}",
        taskMessage,
        commonProperties.getTaskQueueName(),
        taskQueue);
    var actual = waitForResult(taskId, resultQueue);
    log.info("actualResult = {}", actual);
    var endTime = actual.getDateTimeTaskFinished();
    var expected =
        new ResultDTO(
            taskId,
            startTime,
            endTime,
            TaskStatus.DONE,
            "cancel.queue-executor-0001",
            100,
            new double[] {
              -2.8019496130141808, -1.9729062026984527, 13.471272875276737, -7.439241424582051
            },
            "");
    assertSolution(expected, actual);
  }

  private static Stream<SlaeSolver> solvers() {
    return Stream.of(
        SlaeSolver.NUMPY_EXACT_SOLVER,
        SlaeSolver.NUMPY_PINV_SOLVER,
        SlaeSolver.PETSC_SOLVER,
        SlaeSolver.NUNPY_LSTSQ_SOLVER,
        SlaeSolver.SCIPY_GMERS_SOLVER);
  }

  @ParameterizedTest
  @MethodSource("solvers")
  void testExecutorAllSolvers(SlaeSolver slaeSolver) {
    // Prepare and put data to task queue
    var taskId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var task =
        new TaskDTO(
            taskId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4},
              {51, 24.2, 33.3, 34.24},
              {31.1, 232.2, 43.3, 4.4},
              {62.1, 78.2, 92.3, 122.4}
            },
            new double[] {4.3, 3.23, 5.324, 4.553},
            slaeSolver);

    var taskMessage = new Message<>(task, new HashMap<>(), Instant.now());
    taskQueue.publish(commonProperties.getTaskQueueName(), taskMessage);
    var actual = waitForResult(taskId, resultQueue);

    var endTime = actual.getDateTimeTaskFinished();
    var expected =
        new ResultDTO(
            taskId,
            startTime,
            endTime,
            TaskStatus.DONE,
            "cancel.queue-executor-0001",
            100,
            new double[] {
              -2.8019496130141808, -1.9729062026984527, 13.471272875276737, -7.439241424582051
            },
            "");

    assertSolution(expected, actual);
  }

  @Test
  void testExecutorWithErrorInInputData() {
    // Prepare and put data to task queue
    var taskId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var task =
        new TaskDTO(
            taskId,
            startTime,
            new double[][] {
              {
                1.1, 2.2,
              },
              {51, 24.2, 34.24},
              {31.1, 232.2, 43.3, 4.4},
              {62.1, 78.2, 92.3, 122.4, 4.54}
            },
            new double[] {4.3, 3.23, 5.324, 4.553},
            SlaeSolver.NUMPY_EXACT_SOLVER);

    var taskMessage = new Message<>(task, new HashMap<>(), Instant.now());
    taskQueue.publish(commonProperties.getTaskQueueName(), taskMessage);

    var actual = waitForResult(taskId, resultQueue);
    var endTime = actual.getDateTimeTaskFinished();
    var expected =
        new ResultDTO(
            taskId,
            startTime,
            endTime,
            TaskStatus.ERROR,
            "cancel.queue-executor-001",
            50,
            new double[] {},
            "");

    assertSolution(expected, actual);
    assertThat(actual.getErrorMessage())
        .contains("Unexpected <class 'ValueError'>: setting an array element with a sequence.");
  }

  private ResultDTO waitForResult(String taskId, Queue resultQueue) {
    var results = new CopyOnWriteArrayList<ResultDTO>();

    try (var ignored =
        resultQueue.subscribe(
            commonProperties.getResultQueueName(),
            ResultDTO.class,
            (message, ack) -> {
              var payload = message.payload();
              if (taskId.equals(payload.getTaskId())) {
                results.add(payload);
              }
              ack.ack();
            })) {

      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> results.size() == 2);
      return results.getLast();

    } catch (Exception e) {
      throw new AssertionError("Failed to receive result for taskId = " + taskId, e);
    }
  }

  private static void assertSolution(ResultDTO expected, ResultDTO actual) {
    assertThat(actual.getTaskId()).isEqualTo(expected.getTaskId());
    assertThat(actual.getDateTimeTaskCreated()).isEqualTo(expected.getDateTimeTaskCreated());
    assertThat(actual.getTaskStatus()).isEqualTo(expected.getTaskStatus());
    assertThat(actual.getSolution()).containsExactly(expected.getSolution(), offset(1e-6));
  }
}
