/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.junit.jupiter.api.Assertions.*;

import com.mdds.common.AppConstants;
import com.mdds.common.AppConstantsFactory;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.queue.*;
import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import com.rabbitmq.client.ConnectionFactory;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Slf4j
@Testcontainers
class TestExecutor {
  private static Tomcat executor;
  private static final String MDDS_EXECUTOR_HOST =
      ExecutorConfFactory.fromEnvOrDefaultProperties().executorHost();
  private static final int MDDS_EXECUTOR_PORT = findFreePort();
  private static final String MDDS_EXECUTOR_APPLICATION_LOCATION =
      ExecutorConfFactory.fromEnvOrDefaultProperties().webappDirLocation();
  private static Queue resultQueue;
  private static Queue taskQueue;
  private static final String MDDS_GRPC_CORE = "mdds_grpc_core";
  private static final String MDDS_HOME = File.separator + "opt" + File.separator + "mdds";
  private static final String WORK_DIR = MDDS_HOME + File.separator + MDDS_GRPC_CORE;
  private static final int GRPC_SERVER_PORT = 50051;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @Container
  private static final GenericContainer<?> grpcServer =
      new GenericContainer<>(DockerImageName.parse("mddsproject/mdds:test"))
          .withWorkingDirectory(WORK_DIR)
          .withExposedPorts(GRPC_SERVER_PORT);

  @BeforeAll
  static void startServer() throws LifecycleException {
    // Wait for RabbitMq is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .ignoreExceptions()
        .until(TestExecutor::queueIsReady);
    log.info("RabbitMq container is ready");

    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());

    // Connect to Queue
    resultQueue = new RabbitMqQueueProvider().get();
    taskQueue = new RabbitMqQueueProvider().get();

    // Wait for gRPC Server is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .ignoreExceptions()
        .until(TestExecutor::grpcServerIsReady);
    log.info("gRPC Server is ready");

    System.setProperty("mdds.executor.grpc.server.host", grpcServer.getHost());
    System.setProperty(
        "mdds.executor.grpc.server.port",
        String.valueOf(grpcServer.getMappedPort(GRPC_SERVER_PORT)));

    // Start Executor
    executor =
        Executor.start(MDDS_EXECUTOR_HOST, MDDS_EXECUTOR_PORT, MDDS_EXECUTOR_APPLICATION_LOCATION);

    // Wait for Executor is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .ignoreExceptions()
        .until(TestExecutor::executorIsReady);
    log.info("Executor is ready");
  }

  @AfterAll
  static void stopServer() throws LifecycleException {
    if (executor != null) {
      executor.stop();
      log.info("Executor is stopped");
      executor.destroy();
      log.info("Executor is destroyed");
    }
    if (taskQueue != null) {
      taskQueue.close();
      log.info("Task Queue is closed");
    }
    if (resultQueue != null) {
      resultQueue.close();
      log.info("Result Queue is closed");
    }
  }

  @Test
  void testHealthReturnsStatusOk() throws URISyntaxException, IOException {
    var uri = new URI("http://" + MDDS_EXECUTOR_HOST + ":" + MDDS_EXECUTOR_PORT + "/health");
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
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
    taskQueue.publish(AppConstantsFactory.getString(AppConstants.TASK_QUEUE_NAME), taskMessage);
    log.info("Published taskMessage = {}", taskMessage);

    AtomicReference<ResultDTO> actualResultReference = new AtomicReference<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(
            () -> {
              var checkResultMessageHandler =
                  new MessageHandler<ResultDTO>() {
                    @Override
                    public void handle(
                        @Nonnull Message<ResultDTO> message, @Nonnull Acknowledger ack) {
                      // Set the result here
                      actualResultReference.set(message.payload());
                    }
                  };
              try (var ignored =
                  resultQueue.subscribe(
                      AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME),
                      ResultDTO.class,
                      checkResultMessageHandler)) {
                // Do nothing
              }
              return isAssigned(actualResultReference);
            });

    log.info("actualResult = {}", actualResultReference);
    var delta = 0.000001;
    var endTime = actualResultReference.get().getDateTimeTaskFinished();
    var expectedResult =
        new ResultDTO(
            taskId,
            startTime,
            endTime,
            TaskStatus.DONE,
            new double[] {
              -2.8019496130141808, -1.9729062026984527, 13.471272875276737, -7.439241424582051
            },
            "");
    var actualResult = actualResultReference.get();
    assertEquals(expectedResult.getTaskId(), actualResult.getTaskId());
    assertEquals(expectedResult.getDateTimeTaskCreated(), actualResult.getDateTimeTaskCreated());
    assertEquals(expectedResult.getTaskStatus(), actualResult.getTaskStatus());
    assertArrayEquals(expectedResult.getSolution(), actualResult.getSolution(), delta);
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
    taskQueue.publish(AppConstantsFactory.getString(AppConstants.TASK_QUEUE_NAME), taskMessage);

    AtomicReference<ResultDTO> actualResultReference = new AtomicReference<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(
            () -> {
              var checkResultMessageHandler =
                  new MessageHandler<ResultDTO>() {
                    @Override
                    public void handle(
                        @Nonnull Message<ResultDTO> message, @Nonnull Acknowledger ack) {
                      // Set the result here
                      actualResultReference.set(message.payload());
                    }
                  };
              try (var ignored =
                  resultQueue.subscribe(
                      AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME),
                      ResultDTO.class,
                      checkResultMessageHandler)) {
                // Do nothing
              }
              return isAssigned(actualResultReference);
            });

    var delta = 0.000001;
    var endTime = actualResultReference.get().getDateTimeTaskFinished();
    var expectedResult =
        new ResultDTO(
            taskId,
            startTime,
            endTime,
            TaskStatus.DONE,
            new double[] {
              -2.8019496130141808, -1.9729062026984527, 13.471272875276737, -7.439241424582051
            },
            "");
    var actualResult = actualResultReference.get();
    assertEquals(expectedResult.getTaskId(), actualResult.getTaskId());
    assertEquals(expectedResult.getDateTimeTaskCreated(), actualResult.getDateTimeTaskCreated());
    assertEquals(expectedResult.getTaskStatus(), actualResult.getTaskStatus());
    assertArrayEquals(expectedResult.getSolution(), actualResult.getSolution(), delta);
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
    taskQueue.publish(AppConstantsFactory.getString(AppConstants.TASK_QUEUE_NAME), taskMessage);

    AtomicReference<ResultDTO> actualResultReference = new AtomicReference<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(
            () -> {
              var checkResultMessageHandler =
                  new MessageHandler<ResultDTO>() {
                    @Override
                    public void handle(
                        @Nonnull Message<ResultDTO> message, @Nonnull Acknowledger ack) {
                      // Set the result here
                      actualResultReference.set(message.payload());
                    }
                  };
              try (var ignored =
                  resultQueue.subscribe(
                      AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME),
                      ResultDTO.class,
                      checkResultMessageHandler)) {
                // Do nothing
              }
              return isAssigned(actualResultReference);
            });

    var delta = 0.000001;
    var endTime = actualResultReference.get().getDateTimeTaskFinished();
    var expectedResult =
        new ResultDTO(taskId, startTime, endTime, TaskStatus.ERROR, new double[] {}, "");
    var actualResult = actualResultReference.get();
    assertEquals(expectedResult.getTaskId(), actualResult.getTaskId());
    assertEquals(expectedResult.getDateTimeTaskCreated(), actualResult.getDateTimeTaskCreated());
    assertEquals(expectedResult.getTaskStatus(), actualResult.getTaskStatus());
    assertArrayEquals(expectedResult.getSolution(), actualResult.getSolution(), delta);
    assertTrue(
        actualResult
            .getErrorMessage()
            .contains(
                "Unexpected <class 'ValueError'>: setting an array element with a sequence."));
  }

  private static boolean isAssigned(AtomicReference<ResultDTO> actualResult) {
    return actualResult.get() != null;
  }

  private static boolean executorIsReady() throws IOException, URISyntaxException {
    var uri = new URI("http://" + MDDS_EXECUTOR_HOST + ":" + MDDS_EXECUTOR_PORT + "/health");
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    return HttpURLConnection.HTTP_OK == connection.getResponseCode();
  }

  private static boolean queueIsReady() throws IOException, TimeoutException {
    try (var connection =
        createConnectionFactory(
                rabbitMq.getHost(),
                rabbitMq.getAmqpPort(),
                rabbitMq.getAdminUsername(),
                rabbitMq.getAdminPassword())
            .newConnection()) {
      return connection.isOpen();
    }
  }

  private static boolean grpcServerIsReady() {
    var channel =
        ManagedChannelBuilder.forAddress(
                grpcServer.getHost(), grpcServer.getMappedPort(GRPC_SERVER_PORT))
            .usePlaintext()
            .build();
    var stub = HealthGrpc.newBlockingStub(channel);
    var response = stub.check(HealthCheckRequest.newBuilder().build());
    var result = HealthCheckResponse.ServingStatus.SERVING.equals(response.getStatus());
    channel.shutdownNow();
    return result;
  }

  private static ConnectionFactory createConnectionFactory(
      String host, int port, String user, String password) {
    var factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(user);
    factory.setPassword(password);
    return factory;
  }
}
