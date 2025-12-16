/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import com.mdds.common.util.HttpTestClient;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskStatus;
import com.rabbitmq.client.ConnectionFactory;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.io.*;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Slf4j
@Testcontainers
public class BaseEnvironment {
  private static final String GRPC_SERVER_HOST = "grpcserver";
  private static final int GRPC_SERVER_PORT = 50051;

  private static final String MDDS_EXECUTOR_HOST = "executor";
  private static final int MDDS_EXECUTOR_PORT = 35232;

  private static final String MDDS_WEB_SERVER_HOST = "webserver";
  protected static final int MDDS_WEB_SERVER_PORT = 8000;

  private static final String MDDS_RESULT_CONSUMER_HOST = "resultconsumer";
  private static final int MDDS_RESULT_CONSUMER_PORT = 8863;

  private static final String REDIS_HOST = "redis";
  private static final int REDIS_PORT = 6379;

  private static final String RABBITMQ_HOST = "rabbitmq";
  private static final int RABBITMQ_PORT = 5672;

  private static final String MDDS_GRPC_CORE = "mdds_grpc_core";
  private static final String MDDS_HOME = File.separator + "opt" + File.separator + "mdds";
  private static final String GRPC_CORE_WORK_DIR = MDDS_HOME + File.separator + MDDS_GRPC_CORE;

  protected static final Network SHARED_NETWORK = Network.newNetwork();
  protected static HttpTestClient webServerClient =
      new HttpTestClient(MDDS_WEB_SERVER_HOST, MDDS_WEB_SERVER_PORT);

  @Container
  private static final RabbitMQContainer RABBIT_MQ =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withNetwork(SHARED_NETWORK)
          .withNetworkAliases(RABBITMQ_HOST)
          .waitingFor(Wait.forListeningPort())
          .withExposedPorts(RABBITMQ_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7.4"))
          .withNetwork(SHARED_NETWORK)
          .waitingFor(Wait.forListeningPort())
          .withNetworkAliases(REDIS_HOST)
          .withExposedPorts(REDIS_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> GRPC_SERVER =
      new GenericContainer<>(DockerImageName.parse("mddsproject/grpc-server:0.1.0"))
          .withNetwork(SHARED_NETWORK)
          .withNetworkAliases(GRPC_SERVER_HOST)
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_HOST", GRPC_SERVER_HOST)
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_PORT", String.valueOf(GRPC_SERVER_PORT))
          .withWorkingDirectory(GRPC_CORE_WORK_DIR)
          .waitingFor(
              Wait.forSuccessfulCommand(
                  "grpc_health_probe -addr=" + GRPC_SERVER_HOST + ":" + GRPC_SERVER_PORT))
          .withExposedPorts(GRPC_SERVER_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> EXECUTOR =
      new GenericContainer<>(DockerImageName.parse("mddsproject/executor:0.1.0"))
          .withNetwork(SHARED_NETWORK)
          .withNetworkAliases(MDDS_EXECUTOR_HOST)
          .dependsOn(RABBIT_MQ, GRPC_SERVER)
          .withEnv("MDDS_EXECUTOR_HOST", MDDS_EXECUTOR_HOST)
          .withEnv("MDDS_EXECUTOR_PORT", String.valueOf(MDDS_EXECUTOR_PORT))
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_HOST", GRPC_SERVER_HOST)
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_PORT", String.valueOf(GRPC_SERVER_PORT))
          .withEnv("MDDS_RABBITMQ_HOST", RABBITMQ_HOST)
          .withEnv("MDDS_RABBITMQ_PORT", String.valueOf(RABBITMQ_PORT))
          .withEnv("MDDS_RABBITMQ_USER", RABBIT_MQ.getAdminUsername())
          .withEnv("MDDS_RABBITMQ_PASSWORD", RABBIT_MQ.getAdminPassword())
          .waitingFor(Wait.forListeningPort())
          .withExposedPorts(MDDS_EXECUTOR_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> RESULT_CONSUMER =
      new GenericContainer<>(DockerImageName.parse("mddsproject/result-consumer:0.1.0"))
          .withNetwork(SHARED_NETWORK)
          .withNetworkAliases(MDDS_RESULT_CONSUMER_HOST)
          .dependsOn(RABBIT_MQ, REDIS)
          .withEnv("MDDS_RABBITMQ_HOST", RABBITMQ_HOST)
          .withEnv("MDDS_RABBITMQ_PORT", String.valueOf(RABBITMQ_PORT))
          .withEnv("MDDS_RABBITMQ_USER", RABBIT_MQ.getAdminUsername())
          .withEnv("MDDS_RABBITMQ_PASSWORD", RABBIT_MQ.getAdminPassword())
          .withEnv("MDDS_REDIS_HOST", REDIS_HOST)
          .withEnv("MDDS_REDIS_PORT", String.valueOf(REDIS_PORT))
          .withEnv("MDDS_RESULT_CONSUMER_HOST", MDDS_RESULT_CONSUMER_HOST)
          .withEnv("MDDS_RESULT_CONSUMER_PORT", String.valueOf(MDDS_RESULT_CONSUMER_PORT))
          .waitingFor(Wait.forListeningPort())
          .withExposedPorts(MDDS_RESULT_CONSUMER_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> WEB_SERVER =
      new GenericContainer<>(DockerImageName.parse("mddsproject/web-server:0.1.0"))
          .withNetwork(SHARED_NETWORK)
          .withNetworkAliases(MDDS_WEB_SERVER_HOST)
          .dependsOn(RABBIT_MQ, REDIS, EXECUTOR, RESULT_CONSUMER)
          .withEnv("MDDS_SERVER_HOST", MDDS_WEB_SERVER_HOST)
          .withEnv("MDDS_SERVER_PORT", String.valueOf(MDDS_WEB_SERVER_PORT))
          .withEnv("MDDS_RABBITMQ_HOST", RABBITMQ_HOST)
          .withEnv("MDDS_RABBITMQ_PORT", String.valueOf(RABBITMQ_PORT))
          .withEnv("MDDS_RABBITMQ_USER", RABBIT_MQ.getAdminUsername())
          .withEnv("MDDS_RABBITMQ_PASSWORD", RABBIT_MQ.getAdminPassword())
          .withEnv("MDDS_REDIS_HOST", REDIS_HOST)
          .withEnv("MDDS_REDIS_PORT", String.valueOf(REDIS_PORT))
          .waitingFor(Wait.forListeningPort())
          .withExposedPorts(MDDS_WEB_SERVER_PORT);

  @BeforeAll
  static void init() {
    EXECUTOR.followOutput(
        outputFrame -> log.info("[EXECUTOR] {}", outputFrame.getUtf8String().trim()));
    WEB_SERVER.followOutput(
        outputFrame -> log.info("[WEB-SERVER] {}", outputFrame.getUtf8String().trim()));
    RESULT_CONSUMER.followOutput(
        outputFrame -> log.info("[RESULT-CONSUMER] {}", outputFrame.getUtf8String().trim()));
    awaitReady(BaseEnvironment::grpcServerIsReady, "gRPC Server");
    awaitReady(BaseEnvironment::queueIsReady, "RabbitMq");
    awaitReady(BaseEnvironment::redisIsReady, "Redis");
    awaitReady(() -> checkHealth(EXECUTOR, MDDS_EXECUTOR_PORT), "ExecutorApplication");
    awaitReady(() -> checkHealth(RESULT_CONSUMER, MDDS_RESULT_CONSUMER_PORT), "Result Consumer");
    awaitReady(() -> checkHealth(WEB_SERVER, MDDS_WEB_SERVER_PORT), "Web Server");
    webServerClient =
        new HttpTestClient(WEB_SERVER.getHost(), WEB_SERVER.getMappedPort(MDDS_WEB_SERVER_PORT));
  }

  protected static void awaitReady(Callable<Boolean> condition, String name) {
    Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().until(condition);
    log.info("{} container is ready", name);
  }

  private static boolean checkHealth(GenericContainer<?> container, int port)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(container.getHost(), container.getMappedPort(port));
    var response = http.get("/health");
    return response.statusCode() == HttpURLConnection.HTTP_OK;
  }

  private static boolean queueIsReady() throws IOException, TimeoutException {
    try (var connection =
        createConnectionFactory(
                RABBIT_MQ.getHost(),
                RABBIT_MQ.getAmqpPort(),
                RABBIT_MQ.getAdminUsername(),
                RABBIT_MQ.getAdminPassword())
            .newConnection()) {
      return connection.isOpen();
    }
  }

  private static boolean grpcServerIsReady() {
    var channel =
        ManagedChannelBuilder.forAddress(
                GRPC_SERVER.getHost(), GRPC_SERVER.getMappedPort(GRPC_SERVER_PORT))
            .usePlaintext()
            .build();
    var stub = HealthGrpc.newBlockingStub(channel);
    var response = stub.check(HealthCheckRequest.newBuilder().build());
    var result = HealthCheckResponse.ServingStatus.SERVING.equals(response.getStatus());
    channel.shutdownNow();
    return result;
  }

  protected static ResultDTO awaitForResult(String taskId) {
    // Request result using endpoint
    return Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(
            () -> {
              var response = webServerClient.get("/result/" + taskId);
              return JsonHelper.fromJson(response.body(), ResultDTO.class);
            },
            result -> TaskStatus.DONE.equals(result.getTaskStatus()));
  }

  protected static void assertDoneAndEquals(double[] expected, ResultDTO actual) {
    Assertions.assertThat(actual.getTaskStatus()).isEqualTo(TaskStatus.DONE);
    var delta = 0.00000001;
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual.getSolution(), delta);
  }

  private static boolean redisIsReady() throws IOException, InterruptedException {
    var result = REDIS.execInContainer("redis-cli", "ping");
    return result.getExitCode() == 0 && "PONG".equals(result.getStdout().trim());
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

  protected static Stream<SlaeSolver> solvers() {
    return Stream.of(
        SlaeSolver.NUMPY_EXACT_SOLVER,
        SlaeSolver.NUMPY_PINV_SOLVER,
        SlaeSolver.PETSC_SOLVER,
        SlaeSolver.NUNPY_LSTSQ_SOLVER,
        SlaeSolver.SCIPY_GMERS_SOLVER);
  }
}
