/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.grpc.solver.SolveResponse;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import io.grpc.ManagedChannel;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
class TestExecutorApplicationService {
  @Autowired
  @Qualifier("taskQueue")
  private Queue taskQueue;

  @Autowired
  @Qualifier("resultQueue")
  private Queue resultQueue;

  @Autowired private GrpcServerProperties grpcServerConfig;

  @Autowired private CommonProperties commonProperties;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @DynamicPropertySource
  static void rabbitProps(DynamicPropertyRegistry registry) {
    if (!rabbitMq.isRunning()) {
      rabbitMq.start();
    }
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
  }

  @Test
  void testExecutorService() {
    // Prepare and put data to task queue
    var taskId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var task =
        new TaskDTO(
            taskId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var endTime = Instant.now();
    var expectedResult =
        new ResultDTO(
            taskId,
            startTime,
            endTime,
            TaskStatus.DONE,
            100,
            new double[] {1.971, 3.213, 7.243},
            "");

    // Simulate that ExecutorMessageHandler solves the task
    MessageHandler<TaskDTO> messageHandler =
        (message, ack) -> {
          var resultMessage = new Message<>(expectedResult, new HashMap<>(), Instant.now());
          resultQueue.publish(commonProperties.getResultQueueName(), resultMessage);
          ack.ack();
        };
    try (var executorService =
        new ExecutorService(
            taskQueue,
            resultQueue,
            messageHandler,
            taskQueue.subscribe(commonProperties.getTaskQueueName(), TaskDTO.class, messageHandler),
            commonProperties)) {
      var taskMessage = new Message<>(task, new HashMap<>(), Instant.now());
      taskQueue.publish(commonProperties.getTaskQueueName(), taskMessage);

      Awaitility.await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                var checkResultMessageHandler =
                    new MessageHandler<ResultDTO>() {
                      @Override
                      public void handle(
                          @Nonnull Message<ResultDTO> message, @Nonnull Acknowledger ack) {
                        // Check the result here
                        var actualResult = message.payload();
                        assertEquals(expectedResult, actualResult);
                      }
                    };
                try (var ignored =
                    resultQueue.subscribe(
                        commonProperties.getResultQueueName(),
                        ResultDTO.class,
                        checkResultMessageHandler)) {
                  // Do nothing
                }
              });
    }
  }

  @Test
  void testExecutorMessageHandlerWithMock() {
    // given
    var mockedResultQueue = mock(Queue.class);
    var mockedSolverStub = mock(SolverServiceGrpc.SolverServiceBlockingStub.class);
    var mockedChanel = mock(ManagedChannel.class);

    when(mockedSolverStub.solve(any()))
        .thenReturn(
            SolveResponse.newBuilder()
                .addSolution(1.371)
                .addSolution(3.283)
                .addSolution(3.243)
                .build());

    var handler =
        new ExecutorMessageHandler(
            mockedResultQueue, mockedSolverStub, mockedChanel, grpcServerConfig, commonProperties);

    // Prepare and put data to task queue
    var taskId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var task =
        new TaskDTO(
            taskId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var ack = mock(Acknowledger.class);
    var taskMessage = new Message<>(task, new HashMap<>(), Instant.now());
    // when
    handler.handle(taskMessage, ack);

    // then
    verify(mockedResultQueue).publish(anyString(), any());
    verify(ack).ack();
  }
}
