/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.mdds.common.AppConstants;
import com.mdds.common.AppConstantsFactory;
import com.mdds.grpc.solver.SolveResponse;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import dto.ResultDTO;
import dto.SlaeSolver;
import dto.TaskDTO;
import dto.TaskStatus;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class TestExecutorService {
  private static Queue taskQueue;
  private static Queue resultQueue;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @BeforeAll
  static void startServer() {
    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());
    taskQueue = new RabbitMqQueueProvider().get();
    resultQueue = new RabbitMqQueueProvider().get();
  }

  @AfterAll
  static void stopServer() {
    if (taskQueue != null) {
      taskQueue.close();
    }
    if (resultQueue != null) {
      resultQueue.close();
    }
  }

  @Test
  void testExecutorService() {
    var messageHandler = mock(ExecutorMessageHandler.class);
    try (var executorService = new ExecutorService(taskQueue, resultQueue, messageHandler)) {
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
      var taskMessage = new Message<>(task, new HashMap<>(), Instant.now());
      taskQueue.publish(AppConstantsFactory.getString(AppConstants.TASK_QUEUE_NAME), taskMessage);

      var endTime = Instant.now();
      var expectedResult =
          new ResultDTO(
              taskId, startTime, endTime, TaskStatus.DONE, new double[] {1.971, 3.213, 7.243}, "");

      // Simulate that ExecutorMessageHandler solves the task
      doAnswer(
              invocation -> {
                var resultMessage = new Message<>(expectedResult, new HashMap<>(), Instant.now());
                resultQueue.publish(
                    AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), resultMessage);
                return null;
              })
          .when(messageHandler)
          .handle(any(), any());

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
                        AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME),
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
    Queue resultQueue = mock(Queue.class);
    SolverServiceGrpc.SolverServiceBlockingStub solverStub = mock(SolverServiceGrpc.SolverServiceBlockingStub.class);

    when(solverStub.solve(any()))
        .thenReturn(
            SolveResponse.newBuilder()
                .addSolution(1.371)
                .addSolution(3.283)
                .addSolution(3.243)
                .build()
        );

    ExecutorMessageHandler handler = new ExecutorMessageHandler(resultQueue, solverStub);

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
    Acknowledger ack = mock(Acknowledger.class);
    var taskMessage = new Message<>(task, new HashMap<>(), Instant.now());
    // when
    handler.handle(taskMessage, ack);

    // then
    verify(resultQueue).publish(anyString(), any());
    verify(ack).ack();
  }

}
