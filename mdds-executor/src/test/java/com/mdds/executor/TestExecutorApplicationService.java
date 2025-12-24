/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
@SpringBootTest
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

  @MockitoBean private ExecutorMessageHandler executorMessageHandler;

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
    var expected =
        new ResultDTO(
            taskId,
            startTime,
            endTime,
            TaskStatus.DONE,
            100,
            new double[] {1.971, 3.213, 7.243},
            "");

    // Simulate that ExecutorMessageHandler solves the task
    doAnswer(
            invocation -> {
              var resultMessage = new Message<>(expected, new HashMap<>(), Instant.now());
              resultQueue.publish(commonProperties.getResultQueueName(), resultMessage);
              return null;
            })
        .when(executorMessageHandler)
        .handle(any(), any());

    try (var executorService =
        new ExecutorService(
            taskQueue,
            resultQueue,
            executorMessageHandler,
            taskQueue.subscribe(
                commonProperties.getTaskQueueName(), TaskDTO.class, executorMessageHandler),
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
                        var actual = message.payload();
                        assertThat(actual).isEqualTo(expected);
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
    var mockedSolverStub = mock(SolverServiceGrpc.SolverServiceFutureStub.class);
    var mockedChanel = mock(ManagedChannel.class);
    var response =
        SolveResponse.newBuilder().addSolution(1.371).addSolution(3.283).addSolution(3.243).build();

    when(mockedSolverStub.solve(any()))
        .thenReturn(
            new ListenableFuture<SolveResponse>() {
              @Override
              public void addListener(@NonNull Runnable listener, @NonNull Executor executor) {
                throw new UnsupportedOperationException();
              }

              @Override
              public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
              }

              @Override
              public boolean isCancelled() {
                return false;
              }

              @Override
              public boolean isDone() {
                return true;
              }

              @Override
              public SolveResponse get() {
                return response;
              }

              @Override
              public SolveResponse get(long timeout, @NonNull TimeUnit unit) {
                return response;
              }
            });

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

  @Test
  @SuppressWarnings("unchecked")
  void testCancelTask() {
    // given
    var mockedResultQueue = mock(Queue.class);
    var mockedSolverStub = mock(SolverServiceGrpc.SolverServiceFutureStub.class);
    var mockedChanel = mock(ManagedChannel.class);

    when(mockedSolverStub.solve(any()))
        .thenReturn(
            new ListenableFuture<SolveResponse>() {
              @Override
              public void addListener(@NonNull Runnable listener, @NonNull Executor executor) {
                throw new UnsupportedOperationException();
              }

              @Override
              public boolean cancel(boolean mayInterruptIfRunning) {
                return true;
              }

              @Override
              public boolean isCancelled() {
                return true;
              }

              @Override
              public boolean isDone() {
                return true;
              }

              @Override
              public SolveResponse get() {
                throw new CancellationException();
              }

              @Override
              public SolveResponse get(long timeout, @NonNull TimeUnit unit) {
                throw new CancellationException();
              }
            });

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
    ArgumentCaptor<Message<ResultDTO>> messageCaptor = ArgumentCaptor.forClass(Message.class);
    verify(mockedResultQueue).publish(anyString(), messageCaptor.capture());
    var capturedMessage = messageCaptor.getValue();
    assertThat(capturedMessage.payload().getTaskStatus()).isEqualTo(TaskStatus.CANCELLED);
    verify(ack).ack();
  }
}
