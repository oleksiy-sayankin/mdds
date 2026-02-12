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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mdds.common.CommonProperties;
import com.mdds.dto.CancelJobDTO;
import com.mdds.dto.JobDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.grpc.solver.GetJobStatusResponse;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.grpc.solver.RequestStatus;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.grpc.solver.SubmitJobResponse;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
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
  @Qualifier("jobQueue")
  private Queue jobQueue;

  @Autowired
  @Qualifier("resultQueue")
  private Queue resultQueue;

  @Autowired
  @Qualifier("cancelQueue")
  private Queue cancelQueue;

  @Autowired private GrpcServerProperties grpcServerConfig;

  @Autowired private CommonProperties commonProperties;

  @Autowired private ExecutorProperties executorProperties;

  @MockitoBean private ExecutorMessageHandler executorMessageHandler;

  @MockitoBean private CancelMessageHandler cancelMessageHandler;

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
    // Prepare and put data to job queue
    var jobId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var job =
        new JobDTO(
            jobId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var endTime = Instant.now();
    var expected =
        new ResultDTO(
            jobId,
            startTime,
            endTime,
            JobStatus.DONE,
            executorProperties.getCancelQueueName(),
            100,
            new double[] {1.971, 3.213, 7.243},
            "");

    // Simulate that ExecutorMessageHandler solves the job
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
            jobQueue,
            resultQueue,
            cancelQueue,
            executorMessageHandler,
            cancelMessageHandler,
            jobQueue.subscribe(
                commonProperties.getJobQueueName(), JobDTO.class, executorMessageHandler),
            cancelQueue.subscribe(
                executorProperties.getCancelQueueName(), CancelJobDTO.class, cancelMessageHandler),
            commonProperties,
            executorProperties)) {
      var jobMessage = new Message<>(job, new HashMap<>(), Instant.now());
      jobQueue.publish(commonProperties.getJobQueueName(), jobMessage);

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
    var mockedSolverStub = mock(SolverServiceGrpc.SolverServiceBlockingStub.class);
    var mockedChanel = mock(GrpcChannel.class);

    when(mockedSolverStub.submitJob(any()))
        .thenReturn(
            SubmitJobResponse.newBuilder().setRequestStatus(RequestStatus.COMPLETED).build());

    when(mockedSolverStub.withDeadlineAfter(any())).thenReturn(mockedSolverStub);

    when(mockedSolverStub.getJobStatus(any()))
        .thenReturn(
            GetJobStatusResponse.newBuilder()
                .setJobStatus(JobStatus.DONE)
                .addSolution(1.371)
                .addSolution(3.283)
                .addSolution(3.243)
                .setRequestStatus(RequestStatus.COMPLETED)
                .build());

    var handler =
        new ExecutorMessageHandler(
            mockedResultQueue,
            mockedSolverStub,
            mockedChanel,
            commonProperties,
            executorProperties);

    // Prepare and put data to job queue
    var jobId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var job =
        new JobDTO(
            jobId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var ack = mock(Acknowledger.class);
    var jobMessage = new Message<>(job, new HashMap<>(), Instant.now());
    // when
    handler.handle(jobMessage, ack);

    // then
    verify(mockedResultQueue, times(2)).publish(anyString(), any());
    verify(ack).ack();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCancelJob() {
    // given
    var mockedResultQueue = mock(Queue.class);
    var mockedSolverStub = mock(SolverServiceGrpc.SolverServiceBlockingStub.class);
    var mockedChanel = mock(GrpcChannel.class);

    when(mockedSolverStub.submitJob(any()))
        .thenReturn(
            SubmitJobResponse.newBuilder().setRequestStatus(RequestStatus.COMPLETED).build());

    when(mockedSolverStub.withDeadlineAfter(any())).thenReturn(mockedSolverStub);

    when(mockedSolverStub.getJobStatus(any()))
        .thenReturn(
            GetJobStatusResponse.newBuilder()
                .setJobStatus(JobStatus.CANCELLED)
                .setRequestStatus(RequestStatus.COMPLETED)
                .build());

    var handler =
        new ExecutorMessageHandler(
            mockedResultQueue,
            mockedSolverStub,
            mockedChanel,
            commonProperties,
            executorProperties);

    // Prepare and put data to job queue
    var jobId = UUID.randomUUID().toString();
    var startTime = Instant.now();
    var job =
        new JobDTO(
            jobId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var ack = mock(Acknowledger.class);
    var jobMessage = new Message<>(job, new HashMap<>(), Instant.now());
    // when
    handler.handle(jobMessage, ack);

    // then
    ArgumentCaptor<Message<ResultDTO>> messageCaptor = ArgumentCaptor.forClass(Message.class);
    verify(mockedResultQueue, times(2)).publish(anyString(), messageCaptor.capture());
    var capturedMessage = messageCaptor.getValue();
    assertThat(capturedMessage.payload().getJobStatus()).isEqualTo(JobStatus.CANCELLED);
    verify(ack).ack();
  }
}
