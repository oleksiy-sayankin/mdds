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
import com.mdds.domain.SlaeSolver;
import com.mdds.dto.JobDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.GetJobStatusResponse;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.grpc.solver.RequestStatus;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.grpc.solver.SubmitJobResponse;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.CancelBus;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.QueueClient;
import jakarta.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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

  private static final Instant BASE_EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(BASE_EVENT_TIME, ZoneOffset.UTC);

  @Autowired
  @Qualifier("jobQueueClient")
  private QueueClient jobQueueClient;

  @Autowired
  @Qualifier("resultQueueClient")
  private QueueClient resultQueueClient;

  @Autowired private CancelBus cancelBus;

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
    var startTime = BASE_EVENT_TIME;
    var job =
        new JobDTO(
            jobId,
            startTime,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var expected =
        new ResultDTO(
            jobId,
            startTime,
            BASE_EVENT_TIME,
            JobStatus.DONE,
            executorProperties.getId(),
            100,
            new double[] {1.971, 3.213, 7.243},
            "");

    // Simulate that ExecutorMessageHandler solves the job
    doAnswer(
            invocation -> {
              var resultMessage = new Message<>(expected, new HashMap<>(), BASE_EVENT_TIME);
              resultQueueClient.publish(commonProperties.getResultQueueName(), resultMessage);
              return null;
            })
        .when(executorMessageHandler)
        .handle(any(), any());

    try (var executorService =
        new ExecutorService(
            jobQueueClient,
            resultQueueClient,
            cancelBus,
            executorMessageHandler,
            cancelMessageHandler,
            jobQueueClient.subscribe(
                commonProperties.getJobQueueName(), JobDTO.class, executorMessageHandler),
            cancelBus.subscribe(executorProperties.getId(), cancelMessageHandler),
            commonProperties,
            executorProperties)) {
      var jobMessage = new Message<>(job, new HashMap<>(), BASE_EVENT_TIME);
      jobQueueClient.publish(commonProperties.getJobQueueName(), jobMessage);

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
                    resultQueueClient.subscribe(
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
    var mockedResultQueue = mock(QueueClient.class);
    var mockedSolverStub = mock(SolverServiceGrpc.SolverServiceBlockingStub.class);
    var mockedChanel = mock(GrpcChannel.class);

    when(mockedSolverStub.submitJob(any()))
        .thenReturn(
            SubmitJobResponse.newBuilder().setRequestStatus(RequestStatus.COMPLETED).build());

    when(mockedSolverStub.withDeadlineAfter(any())).thenReturn(mockedSolverStub);

    var jobId = UUID.randomUUID().toString();
    when(mockedSolverStub.getJobStatus(any()))
        .thenReturn(
            GetJobStatusResponse.newBuilder()
                .setJobId(jobId)
                .setJobStatus(JobStatus.DONE)
                .addSolution(1.371)
                .addSolution(3.283)
                .addSolution(3.243)
                .setRequestStatus(RequestStatus.COMPLETED)
                .setJobMessage("Solved system of liner algebraic equation")
                .build());

    var handler =
        new ExecutorMessageHandler(
            mockedResultQueue,
            mockedSolverStub,
            mockedChanel,
            commonProperties,
            executorProperties,
            FIXED_CLOCK);

    // Prepare and put data to job queue
    var job =
        new JobDTO(
            jobId,
            BASE_EVENT_TIME,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var ack = mock(Acknowledger.class);
    var jobMessage = new Message<>(job, new HashMap<>(), BASE_EVENT_TIME);
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
    var mockedResultQueue = mock(QueueClient.class);
    var mockedSolverStub = mock(SolverServiceGrpc.SolverServiceBlockingStub.class);
    var mockedChanel = mock(GrpcChannel.class);
    var jobId = UUID.randomUUID().toString();

    when(mockedSolverStub.submitJob(any()))
        .thenReturn(
            SubmitJobResponse.newBuilder().setRequestStatus(RequestStatus.COMPLETED).build());

    when(mockedSolverStub.withDeadlineAfter(any())).thenReturn(mockedSolverStub);

    when(mockedSolverStub.getJobStatus(any()))
        .thenReturn(
            GetJobStatusResponse.newBuilder()
                .setJobId(jobId)
                .setJobStatus(JobStatus.CANCELLED)
                .setRequestStatus(RequestStatus.COMPLETED)
                .setJobMessage("Cancelled by user request")
                .build());

    var handler =
        new ExecutorMessageHandler(
            mockedResultQueue,
            mockedSolverStub,
            mockedChanel,
            commonProperties,
            executorProperties,
            FIXED_CLOCK);

    // Prepare and put data to job queue
    var job =
        new JobDTO(
            jobId,
            BASE_EVENT_TIME,
            new double[][] {
              {1.1, 2.2, 3.3, 4.4}, {51, 24.2, 33.3, 34.24}, {31.1, 232.2, 43.3, 4.4}
            },
            new double[] {4.3, 3.23, 5.324},
            SlaeSolver.NUMPY_EXACT_SOLVER);
    var ack = mock(Acknowledger.class);
    var jobMessage = new Message<>(job, new HashMap<>(), BASE_EVENT_TIME);
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
