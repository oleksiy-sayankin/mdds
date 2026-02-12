/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.CommonProperties;
import com.mdds.dto.JobDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.GetJobStatusRequest;
import com.mdds.grpc.solver.GetJobStatusResponse;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.grpc.solver.RequestStatus;
import com.mdds.grpc.solver.Row;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.grpc.solver.SubmitJobRequest;
import com.mdds.grpc.solver.SubmitJobResponse;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/** Solves system of linear algebraic equations. */
@Slf4j
@Component
public class ExecutorMessageHandler implements MessageHandler<JobDTO> {
  private final SolverServiceGrpc.SolverServiceBlockingStub solverStub;
  private final Queue resultQueue;
  private final GrpcChannel grpcChannel;
  private final CommonProperties commonProperties;
  private final ExecutorProperties executorProperties;
  private static final Duration JOB_TIMEOUT = Duration.ofSeconds(600);
  private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
  private static final Duration POLL_DELAY = Duration.ZERO;
  private static final Duration GRPC_REQUEST_TIMEOUT = Duration.ofSeconds(5);
  private static final Set<JobStatus> VALID_STATUSES =
      Set.of(JobStatus.DONE, JobStatus.CANCELLED, JobStatus.ERROR);

  @Autowired
  public ExecutorMessageHandler(
      @Qualifier("resultQueue") Queue resultQueue,
      GrpcChannel grpcChannel,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.grpcChannel = grpcChannel;
    this.solverStub = SolverServiceGrpc.newBlockingStub(grpcChannel.getChannel());
    this.resultQueue = resultQueue;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
    log.info(
        "Created Executor Message Handler '{}', {}, gRPC Server {}:{}",
        commonProperties.getResultQueueName(),
        resultQueue,
        grpcChannel.getHost(),
        grpcChannel.getPort());
  }

  @VisibleForTesting
  public ExecutorMessageHandler(
      Queue resultQueue,
      SolverServiceGrpc.SolverServiceBlockingStub solverStub,
      GrpcChannel channel,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.grpcChannel = channel;
    this.resultQueue = resultQueue;
    this.solverStub = solverStub;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
  }

  @Override
  public void handle(@Nonnull Message<JobDTO> message, @Nonnull Acknowledger ack) {
    var job = message.payload();
    try {
      log.info("Start handling job {}", job.getId());
      var submitResponse = submit(job);
      if (!completed(submitResponse)) {
        publishErrorFor(job, "Error submitting job: " + submitResponse.getRequestStatusDetails());
        return;
      }
      publishInProgressFor(job);
      var result = awaitForResultFrom(job);
      publishResponse(result);
    } catch (ConditionTimeoutException e) {
      publishErrorFor(job, "Timeout waiting for job status");
    } catch (StatusRuntimeException e) {
      publishErrorFor(job, "Internal gRPC error: " + e.getStatus());
    } catch (Exception e) {
      publishErrorFor(
          job, "Unexpected error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
    } finally {
      ack.ack();
    }
  }

  private SubmitJobResponse submit(JobDTO payload) {
    var submitJobRequest = buildSubmitJobRequest(payload);
    return solverStub.withDeadlineAfter(GRPC_REQUEST_TIMEOUT).submitJob(submitJobRequest);
  }

  private void publishInProgressFor(JobDTO payload) {
    // create message with 'in progress' status
    var jobId = payload.getId();
    var jobCreationDateTime = payload.getDateTime();
    var inProgress =
        new ResultDTO(
            jobId,
            jobCreationDateTime,
            Instant.now(),
            JobStatus.IN_PROGRESS,
            executorProperties.getCancelQueueName(),
            30,
            new double[] {},
            "");
    log.info("Publishing in-progress status for job {}", jobId);
    publish(new Message<>(inProgress, Map.of(), Instant.now()));
  }

  private void publishErrorFor(JobDTO payload, String message) {
    // create error message
    var errorResult =
        new ResultDTO(
            payload.getId(),
            payload.getDateTime(),
            Instant.now(),
            JobStatus.ERROR,
            executorProperties.getCancelQueueName(),
            70,
            new double[] {},
            message);
    log.error("Error for job {} with message '{}'", payload.getId(), message);
    publish(new Message<>(errorResult, Map.of(), Instant.now()));
  }

  private static Instant toInstant(com.google.protobuf.Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  private void publishResponse(GetJobStatusResponse response) {
    // create message with solution and publish to result queue
    var solution = response.getSolutionList().stream().mapToDouble(Double::doubleValue).toArray();
    var resultArray =
        new ResultDTO(
            response.getJobId(),
            toInstant(response.getStartTime()),
            toInstant(response.getEndTime()),
            response.getJobStatus(),
            executorProperties.getCancelQueueName(),
            response.getProgress(),
            solution,
            response.getJobMessage());
    log.info(
        "Published response for job {} with message '{}'",
        response.getJobId(),
        response.getJobMessage());
    publish(new Message<>(resultArray, Map.of(), Instant.now()));
  }

  private GetJobStatusResponse awaitForResultFrom(JobDTO job) {
    var request = buildGetJobStatusRequest(job.getId());
    var attempts = new AtomicInteger(0);
    return Awaitility.await()
        .atMost(JOB_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .pollDelay(POLL_DELAY)
        .ignoreExceptionsMatching(
            // Here we ignore "temporary" problems related to gRPC connection.
            // We expect they can be fixed during next iteration of polling.
            throwable ->
                throwable instanceof StatusRuntimeException sre
                    && switch (sre.getStatus().getCode()) {
                      case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED -> true;
                      default -> false;
                    })
        .until(
            () -> {
              var response =
                  solverStub.withDeadlineAfter(GRPC_REQUEST_TIMEOUT).getJobStatus(request);
              log.debug(
                  "Job status attempt {}: id {}, requestStatus {}, jobStatus {}, msg '{}'",
                  attempts.incrementAndGet(),
                  job.getId(),
                  response.getRequestStatus(),
                  response.getJobStatus(),
                  response.getJobMessage());
              return response;
            },
            response -> completed(response) && jobIsInTerminalState(response));
  }

  private static boolean jobIsInTerminalState(GetJobStatusResponse response) {
    return VALID_STATUSES.contains(response.getJobStatus());
  }

  private static boolean completed(SubmitJobResponse response) {
    return RequestStatus.COMPLETED.equals(response.getRequestStatus());
  }

  private static boolean completed(GetJobStatusResponse response) {
    return RequestStatus.COMPLETED.equals(response.getRequestStatus());
  }

  private void publish(Message<ResultDTO> resultMessage) {
    resultQueue.publish(commonProperties.getResultQueueName(), resultMessage);
    log.info(
        "Published result for job with id {} and status {} to queue '{}', {}",
        resultMessage.payload().getJobId(),
        resultMessage.payload().getJobStatus(),
        commonProperties.getResultQueueName(),
        resultQueue);
  }

  private static GetJobStatusRequest buildGetJobStatusRequest(String jobId) {
    log.info("Building get job status request for job {}", jobId);
    var requestBuilder = GetJobStatusRequest.newBuilder().setJobId(jobId);
    return requestBuilder.build();
  }

  private static SubmitJobRequest buildSubmitJobRequest(JobDTO payload) {
    log.info("Building submit solve job request for job {}", payload.getId());
    // set solving method for gRPC request
    var requestBuilder =
        SubmitJobRequest.newBuilder().setMethod(payload.getSlaeSolvingMethod().getName());

    // set matrix for gRPC request
    for (var row : payload.getMatrix()) {
      var rowBuilder = Row.newBuilder();
      for (var value : row) {
        rowBuilder.addValues(value);
      }
      requestBuilder.addMatrix(rowBuilder.build());
    }

    // set right hand side for gRPC request
    for (var value : payload.getRhs()) {
      requestBuilder.addRhs(value);
    }

    // set job id
    requestBuilder.setJobId(payload.getId());
    return requestBuilder.build();
  }

  @Override
  public String toString() {
    return "ExecutorMessageHandler[gRPC="
        + grpcChannel.getHost()
        + ":"
        + grpcChannel.getPort()
        + ", '"
        + commonProperties.getResultQueueName()
        + "',"
        + resultQueue
        + "]";
  }
}
