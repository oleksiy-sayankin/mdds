/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.mdds.common.AppConstants;
import com.mdds.common.AppConstantsFactory;
import com.mdds.grpc.solver.Row;
import com.mdds.grpc.solver.SolveRequest;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import dto.ResultDTO;
import dto.TaskDTO;
import dto.TaskStatus;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;

/** Solves system of linear algebraic equations. */
@Slf4j
public class ExecutorMessageHandler implements MessageHandler<TaskDTO> {
  private final SolverServiceGrpc.SolverServiceBlockingStub solverStub;
  private final @Nonnull Queue resultQueue;

  public ExecutorMessageHandler(@Nonnull Queue resultQueue) {
    var channel =
        ManagedChannelBuilder.forAddress(
                ExecutorConfFactory.fromEnvOrDefaultProperties().grpcHost(),
                ExecutorConfFactory.fromEnvOrDefaultProperties().grpcPort())
            .usePlaintext()
            .build();
    this.solverStub = SolverServiceGrpc.newBlockingStub(channel);
    this.resultQueue = resultQueue;
  }

  @Override
  public void handle(@Nonnull Message<TaskDTO> message, @Nonnull Acknowledger ack) {
    var payload = message.payload();

    // set solving method for gRPC request
    var requestBuilder =
        SolveRequest.newBuilder().setMethod(payload.getSlaeSolvingMethod().getName());

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

    // RPC call
    var response = solverStub.solve(requestBuilder.build());

    // extract solution from response
    var solution = response.getSolutionList().stream().mapToDouble(Double::doubleValue).toArray();

    // create message with solution and publish to result queue
    var resultArray =
        new ResultDTO(
            payload.getId(), payload.getDateTime(), Instant.now(), TaskStatus.DONE, solution, "");
    var resultMessage = new Message<>(resultArray, new HashMap<>(), Instant.now());

    resultQueue.publish(
        AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), resultMessage);

    // inform queue that message was processed
    ack.ack();
    log.debug("Solved system of linear algebraic equations task {}", payload.getId());
  }
}
