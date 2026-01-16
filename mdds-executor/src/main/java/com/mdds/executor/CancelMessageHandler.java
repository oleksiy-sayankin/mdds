/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.mdds.dto.CancelTaskDTO;
import com.mdds.grpc.solver.CancelTaskRequest;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Cancels task by task id. */
@Slf4j
@Component
public class CancelMessageHandler implements MessageHandler<CancelTaskDTO> {
  private final SolverServiceGrpc.SolverServiceBlockingStub solverStub;

  @Autowired
  public CancelMessageHandler(GrpcChannel grpcChannel) {
    this.solverStub = SolverServiceGrpc.newBlockingStub(grpcChannel.getChannel());
  }

  @Override
  public void handle(@NonNull Message<CancelTaskDTO> message, @NonNull Acknowledger ack) {
    var payload = message.payload();
    try {
      var response = solverStub.cancelTask(buildCancelRequest(payload));
      log.info(
          "Cancel request: taskId={}, status={}, message={}",
          payload.getTaskId(),
          response.getRequestStatus(),
          response.getRequestStatusDetails());
    } catch (StatusRuntimeException e) {
      log.error("Cancel task failed", e);
    }
    ack.ack();
  }

  private static CancelTaskRequest buildCancelRequest(CancelTaskDTO cancelTaskDTO) {
    log.info("Building cancel request for task {}", cancelTaskDTO.getTaskId());
    return CancelTaskRequest.newBuilder().setTaskId(cancelTaskDTO.getTaskId()).build();
  }
}
