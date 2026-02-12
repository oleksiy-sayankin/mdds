/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.mdds.dto.CancelJobDTO;
import com.mdds.grpc.solver.CancelJobRequest;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Cancels job by job id. */
@Slf4j
@Component
public class CancelMessageHandler implements MessageHandler<CancelJobDTO> {
  private final SolverServiceGrpc.SolverServiceBlockingStub solverStub;

  @Autowired
  public CancelMessageHandler(GrpcChannel grpcChannel) {
    this.solverStub = SolverServiceGrpc.newBlockingStub(grpcChannel.getChannel());
  }

  @Override
  public void handle(@NonNull Message<CancelJobDTO> message, @NonNull Acknowledger ack) {
    var payload = message.payload();
    try {
      var response = solverStub.cancelJob(buildCancelRequest(payload));
      log.info(
          "Cancel request: jobId={}, status={}, message={}",
          payload.getJobId(),
          response.getRequestStatus(),
          response.getRequestStatusDetails());
    } catch (StatusRuntimeException e) {
      log.error("Cancel job failed", e);
    }
    ack.ack();
  }

  private static CancelJobRequest buildCancelRequest(CancelJobDTO cancelJobDTO) {
    log.info("Building cancel request for job {}", cancelJobDTO.getJobId());
    return CancelJobRequest.newBuilder().setJobId(cancelJobDTO.getJobId()).build();
  }
}
