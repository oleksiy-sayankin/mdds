/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.grpc.solver.JobStatus.IN_PROGRESS;
import static com.mdds.grpc.solver.JobStatus.NEW;

import com.mdds.dto.CancelJobDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.queue.Message;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import java.time.Instant;
import java.util.HashMap;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Cancels job by job id. */
@Slf4j
@RestController
@RequestMapping("/cancel")
public class ServerCancelController {
  private final DataStorage storage;
  private final Queue cancelQueue;
  private static final Set<JobStatus> ALLOWED = Set.of(IN_PROGRESS, NEW);

  @Autowired
  public ServerCancelController(DataStorage storage, @Qualifier("cancelQueue") Queue cancelQueue) {
    this.storage = storage;
    this.cancelQueue = cancelQueue;
  }

  @PostMapping("/{jobId}")
  public ResponseEntity<Void> cancel(@PathVariable("jobId") String jobId) {
    log.info("Processing request in cancel controller for {}...", jobId);
    var opt = storage.get(jobId, ResultDTO.class);
    if (opt.isEmpty()) {
      throw new CanNotCancelJobException(
          HttpStatus.NOT_FOUND, "Can not cancel job " + jobId + ". No cancel queue name known");
    }
    var result = opt.get();
    if (!ALLOWED.contains(result.getJobStatus())) {
      throw new CanNotCancelJobException(
          HttpStatus.CONFLICT, "Job " + jobId + " is already " + result.getJobStatus());
    }

    var cancelQueueName = result.getCancelQueueName();
    if (cancelQueueName == null || cancelQueueName.isBlank()) {
      throw new CanNotCancelJobException(
          HttpStatus.INTERNAL_SERVER_ERROR,
          "Can not cancel job " + jobId + ". Cancel queue name is empty");
    }

    cancelQueue.publish(
        cancelQueueName, new Message<>(new CancelJobDTO(jobId), new HashMap<>(), Instant.now()));
    return ResponseEntity.accepted().build();
  }
}
