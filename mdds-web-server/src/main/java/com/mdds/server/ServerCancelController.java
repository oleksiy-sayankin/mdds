/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.dto.TaskStatus.IN_PROGRESS;
import static com.mdds.dto.TaskStatus.NEW;

import com.mdds.dto.CancelTaskDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
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

/** Cancels task by task id. */
@Slf4j
@RestController
@RequestMapping("/cancel")
public class ServerCancelController {
  private final DataStorage storage;
  private final Queue cancelQueue;
  private static final Set<TaskStatus> ALLOWED = Set.of(IN_PROGRESS, NEW);

  @Autowired
  public ServerCancelController(DataStorage storage, @Qualifier("cancelQueue") Queue cancelQueue) {
    this.storage = storage;
    this.cancelQueue = cancelQueue;
  }

  @PostMapping("/{taskId}")
  public ResponseEntity<Void> cancel(@PathVariable("taskId") String taskId) {
    log.info("Processing request in cancel controller for {}...", taskId);
    var opt = storage.get(taskId, ResultDTO.class);
    if (opt.isEmpty()) {
      throw new CanNotCancelTaskException(
          HttpStatus.NOT_FOUND, "Can not cancel task " + taskId + ". No cancel queue name known");
    }
    var result = opt.get();
    if (!ALLOWED.contains(result.getTaskStatus())) {
      throw new CanNotCancelTaskException(
          HttpStatus.CONFLICT, "Task " + taskId + " is already " + result.getTaskStatus());
    }

    var cancelQueueName = result.getCancelQueueName();
    if (cancelQueueName == null || cancelQueueName.isBlank()) {
      throw new CanNotCancelTaskException(
          HttpStatus.INTERNAL_SERVER_ERROR,
          "Can not cancel task " + taskId + ". Cancel queue name is empty");
    }

    cancelQueue.publish(
        cancelQueueName, new Message<>(new CancelTaskDTO(taskId), new HashMap<>(), Instant.now()));
    return ResponseEntity.accepted().build();
  }
}
