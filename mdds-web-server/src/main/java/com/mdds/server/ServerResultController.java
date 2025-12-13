/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.dto.ResultDTO;
import com.mdds.storage.DataStorage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Get result as document from key-value data storage. */
@Slf4j
@RestController
@RequestMapping("/result")
public class ServerResultController {
  private final DataStorage storage;

  @Autowired
  public ServerResultController(DataStorage storage) {
    this.storage = storage;
  }

  @GetMapping("/{taskId}")
  public ResultDTO result(@PathVariable("taskId") String taskId) {
    log.info("Processing request in result controller for {}...", taskId);
    var result = storage.get(taskId, ResultDTO.class);
    result.ifPresentOrElse(
        value -> log.info("Found result for task {} with status {}", taskId, value.getTaskStatus()),
        () -> log.info("No result found for {}", taskId));
    if (result.isPresent()) {
      return result.get();
    } else {
      throw new NoResultFoundException("No result found for " + taskId);
    }
  }
}
