/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.data.source.DataSourceProviderFactory.fromDescriptor;
import static com.mdds.server.ServerHelper.extractDescriptor;
import static com.mdds.server.ServerHelper.extractSolvingMethod;
import static com.mdds.server.ServerHelper.unwrapOrSendError;

import com.mdds.common.CommonProperties;
import com.mdds.dto.JobDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SolveRequestDTO;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.queue.Message;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Controller for solving system of linear algebraic equations. */
@Slf4j
@RestController
public class ServerSolveController {
  private final DataStorage storage;
  private final Queue queue;
  private final CommonProperties commonProperties;

  @Autowired
  public ServerSolveController(
      DataStorage storage, @Qualifier("jobQueue") Queue queue, CommonProperties commonProperties) {
    this.storage = storage;
    this.queue = queue;
    this.commonProperties = commonProperties;
    log.info(
        "Created Server Solve controller with storage {}, queue '{}' = {}",
        storage,
        commonProperties.getJobQueueName(),
        queue);
  }

  @PostMapping(
      path = "/solve",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public JobIdResponseDTO solve(@RequestBody SolveRequestDTO request) {
    var rawType = request.getDataSourceType();
    var rawMethod = request.getSlaeSolvingMethod();
    var params = request.getParams();
    log.info(
        "Processing request in Solve Controller, dataSourceType = {},  slaeSolvingMethod = {},"
            + " params = {}",
        rawType,
        rawMethod,
        params);

    var descriptor = unwrapOrSendError(extractDescriptor(rawType, params));
    var provider = unwrapOrSendError(fromDescriptor(descriptor));
    var matrix = unwrapOrSendError(provider.loadMatrix());
    var rhs = unwrapOrSendError(provider.loadRhs());
    var method = unwrapOrSendError(extractSolvingMethod(rawMethod));

    // store initial result
    var jobId = UUID.randomUUID().toString();
    var now = Instant.now();
    // Put result to storage
    storage.put(jobId, new ResultDTO(jobId, now, null, JobStatus.NEW, null, 10, null, null));

    // create JobDTO and publish to queue
    queue.publish(
        commonProperties.getJobQueueName(),
        new Message<>(new JobDTO(jobId, now, matrix, rhs, method), Collections.emptyMap(), now));
    log.info(
        "Published job with jobId = {}, to queue '{}' = {}",
        jobId,
        commonProperties.getJobQueueName(),
        queue);
    return new JobIdResponseDTO(jobId);
  }
}
