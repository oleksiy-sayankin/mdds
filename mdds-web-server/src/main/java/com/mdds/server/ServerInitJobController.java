/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.dto.JobIdResponseDTO;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * Returns existing job identifier if a job exists for given pair [login, sessionId] or creates a
 * new job otherwise.
 */
@Slf4j
@RestController
public class ServerInitJobController {

  private final UserLookupService userLookupService;
  private final JobInitService jobInitService;

  public ServerInitJobController(
      UserLookupService userLookupService, JobInitService jobInitService) {
    this.userLookupService = userLookupService;
    this.jobInitService = jobInitService;
  }

  @PostMapping(path = "/jobs/init-job")
  public JobIdResponseDTO initJob(
      @RequestHeader(value = "X-MDDS-User-Login", required = true) String userLogin,
      @RequestHeader(value = "X-MDDS-Upload-Session-Id", required = true) String uploadSessionId) {
    var userId = userLookupService.findUserIdOrGuest(userLogin);
    var jobId = jobInitService.findOrCreateJobId(userId, uploadSessionId);
    try (var ignoredJobId = MDC.putCloseable("jobId", jobId);
        var ignoredUserId = MDC.putCloseable("userId", Long.toString(userId));
        var ignoredEvent = MDC.putCloseable("event", "init_job")) {
      log.info("Initialized job draft");
      return new JobIdResponseDTO(jobId);
    }
  }
}
