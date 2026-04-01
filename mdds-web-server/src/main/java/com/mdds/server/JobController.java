/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.JobType;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.JobUploadUrlRequestDTO;
import com.mdds.dto.JobUploadUrlResponseDTO;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * Returns existing job identifier if a job exists for given pair [login, sessionId] or creates a
 * new job otherwise.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class JobController {

  private final UserLookupService userLookupService;
  private final JobCreationService jobCreationService;
  private final JobInputUploadService jobInputUploadService;

  @PostMapping(
      path = "/jobs",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<JobIdResponseDTO> createOrReuseDraftJob(
      @RequestHeader(value = "X-MDDS-User-Login", required = true) String userLogin,
      @RequestHeader(value = "X-MDDS-Upload-Session-Id", required = true) String uploadSessionId,
      @Valid @RequestBody CreateJobRequestDTO createJobRequestDTO) {
    var jobType = JobType.from(createJobRequestDTO.jobType());
    var userId = userLookupService.findUserId(userLogin);
    var result = jobCreationService.createOrReuseDraftJob(userId, uploadSessionId, jobType);
    var created = result.created();
    var jobId = result.jobId();
    try (var ignoredJobId = MDC.putCloseable("jobId", jobId);
        var ignoredUserId = MDC.putCloseable("userId", Long.toString(userId));
        var ignoredEvent = MDC.putCloseable("event", created ? "create_job" : "reuse_job")) {
      log.info(created ? "Created draft job" : "Reused existing draft job");
      return created
          ? ResponseEntity.status(HttpStatus.CREATED).body(new JobIdResponseDTO(jobId))
          : ResponseEntity.ok(new JobIdResponseDTO(jobId));
    }
  }

  @PostMapping(
      path = "/jobs/{jobId}/inputs",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public JobUploadUrlResponseDTO issueUploadUrl(
      @PathVariable("jobId") String jobId,
      @RequestHeader(value = "X-MDDS-User-Login", required = true) String userLogin,
      @Valid @RequestBody JobUploadUrlRequestDTO jobUploadUrlRequestDTO) {

    var userId = userLookupService.findUserId(userLogin);
    var inputSlot = jobUploadUrlRequestDTO.inputSlot();
    var uploadUrl = jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot);
    try (var ignoredJobId = MDC.putCloseable("jobId", jobId);
        var ignoredUserId = MDC.putCloseable("userId", Long.toString(userId));
        var ignoredEvent = MDC.putCloseable("event", "issue_upload_url")) {
      log.info("Issued pre-signed upload URL.");
      return new JobUploadUrlResponseDTO(
          jobId, uploadUrl.uploadUrl().toString(), uploadUrl.expiresAt());
    }
  }
}
