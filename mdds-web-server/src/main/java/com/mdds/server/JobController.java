/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.JobType;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.JobSetParamsRequestDTO;
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
import org.springframework.web.bind.annotation.PutMapping;
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
  private final JobParamsService jobParamsService;

  private static final String JOB_ID = "jobId";
  private static final String USER_ID = "userId";
  private static final String EVENT = "event";

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
    try (var ignoredJobId = MDC.putCloseable(JOB_ID, jobId);
        var ignoredUserId = MDC.putCloseable(USER_ID, Long.toString(userId));
        var ignoredEvent = MDC.putCloseable(EVENT, created ? "create_job" : "reuse_job")) {
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
    try (var ignoredJobId = MDC.putCloseable(JOB_ID, jobId);
        var ignoredUserId = MDC.putCloseable(USER_ID, Long.toString(userId));
        var ignoredEvent = MDC.putCloseable(EVENT, "issue_upload_url")) {
      log.info("Issued pre-signed upload URL.");
      return new JobUploadUrlResponseDTO(
          jobId, uploadUrl.uploadUrl().toString(), uploadUrl.expiresAt());
    }
  }

  @PutMapping(path = "/jobs/{jobId}/params", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Void> setParams(
      @PathVariable("jobId") String jobId,
      @RequestHeader(value = "X-MDDS-User-Login", required = true) String userLogin,
      @RequestBody JobSetParamsRequestDTO setParamsRequestDTO) {

    var userId = userLookupService.findUserId(userLogin);
    var params = setParamsRequestDTO.params();
    try (var ignoredJobId = MDC.putCloseable(JOB_ID, jobId);
        var ignoredUserId = MDC.putCloseable(USER_ID, Long.toString(userId));
        var ignoredEvent = MDC.putCloseable(EVENT, "set_job_params")) {
      jobParamsService.replaceParams(userId, jobId, params);
      log.info("Job parameters are set.");
      return ResponseEntity.ok().build();
    }
  }
}
