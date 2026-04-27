/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.mdds.domain.JobStatus;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.JobSubmitResponseDTO;
import com.mdds.dto.JobUploadUrlRequestDTO;
import com.mdds.dto.JobUploadUrlResponseDTO;
import jakarta.validation.Valid;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
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
  private final JobParamsService jobParamsService;
  private final JobSubmissionService jobSubmissionService;

  private static final String JOB_ID = "jobId";
  private static final String USER_ID = "userId";
  private static final String EVENT = "event";

  private static final String APPLICATION_MERGE_PATCH_JSON_VALUE = "application/merge-patch+json";

  @PostMapping(
      path = "/jobs",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<JobIdResponseDTO> createOrReuseDraftJob(
      @RequestHeader(value = "X-MDDS-User-Login", required = true) String userLogin,
      @RequestHeader(value = "X-MDDS-Upload-Session-Id", required = true) String uploadSessionId,
      @Valid @RequestBody CreateJobRequestDTO createJobRequestDTO) {
    var jobType = createJobRequestDTO.jobType();
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

  @PatchMapping(path = "/jobs/{jobId}/params", consumes = APPLICATION_MERGE_PATCH_JSON_VALUE)
  public ResponseEntity<Void> mergeParams(
      @PathVariable("jobId") String jobId,
      @RequestHeader(value = "X-MDDS-User-Login", required = true) String userLogin,
      @RequestBody JsonNode patchNode) {
    var userId = userLookupService.findUserId(userLogin);
    var params = extractPatchParams(patchNode);
    try (var ignoredJobId = MDC.putCloseable(JOB_ID, jobId);
        var ignoredUserId = MDC.putCloseable(USER_ID, Long.toString(userId));
        var ignoredEvent = MDC.putCloseable(EVENT, "patch_job_params")) {
      jobParamsService.mergeParams(userId, jobId, params);
      log.info("Job parameters patch was applied.");
      return ResponseEntity.ok().build();
    }
  }

  @PostMapping(path = "/jobs/{jobId}/submit")
  public ResponseEntity<JobSubmitResponseDTO> submit(
      @PathVariable("jobId") String jobId,
      @RequestHeader(value = "X-MDDS-User-Login", required = true) String userLogin) {

    var userId = userLookupService.findUserId(userLogin);

    try (var ignoredJobId = MDC.putCloseable(JOB_ID, jobId);
        var ignoredUserId = MDC.putCloseable(USER_ID, Long.toString(userId));
        var ignoredEvent = MDC.putCloseable(EVENT, "submit_job")) {
      jobSubmissionService.submit(userId, jobId);
      log.info("Submitted job.");
      return ResponseEntity.accepted()
          .body(new JobSubmitResponseDTO(jobId, JobStatus.SUBMITTED.toString()));
    }
  }

  private static Map<String, JsonNode> extractPatchParams(JsonNode patchNode) {
    if (patchNode == null || !patchNode.isObject()) {
      throw new MergePatchDocumentMustBeJsonObjectException(
          "The merge patch document must be a JSON object.");
    }

    return patchNode
        .propertyStream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
