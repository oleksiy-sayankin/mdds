/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.JobStatus;
import com.mdds.persistence.entity.JobEntity;
import com.mdds.server.jpa.JobsRepository;
import java.net.URL;
import java.time.Instant;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobOutputsService {

  private final JobsRepository jobsRepository;
  private final ObjectStorageService objectStorageService;
  private final JobProfileRegistry jobProfileRegistry;

  @Transactional(readOnly = true)
  public IssueDownloadUrlResult issueDownloadUrl(
      long requestedUserId, String requestedJobId, String outputSlot) {

    var existingJob =
        jobsRepository
            .findByIdAndUserId(requestedJobId, requestedUserId)
            .orElseThrow(
                () ->
                    new JobDoesNotExistException(
                        String.format("Job with id '%s' does not exist.", requestedJobId)));

    var existingUserId = existingJob.getUserId();
    var existingJobId = existingJob.getId();
    var existingJobType = existingJob.getJobType();

    var normalizedOutputSlot = normalize(outputSlot);
    if (isNullOrBlank(normalizedOutputSlot)) {
      throw new OutputSlotIsNullOrBlankException("Output slot is null or blank.");
    }

    if (!isInDoneState(existingJob)) {
      throw new JobIsNotDoneException(
          String.format(
              "Job '%s' is not in DONE state and no output artifacts can be downloaded.",
              existingJobId));
    }

    var profile = jobProfileRegistry.forType(existingJobType);
    var artifact = profile.outputArtifacts().get(normalizedOutputSlot);
    if (artifact == null) {
      throw new UnknownOrUnsupportedOutputSlotException(
          String.format(
              "Unknown or unsupported output slot '%s' for the given jobType '%s'.",
              normalizedOutputSlot, existingJobType));
    }

    var fileName = artifact.fileName();

    var outputObjectKey =
        ObjectKeyBuilder.canonicalOutputObjectKey(existingUserId, existingJobId, fileName);
    if (!objectStorageService.exists(outputObjectKey)) {
      log.error(
          "Job '{}' is DONE but output artifact '{}' does not exist in object storage.",
          existingJobId,
          outputObjectKey);
      throw new OutputArtifactDoesNotExistException("Output artifact does not exist.");
    }

    var result = objectStorageService.issueDownloadUrl(existingUserId, existingJobId, fileName);
    return new IssueDownloadUrlResult(result.downloadUrl(), result.expiresAt());
  }

  public record IssueDownloadUrlResult(URL downloadUrl, Instant expiresAt) {}

  private static String normalize(String value) {
    return value == null ? null : value.trim().toLowerCase(Locale.ROOT);
  }

  private static boolean isNullOrBlank(String outputSlot) {
    return outputSlot == null || outputSlot.isBlank();
  }

  private static boolean isInDoneState(JobEntity jobEntity) {
    return JobStatus.DONE.equals(jobEntity.getStatus());
  }
}
