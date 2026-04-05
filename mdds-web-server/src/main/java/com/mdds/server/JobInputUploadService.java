/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.JobProfiles;
import com.mdds.domain.JobStatus;
import com.mdds.persistence.entity.JobEntity;
import com.mdds.server.jpa.JobsRepository;
import java.net.URL;
import java.time.Instant;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** Creates pre-signed url for a job for certain inputs. */
@Service
@RequiredArgsConstructor
public class JobInputUploadService {

  private final JobsRepository jobsRepository;
  private final ObjectStoragePresignService objectStoragePresignService;

  public IssueUploadUrlResult issueUploadUrl(
      long requestedUserId, String requestedJobId, String inputSlot) {
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
    var profile = JobProfiles.forType(existingJobType);

    if (!profile.supportsInputUploadUrl()) {
      throw new InputUploadUrlNotSupportedForJobTypeException(
          String.format(
              "Input upload URL requests are not supported for the given jobType: '%s'.",
              existingJobType.value()));
    }

    var normalizedInputSlot = normalize(inputSlot);
    if (isNullOrBlank(normalizedInputSlot)) {
      throw new InputSlotIsNullOrBlankException("Input slot is null or blank.");
    }

    if (!isInDraftState(existingJob)) {
      throw new JobIsNotDraftException(
          String.format(
              "Job '%s' is not in DRAFT state and no more input artifacts can be uploaded.",
              existingJobId));
    }
    var artifact = profile.inputArtifacts().get(normalizedInputSlot);
    if (artifact == null) {
      throw new UnknownOrUnsupportedInputSlotException(
          String.format(
              "Unknown or unsupported input slot '%s' for the given jobType '%s'.",
              normalizedInputSlot, existingJobType.value()));
    }
    var fileName = artifact.fileName();

    var presigned =
        objectStoragePresignService.issueUploadUrl(existingUserId, existingJobId, fileName);
    return new IssueUploadUrlResult(presigned.uploadUrl(), presigned.expiresAt());
  }

  public record IssueUploadUrlResult(URL uploadUrl, Instant expiresAt) {}

  private static String normalize(String value) {
    return value == null ? null : value.trim().toLowerCase(Locale.ROOT);
  }

  private static boolean isInDraftState(JobEntity jobEntity) {
    return JobStatus.DRAFT.equals(jobEntity.getStatus());
  }

  private static boolean isNullOrBlank(String inputSlot) {
    return inputSlot == null || inputSlot.isBlank();
  }
}
