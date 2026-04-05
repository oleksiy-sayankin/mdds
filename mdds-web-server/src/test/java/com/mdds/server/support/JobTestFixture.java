/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.mdds.domain.JobStatus;
import com.mdds.persistence.entity.JobParamEntity;
import com.mdds.server.UserLookupService;
import com.mdds.server.jpa.JobParamsRepository;
import com.mdds.server.jpa.JobsRepository;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.transaction.annotation.Transactional;

@TestComponent
@RequiredArgsConstructor
public class JobTestFixture {
  private final JobsRepository jobsRepository;
  private final UserLookupService userLookupService;
  private final JobParamsRepository jobParamsRepository;

  @Transactional
  public void forceStatus(String jobId, JobStatus status) {
    var job =
        jobsRepository
            .findById(jobId)
            .orElseThrow(() -> new IllegalStateException("Job not found in fixture: " + jobId));

    job.setStatus(status);
    jobsRepository.save(job);
  }

  public Map<String, JsonNode> jobParams(String jobId) {
    var result = new HashMap<String, JsonNode>();

    var jobParamsEntities = jobParamsRepository.findAllByIdJobId(jobId);
    for (JobParamEntity jobParamEntity : jobParamsEntities) {
      result.put(jobParamEntity.getId().getParamName(), jobParamEntity.getParamValue());
    }
    return result;
  }

  public long countByUserIdAndUploadSessionId(String user, String session) {
    long userId = userLookupService.findUserId(user);
    return jobsRepository.countByUserIdAndUploadSessionId(userId, session);
  }
}
