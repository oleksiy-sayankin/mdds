/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server.support;

import com.mdds.domain.JobStatus;
import com.mdds.server.UserLookupService;
import com.mdds.server.jpa.JobsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.transaction.annotation.Transactional;

@TestComponent
@RequiredArgsConstructor
public class JobTestFixture {
  private final JobsRepository jobsRepository;
  private final UserLookupService userLookupService;

  @Transactional
  public void forceStatus(String jobId, JobStatus status) {
    var job =
        jobsRepository
            .findById(jobId)
            .orElseThrow(() -> new IllegalStateException("Job not found in fixture: " + jobId));

    job.setStatus(status);
    jobsRepository.save(job);
  }

  public long countByUserIdAndUploadSessionId(String user, String session) {
    long userId = userLookupService.findUserId(user);
    return jobsRepository.countByUserIdAndUploadSessionId(userId, session);
  }
}
