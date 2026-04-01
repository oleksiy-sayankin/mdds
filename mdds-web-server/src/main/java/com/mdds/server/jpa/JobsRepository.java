/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server.jpa;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.persistence.entity.JobEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository to process JobEntity. */
public interface JobsRepository extends JpaRepository<JobEntity, String> {

  @VisibleForTesting
  long countByUserIdAndUploadSessionId(Long userId, String uploadSessionId);

  Optional<JobEntity> findByUserIdAndUploadSessionId(Long userId, String uploadSessionId);
}
