/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server.jpa;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.persistence.entity.JobEntity;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** JPA repository to process JobEntity. */
public interface JobsRepository extends JpaRepository<JobEntity, String> {

  @VisibleForTesting
  long countByUserIdAndUploadSessionId(Long userId, String uploadSessionId);

  Optional<JobEntity> findByUserIdAndUploadSessionId(Long userId, String uploadSessionId);

  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select je from JobEntity je where je.id = :id and je.userId = :userId")
  Optional<JobEntity> lockByIdAndUserId(@Param("id") String id, @Param("userId") Long userId);

  @Query("select je from JobEntity je where je.id = :id and je.userId = :userId")
  Optional<JobEntity> findByIdAndUserId(@Param("id") String id, @Param("userId") Long userId);
}
