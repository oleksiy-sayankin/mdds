/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server.jpa;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.dto.Jobs;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository to process Jobs. */
public interface JobsRepository extends JpaRepository<Jobs, String> {

  @VisibleForTesting
  long countByUserIdAndUploadSessionId(Long userId, String uploadSessionId);

  Optional<Jobs> findByUserIdAndUploadSessionId(Long userId, String uploadSessionId);
}
