/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server.jpa;

import com.mdds.persistence.entity.JobParamEntity;
import com.mdds.persistence.entity.JobParamId;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;

/** JPA repository to process JobParamEntity. */
public interface JobParamsRepository extends JpaRepository<JobParamEntity, JobParamId> {

  @Modifying
  void deleteByIdJobId(String jobId);

  List<JobParamEntity> findAllByIdJobId(String jobId);
}
