/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.persistence.entity;

import com.mdds.domain.JobStatus;
import com.mdds.domain.JobType;
import com.mdds.persistence.converter.JobTypeConverter;
import jakarta.annotation.Nonnull;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Represents persisted job metadata. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "jobs")
public class JobEntity {
  @Id private String id;

  @Column(name = "user_id", nullable = false)
  @Nonnull
  private Long userId;

  @Column(name = "upload_session_id", nullable = false)
  @Nonnull
  private String uploadSessionId;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  @Nonnull
  private JobStatus status;

  @Column(name = "job_type", nullable = false)
  @Nonnull
  @Convert(converter = JobTypeConverter.class)
  private JobType jobType;

  @Column(name = "progress", nullable = false)
  private int progress;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "submitted_at")
  private Instant submittedAt;

  @Column(name = "started_at")
  private Instant startedAt;

  @Column(name = "finished_at")
  private Instant finishedAt;
}
