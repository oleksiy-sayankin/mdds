/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.persistence.converter;

import com.mdds.domain.JobStatus;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class JobStatusConverter implements AttributeConverter<JobStatus, String> {
  @Override
  public String convertToDatabaseColumn(JobStatus attribute) {
    return attribute.getCode();
  }

  @Override
  public JobStatus convertToEntityAttribute(String dbData) {
    return JobStatus.from(dbData);
  }
}
