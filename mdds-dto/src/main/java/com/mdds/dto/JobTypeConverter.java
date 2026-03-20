/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/** Converts string value to enumeration and vise versa. */
@Converter
public class JobTypeConverter implements AttributeConverter<JobType, String> {

  @Override
  public String convertToDatabaseColumn(JobType jobType) {
    return jobType == null ? null : jobType.value();
  }

  @Override
  public JobType convertToEntityAttribute(String dbData) {
    return (dbData == null) ? null : JobType.from(dbData);
  }
}
