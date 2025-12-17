/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestDataSourceDescriptor {

  @Test
  void testParseType() {
    var actualType = DataSourceDescriptor.Type.parse("s3");
    assertThat(actualType).isEqualTo(DataSourceDescriptor.Type.S3);
    actualType = DataSourceDescriptor.Type.parse("http_request");
    assertThat(actualType).isEqualTo(DataSourceDescriptor.Type.HTTP_REQUEST);
    actualType = DataSourceDescriptor.Type.parse("mysql");
    assertThat(actualType).isEqualTo(DataSourceDescriptor.Type.MYSQL);
  }

  @Test
  void testParseTypeException() {
    assertThatThrownBy(() -> DataSourceDescriptor.Type.parse("wrong_descriptor_type"))
        .isInstanceOf(DataSourceTypeException.class)
        .hasMessageContaining("Can not parse data source type");
  }

  @Test
  void testIsValid() {
    assertThat(DataSourceDescriptor.Type.isValid(null)).isFalse();
    assertThat(DataSourceDescriptor.Type.isValid("wrong_descriptor_type")).isFalse();
    assertThat(DataSourceDescriptor.Type.isValid("s3")).isTrue();
    assertThat(DataSourceDescriptor.Type.isValid("http_request")).isTrue();
    assertThat(DataSourceDescriptor.Type.isValid("mysql")).isTrue();
  }
}
