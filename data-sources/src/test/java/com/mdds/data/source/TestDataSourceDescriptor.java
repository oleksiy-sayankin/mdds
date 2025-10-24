/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDataSourceDescriptor {

  @Test
  void testParseType() {
    var actualType = DataSourceDescriptor.Type.parse("s3");
    Assertions.assertEquals(DataSourceDescriptor.Type.S3, actualType);
    actualType = DataSourceDescriptor.Type.parse("http_request");
    Assertions.assertEquals(DataSourceDescriptor.Type.HTTP_REQUEST, actualType);
    actualType = DataSourceDescriptor.Type.parse("mysql");
    Assertions.assertEquals(DataSourceDescriptor.Type.MYSQL, actualType);
  }

  @Test
  void testParseTypeException() {
    assertThrows(
        DataSourceTypeException.class,
        () -> DataSourceDescriptor.Type.parse("wrong_descriptor_type"));
  }

  @Test
  void testIsValid() {
    Assertions.assertFalse(DataSourceDescriptor.Type.isValid(null));
    Assertions.assertFalse(DataSourceDescriptor.Type.isValid("wrong_descriptor_type"));
    Assertions.assertTrue(DataSourceDescriptor.Type.isValid("s3"));
    Assertions.assertTrue(DataSourceDescriptor.Type.isValid("http_request"));
    Assertions.assertTrue(DataSourceDescriptor.Type.isValid("mysql"));
  }
}
