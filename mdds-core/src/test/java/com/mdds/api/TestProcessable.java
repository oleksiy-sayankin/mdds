/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.api;

import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestProcessable {

  @Test
  void testOf() {
    var testClass = new TestClass();
    var result = Processable.of(testClass);
    Assertions.assertEquals(testClass, result.get());
  }

  @Test
  void testIfPresent() {
    var testClass = new TestClass();
    testClass.setId(100);
    var result = Processable.of(testClass);
    result.ifPresent(r -> Assertions.assertEquals(100, r.getId()));
  }

  @Test
  void testIfFailure() {
    var result = Processable.failure("Test failure", new Exception());
    result.ifFailure(r -> Assertions.assertEquals("Test failure", r.getErrorMessage()));
  }

  @Getter
  @Setter
  static class TestClass {
    private int id;
  }
}
