/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.api;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

class TestProcessable {

  @Test
  void testOf() {
    var testClass = new TestClass();
    var result = Processable.of(testClass);
    assertThat(testClass).isEqualTo(result.get());
  }

  @Test
  void testIfPresent() {
    var testClass = new TestClass();
    testClass.setId(100);
    var result = Processable.of(testClass);
    result.ifPresent(r -> assertThat(r.getId()).isEqualTo(100));
  }

  @Test
  void testIfFailure() {
    var result = Processable.failure("Test failure", new Exception());
    result.ifFailure(r -> assertThat(r.getErrorMessage()).isEqualTo("Test failure"));
  }

  @Getter
  @Setter
  static class TestClass {
    private int id;
  }
}
