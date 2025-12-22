/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
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

  @Test
  void testNoValuePresent() {
    var result = Processable.failure("Test failure");
    assertThatThrownBy(result::get)
        .isInstanceOf(ProcessableStateException.class)
        .hasMessageContaining("No value present. Error:");
  }

  @Test
  void testToOptional() {
    var testClass = new TestClass();
    var result = Processable.of(testClass);
    var optional = result.toOptional();
    assertThat(optional).isPresent();
  }

  @Test
  void testMap() {
    Map<String, User> map =
        Map.of("1", new User("John"), "2", new User("Mike"), "3", new User("Neo"));

    var result =
        Processable.of(Objects.requireNonNull(findById(map, "1")))
            .map(User::getUsername)
            .map(String::toUpperCase);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get()).isEqualTo("JOHN");
    result =
        Processable.ofNullable(findById(map, "5")).map(User::getUsername).map(String::toUpperCase);
    assertThat(result.isFailure()).isTrue();
  }

  private static User findById(Map<String, User> map, String id) {
    for (Map.Entry<String, User> entry : map.entrySet()) {
      if (entry.getKey().equals(id)) {
        return entry.getValue();
      }
    }
    return null;
  }

  @Getter
  @Setter
  private static class TestClass {
    private int id;
  }

  @Getter
  @Setter
  @AllArgsConstructor
  private static class User {
    private String username;
  }
}
