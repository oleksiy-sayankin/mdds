/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.api;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Getter;

/**
 * Basic class for processing any methods. Very similar to Java Optional, but the difference is when
 * we can not get the result of a method we return so-called failure. Failure means that
 * <i>value</i> is <b>null</b>, and we can check for <i>errorMessage</i> or/and <i>cause</i> of
 * failure represented as <i>Throwable</i>.
 *
 * @param <T> type of the return object in case when method was executed successfully.
 */
public class Processable<T> {
  private final T value;
  @Getter private final String errorMessage;
  @Getter private final Throwable cause;

  private Processable(T value, String errorMessage, Throwable cause) {
    this.value = value;
    this.errorMessage = errorMessage;
    this.cause = cause;
  }

  public static <T> Processable<T> of(@Nonnull T value) {
    return new Processable<>(value, null, null);
  }

  public static <T> Processable<T> failure(String message, Throwable cause) {
    return new Processable<>(null, message, cause);
  }

  public static <T> Processable<T> failure(String message) {
    return new Processable<>(null, message, null);
  }

  public boolean isPresent() {
    return value != null;
  }

  public boolean isFailure() {
    return value == null;
  }

  public Optional<T> toOptional() {
    return Optional.ofNullable(value);
  }

  public T get() {
    if (value == null)
      throw new ProcessableStateException("No value present. Error: " + errorMessage, cause);
    return value;
  }

  public void ifPresent(Consumer<? super T> action) {
    if (isPresent()) action.accept(value);
  }

  public void ifFailure(Consumer<? super Processable<T>> action) {
    if (isFailure()) action.accept(this);
  }

  public <U> Processable<U> map(Function<? super T, ? extends U> mapper) {
    if (isFailure()) return failure(errorMessage, cause);
    try {
      return of(mapper.apply(value));
    } catch (Exception e) {
      return failure(e.getMessage(), e);
    }
  }
}
