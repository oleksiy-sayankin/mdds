/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

import jakarta.annotation.Nonnull;
import java.util.Map;
import lombok.Getter;

/**
 * This descriptor gives information on source of the data that is used to solve system of linear
 * algebraic equation. We can get data from http request itself or from s3 storage or from MySql
 * database etc.
 *
 * <p>Key/Value storage <i>params</i> contains additional information to connect to the data, e.g.
 * connection string, user, password etc.
 */
@Getter
public class DataSourceDescriptor {
  public enum Type {
    HTTP_REQUEST("http_request"),
    S3("s3"),
    MYSQL("mysql");

    @Getter private final String name;

    Type(String name) {
      this.name = name;
    }

    public static Type parse(String type) {
      return switch (type) {
        case "http_request" -> HTTP_REQUEST;
        case "s3" -> S3;
        case "mysql" -> MYSQL;
        default -> throw new DataSourceTypeException("Can not parse data source type" + type);
      };
    }

    public static boolean isValid(String type) {
      if (type == null) {
        return false;
      }
      for (var dsType : Type.values()) {
        if (dsType.getName().equals(type)) {
          return true;
        }
      }
      return false;
    }
  }

  @Nonnull private final Type type;
  @Nonnull private final Map<String, Object> params;

  private DataSourceDescriptor(@Nonnull Type type, @Nonnull Map<String, Object> params) {
    this.type = type;
    this.params = params;
  }

  public static DataSourceDescriptor of(@Nonnull Type type, @Nonnull Map<String, Object> params) {
    return new DataSourceDescriptor(type, params);
  }
}
