/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.mysql;

import java.util.Map;
import lombok.Getter;

/** Configuration class for MySql data source provider. */
@Getter
public class MySqlConfig {
  private final String url;
  private final String user;
  private final String password;
  private final String dbName;
  private final String matrixTableName;
  private final String matrixJsonFieldName;
  private final String matrixPrimaryKeyFieldName;
  private final String matrixPrimaryKeyFieldValue;
  private final String rhsTableName;
  private final String rhsJsonFieldName;
  private final String rhsPrimaryKeyFieldName;
  private final String rhsPrimaryKeyFieldValue;

  private MySqlConfig(Map<String, Object> params) {
    this.url = (String) params.get("mysql.url");
    this.user = (String) params.get("mysql.user");
    this.password = (String) params.get("mysql.password");
    this.dbName = (String) params.get("mysql.db.name");
    this.matrixTableName = (String) params.get("mysql.matrix.table.name");
    this.matrixJsonFieldName = (String) params.get("mysql.matrix.json.field.name");
    this.matrixPrimaryKeyFieldName = (String) params.get("mysql.matrix.primary.key.field.name");
    this.matrixPrimaryKeyFieldValue = (String) params.get("mysql.matrix.primary.key.field.value");
    this.rhsTableName = (String) params.get("mysql.rhs.table.name");
    this.rhsJsonFieldName = (String) params.get("mysql.rhs.json.field.name");
    this.rhsPrimaryKeyFieldName = (String) params.get("mysql.rhs.primary.key.field.name");
    this.rhsPrimaryKeyFieldValue = (String) params.get("mysql.rhs.primary.key.field.value");
  }

  public static MySqlConfig of(Map<String, Object> params) {
    return new MySqlConfig(params);
  }

  @Override
  public String toString() {
    return "MySqlConfig{"
        + "url='"
        + url
        + '\''
        + ", user='"
        + user
        + '\''
        + ", dbName='"
        + dbName
        + '\''
        + ", matrixTableName='"
        + matrixTableName
        + '\''
        + ", matrixJsonFieldName='"
        + matrixJsonFieldName
        + '\''
        + ", matrixPrimaryKeyFieldName='"
        + matrixPrimaryKeyFieldName
        + '\''
        + ", matrixPrimaryKeyFieldValue='"
        + matrixPrimaryKeyFieldValue
        + '\''
        + ", rhsTableName='"
        + rhsTableName
        + '\''
        + ", rhsJsonFieldName='"
        + rhsJsonFieldName
        + '\''
        + ", rhsPrimaryKeyFieldName='"
        + rhsPrimaryKeyFieldName
        + '\''
        + ", rhsPrimaryKeyFieldValue='"
        + rhsPrimaryKeyFieldValue
        + '\''
        + '}';
  }
}
