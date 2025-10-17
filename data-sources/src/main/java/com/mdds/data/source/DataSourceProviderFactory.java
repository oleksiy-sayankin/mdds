/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

import com.mdds.api.Processable;
import com.mdds.data.source.provider.SlaeDataSourceProvider;
import com.mdds.data.source.provider.http.request.HttpRequestConfig;
import com.mdds.data.source.provider.http.request.HttpRequestSlaeDataSourceProvider;
import com.mdds.data.source.provider.mysql.MySqlConfig;
import com.mdds.data.source.provider.mysql.MySqlSlaeDataSourceProvider;
import com.mdds.data.source.provider.s3.S3Config;
import com.mdds.data.source.provider.s3.S3DataSlaeDataSourceProvider;

/** Factory to create Data Source Provider from its descriptor. */
public final class DataSourceProviderFactory {
  private DataSourceProviderFactory() {}

  public static Processable<SlaeDataSourceProvider> fromDescriptor(
      DataSourceDescriptor descriptor) {
    var params = descriptor.getParams();
    var type = descriptor.getType();
    switch (type) {
      case HTTP_REQUEST -> {
        return Processable.of(new HttpRequestSlaeDataSourceProvider(HttpRequestConfig.of(params)));
      }
      case S3 -> {
        return Processable.of(new S3DataSlaeDataSourceProvider(S3Config.of(params)));
      }
      case MYSQL -> {
        return Processable.of(new MySqlSlaeDataSourceProvider(MySqlConfig.of(params)));
      }
      default -> {
        return Processable.failure("Unknown descriptor type: " + type);
      }
    }
  }
}
