/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "mdds.object-storage")
@Validated
public record ObjectStorageProperties(
    @NotBlank(message = "must not be null or blank.") String bucket,
    @NotBlank(message = "must not be null or blank.") String region,
    String internalEndpoint,
    @NotBlank(message = "must not be null or blank.") String publicEndpoint,
    @NotBlank(message = "must not be null or blank.") String accessKey,
    @NotBlank(message = "must not be null or blank.") String secretKey,
    boolean pathStyleAccessEnabled,
    @NotNull(message = "must not be null.") Duration presignPutTtl) {}
