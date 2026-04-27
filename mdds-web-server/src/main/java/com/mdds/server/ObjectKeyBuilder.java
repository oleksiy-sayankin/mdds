/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Creates object key by userId, jobId. */
public final class ObjectKeyBuilder {
  private ObjectKeyBuilder() {}

  private static final String JOBS = "jobs";
  private static final String IN = "in";
  private static final String OUT = "out";
  private static final String MANIFEST_JSON = "manifest.json";

  public static String canonicalInputObjectKey(long userId, String jobId, String fileName) {
    return canonicalKey(userId, jobId) + "/" + IN + "/" + fileName;
  }

  public static String canonicalOutputObjectKey(long userId, String jobId, String fileName) {
    return canonicalKey(userId, jobId) + "/" + OUT + "/" + fileName;
  }

  public static String manifestObjectKey(long userId, String jobId) {
    return canonicalKey(userId, jobId) + "/" + MANIFEST_JSON;
  }

  private static String canonicalKey(long userId, String jobId) {
    return JOBS + "/" + userId + "/" + jobId;
  }
}
