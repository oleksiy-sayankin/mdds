/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

import static com.mdds.domain.JobType.SOLVING_SLAE;
import static com.mdds.domain.JobType.SOLVING_SLAE_PARALLEL;

import java.util.Map;

/** Contains list of all job profiles. */
public final class JobProfiles {

  private static final Map<JobType, JobProfile> PROFILES =
      Map.of(
          SOLVING_SLAE,
          new JobProfile(
              SOLVING_SLAE,
              Map.of(
                  "matrix", new ArtifactSpec("matrix.csv", ArtifactFormat.CSV),
                  "rhs", new ArtifactSpec("rhs.csv", ArtifactFormat.CSV)),
              Map.of("solution", new ArtifactSpec("solution.csv", ArtifactFormat.CSV)),
              Map.of(
                  "solvingMethod",
                  new JobParamSpec(ParamType.ENUM, true, SlaeSolver.asStringSet()),
                  "precision",
                  new JobParamSpec(ParamType.NUMBER, false)),
              true),
          SOLVING_SLAE_PARALLEL,
          new JobProfile(SOLVING_SLAE_PARALLEL, Map.of(), Map.of(), Map.of(), false));

  static {
    for (var jobType : JobType.values()) {
      var profile = PROFILES.get(jobType);
      if (profile == null) {
        throw new JobProfileNotConfiguredException("No profile for job type: '" + jobType + "'.");
      }
      if (profile.jobType() != jobType) {
        throw new InvalidProfileKeyException(
            "Profile key "
                + jobType
                + " does not match embedded profile for job type: '"
                + profile.jobType()
                + "'.");
      }
    }
  }

  public static JobProfile forType(JobType jobType) {
    var profile = PROFILES.get(jobType);
    if (profile == null) {
      throw new JobProfileNotConfiguredException("No profile for job type: " + jobType);
    }
    return profile;
  }

  private JobProfiles() {}
}
