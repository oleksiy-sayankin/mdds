/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.ArtifactFormat;
import com.mdds.domain.ArtifactSpec;
import com.mdds.domain.JobParamSpec;
import com.mdds.domain.JobProfile;
import com.mdds.domain.JobProfileNotConfiguredException;
import com.mdds.domain.ParamType;
import com.mdds.domain.SlaeSolver;
import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/** Container for all available job types. */
public interface JobProfileRegistry {
  /**
   * Returns job profile for given job type.
   *
   * @param jobType job type defined in job profile;
   * @return job profile for given job type.
   */
  JobProfile forType(String jobType);

  /**
   * Returns all available job types including disabled ones.
   *
   * @return set of all job types.
   */
  Set<String> jobTypes();
}

@Service
@ConditionalOnProperty(
    name = "mdds.job-profile.mode",
    havingValue = "inmemory",
    matchIfMissing = true)
class InMemoryJobProfileRegistry implements JobProfileRegistry {
  private final Map<String, JobProfile> profiles =
      Map.of(
          "solving_slae",
          new JobProfile(
              true,
              Map.of(
                  "matrix", new ArtifactSpec("matrix.csv", ArtifactFormat.CSV),
                  "rhs", new ArtifactSpec("rhs.csv", ArtifactFormat.CSV)),
              Map.of("solution", new ArtifactSpec("solution.csv", ArtifactFormat.CSV)),
              Map.of(
                  "solvingMethod",
                  new JobParamSpec(ParamType.ENUM, true, SlaeSolver.asStringSet()),
                  "tolerance",
                  new JobParamSpec(ParamType.NUMBER, false))),
          "solving_slae_parallel",
          new JobProfile(false, Map.of(), Map.of(), Map.of()));

  @Override
  public JobProfile forType(String jobType) {
    var profile = profiles.get(jobType);
    if (profile == null) {
      throw new JobProfileNotConfiguredException("No profile for job type: '" + jobType + "'.");
    }
    return profile;
  }

  @Override
  public Set<String> jobTypes() {
    return profiles.keySet();
  }
}

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "mdds.job-profile.mode", havingValue = "yaml")
class YamlJobProfileRegistry implements JobProfileRegistry {
  private final JobProfilesProperties jobProfilesProperties;
  private final Map<String, JobProfile> profiles = new HashMap<>();

  @PostConstruct
  public void init() {
    var jobs = jobProfilesProperties.jobs();
    for (JobProfileConfig jobProfileConfig : jobs) {
      var jobType = jobProfileConfig.type();
      profiles.put(jobType, JobProfileMapper.toDomain(jobProfileConfig));
    }
  }

  @Override
  public JobProfile forType(String jobType) {
    var profile = profiles.get(jobType);
    if (profile == null) {
      throw new JobProfileNotConfiguredException("No profile for job type: '" + jobType + "'.");
    }
    return profile;
  }

  @Override
  public Set<String> jobTypes() {
    return profiles.keySet();
  }
}
