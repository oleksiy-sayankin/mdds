/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.domain.JobProfileNotConfiguredException;
import com.mdds.domain.ParamType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;

@SpringBootTest(
    classes = TestYamlJobProfileRead.TestConfig.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "spring.config.import=classpath:test-job-profiles.yml",
      "mdds.job-profile.mode=yaml"
    })
class TestYamlJobProfileRead {
  @Autowired private JobProfileRegistry jobProfileRegistry;

  @TestConfiguration(proxyBeanMethods = false)
  @EnableAutoConfiguration(
      exclude = {
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        FlywayAutoConfiguration.class
      })
  @Import({JobProfilesConfig.class, YamlJobProfileRegistry.class})
  static class TestConfig {}

  @Test
  void testProfileReadByType() {
    var jobProfile = jobProfileRegistry.forType("other_test_job_type");
    assertThat(jobProfile.enabled()).isTrue();

    // input slots
    var inputArtifacts = jobProfile.inputArtifacts();
    assertThat(inputArtifacts).containsKey("some_test_input_slot");
    var artifactSpec = inputArtifacts.get("some_test_input_slot");
    assertThat(artifactSpec.fileName()).isEqualTo("input.csv");
    var artifactFormat = artifactSpec.format();
    assertThat(artifactFormat.getValue()).isEqualTo("csv");

    // params
    var params = jobProfile.paramSpecs();
    assertThat(params)
        .containsKey("someStringTestParam")
        .containsKey("someMapTestParam")
        .containsKey("someBooleanTestParam")
        .containsKey("someEnumTestParam")
        .containsKey("someNumberTestParam");
    var paramSpec = params.get("someStringTestParam");
    assertThat(paramSpec.type()).isEqualTo(ParamType.STRING);
    assertThat(paramSpec.required()).isTrue();
    paramSpec = params.get("someMapTestParam");
    assertThat(paramSpec.type()).isEqualTo(ParamType.MAP);
    assertThat(paramSpec.required()).isFalse();
    paramSpec = params.get("someNumberTestParam");
    assertThat(paramSpec.type()).isEqualTo(ParamType.NUMBER);
    assertThat(paramSpec.required()).isTrue();
    paramSpec = params.get("someBooleanTestParam");
    assertThat(paramSpec.type()).isEqualTo(ParamType.BOOLEAN);
    assertThat(paramSpec.required()).isFalse();
    paramSpec = params.get("someEnumTestParam");
    assertThat(paramSpec.type()).isEqualTo(ParamType.ENUM);
    assertThat(paramSpec.required()).isFalse();
    assertThat(paramSpec.allowedValues())
        .containsExactlyInAnyOrder("first_value", "second_value", "third_value");

    // output slot
    var outputArtifacts = jobProfile.outputArtifacts();
    assertThat(outputArtifacts).containsKey("my_other_test_output_slot");
    artifactSpec = outputArtifacts.get("my_other_test_output_slot");
    assertThat(artifactSpec.fileName()).isEqualTo("other_output.csv");
    artifactFormat = artifactSpec.format();
    assertThat(artifactFormat.getValue()).isEqualTo("csv");
  }

  @Test
  void testProfileReadNoInputArtifacts() {
    var jobProfile = jobProfileRegistry.forType("no_input_slot_job_type");
    assertThat(jobProfile.enabled()).isTrue();
    var inputArtifacts = jobProfile.inputArtifacts();
    assertThat(inputArtifacts).isEmpty();
  }

  @Test
  void testUnknownJobType() {
    var jobType = "unknown_job_type";
    assertThatExceptionOfType(JobProfileNotConfiguredException.class)
        .isThrownBy(() -> jobProfileRegistry.forType(jobType))
        .withMessage("No profile for job type: '" + jobType + "'.");
  }

  @Test
  void testDisabledJobType() {
    var jobProfile = jobProfileRegistry.forType("disabled_job_type");
    assertThat(jobProfile.enabled()).isFalse();
  }

  @Test
  void testReadAllJobTypes() {
    var jobTypes = jobProfileRegistry.jobTypes();
    assertThat(jobTypes)
        .containsExactlyInAnyOrder(
            "solving_slae",
            "test_job_type",
            "other_test_job_type",
            "no_input_slot_job_type",
            "solving_slae_parallel",
            "disabled_job_type");
  }
}
