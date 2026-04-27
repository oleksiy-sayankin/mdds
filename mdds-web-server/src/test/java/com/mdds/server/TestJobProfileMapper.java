/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.domain.ArtifactFormat;
import com.mdds.domain.ParamType;
import com.mdds.domain.UnknownArtifactFormatException;
import com.mdds.domain.UnknownParamTypeException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestJobProfileMapper {

  private static Stream<List<String>> enumValues() {
    return Stream.of(null, List.of());
  }

  @ParameterizedTest
  @MethodSource("enumValues")
  void testToDomainEmptyEnum(List<String> enumValues) {
    var jobProfileConfig =
        new JobProfileConfig(
            "no_enum_values",
            true,
            List.of(
                new ArtifactConfig("matrix", "csv", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            List.of(new JobParamConfig("solvingMethod", "enum", true, enumValues)),
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    assertThatExceptionOfType(NoEnumValuesSpecifiedException.class)
        .isThrownBy(() -> JobProfileMapper.toDomain(jobProfileConfig))
        .withMessage("No enum values specified for 'solvingMethod'.");
  }

  @Test
  void testUnknownArtifactFormat() {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            List.of(
                new ArtifactConfig("matrix", "unknown_format", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            List.of(new JobParamConfig("solvingMethod", "enum", true, List.of())),
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    assertThatExceptionOfType(UnknownArtifactFormatException.class)
        .isThrownBy(() -> JobProfileMapper.toDomain(jobProfileConfig))
        .withMessage("Unknown or unsupported artifact format: 'unknown_format'.");
  }

  @Test
  void testEmptyArtifactFormat() {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            List.of(
                new ArtifactConfig("matrix", "", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            List.of(new JobParamConfig("solvingMethod", "enum", true, List.of())),
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    assertThatExceptionOfType(UnknownArtifactFormatException.class)
        .isThrownBy(() -> JobProfileMapper.toDomain(jobProfileConfig))
        .withMessage("Artifact format must not be null or blank.");
  }

  @Test
  void testUnknownParamType() {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            List.of(
                new ArtifactConfig("matrix", "csv", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            List.of(new JobParamConfig("solvingMethod", "unknown_type", true, List.of())),
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    assertThatExceptionOfType(UnknownParamTypeException.class)
        .isThrownBy(() -> JobProfileMapper.toDomain(jobProfileConfig))
        .withMessage("Unknown or unsupported parameter type: 'unknown_type'.");
  }

  @Test
  void testEmptyParamType() {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            List.of(
                new ArtifactConfig("matrix", "csv", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            List.of(new JobParamConfig("solvingMethod", "", true, List.of())),
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    assertThatExceptionOfType(UnknownParamTypeException.class)
        .isThrownBy(() -> JobProfileMapper.toDomain(jobProfileConfig))
        .withMessage("Parameter type must not be null or blank.");
  }

  @Test
  void testToDomain() {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            List.of(
                new ArtifactConfig("matrix", "csv", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            List.of(
                new JobParamConfig("tolerance", "number", false, List.of()),
                new JobParamConfig(
                    "solvingMethod",
                    "enum",
                    true,
                    List.of(
                        "numpy_exact_solver",
                        "numpy_lstsq_solver",
                        "numpy_pinv_solver",
                        "petsc_solver",
                        "scipy_gmres_solver"))),
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    var jobProfile = JobProfileMapper.toDomain(jobProfileConfig);

    assertThat(jobProfile).isNotNull();
    assertThat(jobProfile.enabled()).isTrue();
    assertThat(jobProfile.inputArtifacts()).containsKey("matrix").containsKey("rhs");

    var matrixSpec = jobProfile.inputArtifacts().get("matrix");
    assertThat(matrixSpec.fileName()).isEqualTo("matrix.csv");
    assertThat(matrixSpec.format()).isEqualTo(ArtifactFormat.CSV);

    var rhsSpec = jobProfile.inputArtifacts().get("rhs");
    assertThat(rhsSpec.fileName()).isEqualTo("rhs.csv");
    assertThat(rhsSpec.format()).isEqualTo(ArtifactFormat.CSV);

    assertThat(jobProfile.paramSpecs()).containsKey("solvingMethod");
    var solvingMethod = jobProfile.paramSpecs().get("solvingMethod");
    assertThat(solvingMethod.required()).isTrue();
    assertThat(solvingMethod.type()).isEqualTo(ParamType.ENUM);
    assertThat(solvingMethod.allowedValues())
        .containsExactlyInAnyOrder(
            "numpy_exact_solver",
            "numpy_lstsq_solver",
            "numpy_pinv_solver",
            "petsc_solver",
            "scipy_gmres_solver");

    assertThat(jobProfile.paramSpecs()).containsKey("tolerance");
    var tolerance = jobProfile.paramSpecs().get("tolerance");
    assertThat(tolerance.required()).isFalse();

    assertThat(jobProfile.outputArtifacts()).containsKey("solution");
    var solutionSpec = jobProfile.outputArtifacts().get("solution");
    assertThat(solutionSpec.fileName()).isEqualTo("solution.csv");
    assertThat(solutionSpec.format()).isEqualTo(ArtifactFormat.CSV);
  }

  private static Stream<List<ArtifactConfig>> artifactValues() {
    return Stream.of(null, List.of());
  }

  @ParameterizedTest
  @MethodSource("artifactValues")
  void testToDomainEmptyInputSlots(List<ArtifactConfig> inputSlots) {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            inputSlots,
            List.of(
                new JobParamConfig("tolerance", "number", false, List.of()),
                new JobParamConfig(
                    "solvingMethod",
                    "enum",
                    true,
                    List.of(
                        "numpy_exact_solver",
                        "numpy_lstsq_solver",
                        "numpy_pinv_solver",
                        "petsc_solver",
                        "scipy_gmres_solver"))),
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    var jobProfile = JobProfileMapper.toDomain(jobProfileConfig);

    assertThat(jobProfile).isNotNull();
    assertThat(jobProfile.enabled()).isTrue();
    assertThat(jobProfile.inputArtifacts()).isEmpty();

    assertThat(jobProfile.paramSpecs()).containsKey("solvingMethod");
    var solvingMethod = jobProfile.paramSpecs().get("solvingMethod");
    assertThat(solvingMethod.required()).isTrue();
    assertThat(solvingMethod.type()).isEqualTo(ParamType.ENUM);
    assertThat(solvingMethod.allowedValues())
        .containsExactlyInAnyOrder(
            "numpy_exact_solver",
            "numpy_lstsq_solver",
            "numpy_pinv_solver",
            "petsc_solver",
            "scipy_gmres_solver");

    assertThat(jobProfile.paramSpecs()).containsKey("tolerance");
    var tolerance = jobProfile.paramSpecs().get("tolerance");
    assertThat(tolerance.required()).isFalse();

    assertThat(jobProfile.outputArtifacts()).containsKey("solution");
    var solutionSpec = jobProfile.outputArtifacts().get("solution");
    assertThat(solutionSpec.fileName()).isEqualTo("solution.csv");
    assertThat(solutionSpec.format()).isEqualTo(ArtifactFormat.CSV);
  }

  private static Stream<List<JobParamConfig>> paramValues() {
    return Stream.of(null, List.of());
  }

  @ParameterizedTest
  @MethodSource("paramValues")
  void testToDomainEmptyParams(List<JobParamConfig> params) {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            List.of(
                new ArtifactConfig("matrix", "csv", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            params,
            List.of(new ArtifactConfig("solution", "csv", "solution.csv")));

    var jobProfile = JobProfileMapper.toDomain(jobProfileConfig);

    assertThat(jobProfile).isNotNull();
    assertThat(jobProfile.enabled()).isTrue();
    assertThat(jobProfile.inputArtifacts()).containsKey("matrix").containsKey("rhs");

    var matrixSpec = jobProfile.inputArtifacts().get("matrix");
    assertThat(matrixSpec.fileName()).isEqualTo("matrix.csv");
    assertThat(matrixSpec.format()).isEqualTo(ArtifactFormat.CSV);

    var rhsSpec = jobProfile.inputArtifacts().get("rhs");
    assertThat(rhsSpec.fileName()).isEqualTo("rhs.csv");
    assertThat(rhsSpec.format()).isEqualTo(ArtifactFormat.CSV);

    assertThat(jobProfile.paramSpecs()).isEmpty();

    assertThat(jobProfile.outputArtifacts()).containsKey("solution");
    var solutionSpec = jobProfile.outputArtifacts().get("solution");
    assertThat(solutionSpec.fileName()).isEqualTo("solution.csv");
    assertThat(solutionSpec.format()).isEqualTo(ArtifactFormat.CSV);
  }

  @ParameterizedTest
  @MethodSource("artifactValues")
  void testToDomainEmptyOutputSlots(List<ArtifactConfig> outputSlots) {
    var jobProfileConfig =
        new JobProfileConfig(
            "test",
            true,
            List.of(
                new ArtifactConfig("matrix", "csv", "matrix.csv"),
                new ArtifactConfig("rhs", "csv", "rhs.csv")),
            List.of(
                new JobParamConfig("tolerance", "number", false, List.of()),
                new JobParamConfig(
                    "solvingMethod",
                    "enum",
                    true,
                    List.of(
                        "numpy_exact_solver",
                        "numpy_lstsq_solver",
                        "numpy_pinv_solver",
                        "petsc_solver",
                        "scipy_gmres_solver"))),
            outputSlots);

    var jobProfile = JobProfileMapper.toDomain(jobProfileConfig);

    assertThat(jobProfile).isNotNull();
    assertThat(jobProfile.enabled()).isTrue();
    assertThat(jobProfile.inputArtifacts()).containsKey("matrix").containsKey("rhs");

    var matrixSpec = jobProfile.inputArtifacts().get("matrix");
    assertThat(matrixSpec.fileName()).isEqualTo("matrix.csv");
    assertThat(matrixSpec.format()).isEqualTo(ArtifactFormat.CSV);

    var rhsSpec = jobProfile.inputArtifacts().get("rhs");
    assertThat(rhsSpec.fileName()).isEqualTo("rhs.csv");
    assertThat(rhsSpec.format()).isEqualTo(ArtifactFormat.CSV);

    assertThat(jobProfile.paramSpecs()).containsKey("solvingMethod");
    var solvingMethod = jobProfile.paramSpecs().get("solvingMethod");
    assertThat(solvingMethod.required()).isTrue();
    assertThat(solvingMethod.type()).isEqualTo(ParamType.ENUM);
    assertThat(solvingMethod.allowedValues())
        .containsExactlyInAnyOrder(
            "numpy_exact_solver",
            "numpy_lstsq_solver",
            "numpy_pinv_solver",
            "petsc_solver",
            "scipy_gmres_solver");

    assertThat(jobProfile.paramSpecs()).containsKey("tolerance");
    var tolerance = jobProfile.paramSpecs().get("tolerance");
    assertThat(tolerance.required()).isFalse();

    assertThat(jobProfile.outputArtifacts()).isEmpty();
  }
}
