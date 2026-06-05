/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test.e2e.ui;

import static com.codeborne.selenide.Condition.enabled;
import static com.codeborne.selenide.Condition.text;
import static com.codeborne.selenide.Selenide.$;
import static com.codeborne.selenide.Selenide.closeWebDriver;
import static com.codeborne.selenide.Selenide.open;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.codeborne.selenide.Configuration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openqa.selenium.chrome.ChromeOptions;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestSolverUiE2EIT {
  private static final double SOLUTION_TOLERANCE = 1.0E-7;
  private static final String CHROME_VERSION =
      System.getProperty("mdds.e2e.ui.chrome.version", "135");
  private static final String WEB_SERVER_SERVICE = "web-server-1";
  private static final int WEB_SERVER_PORT = 8000;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Path DOWNLOAD_DIR = Path.of("target", "selenide-downloads").toAbsolutePath();

  @Container
  static final ComposeContainer ENVIRONMENT =
      new ComposeContainer(getFileFromResources("e2e/ui/docker-compose.yml"))
          .withExposedService(
              WEB_SERVER_SERVICE,
              WEB_SERVER_PORT,
              Wait.forHttp("/health").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

  @BeforeAll
  static void setup() throws Exception {
    var host = ENVIRONMENT.getServiceHost(WEB_SERVER_SERVICE, WEB_SERVER_PORT);
    var port = ENVIRONMENT.getServicePort(WEB_SERVER_SERVICE, WEB_SERVER_PORT);

    Files.createDirectories(DOWNLOAD_DIR);

    try (var files = Files.list(DOWNLOAD_DIR)) {
      files.forEach(
          path -> {
            try {
              Files.deleteIfExists(path);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }

    Configuration.baseUrl = "http://" + host + ":" + port;
    Configuration.browser = "chrome";
    Configuration.headless = true;
    Configuration.timeout = 60_000;
    Configuration.downloadsFolder = DOWNLOAD_DIR.toString();

    var chromeOptions = new ChromeOptions();

    // Use Chrome for Testing instead of the locally installed Chrome.
    // Version 135 matches selenium-devtools-v135 from Selenium 4.31.0 by default.
    chromeOptions.setBrowserVersion(CHROME_VERSION);

    chromeOptions.addArguments("--no-sandbox");
    chromeOptions.addArguments("--disable-dev-shm-usage");
    chromeOptions.addArguments("--headless=new");

    chromeOptions.setExperimentalOption(
        "prefs",
        java.util.Map.of(
            "download.default_directory", DOWNLOAD_DIR.toString(),
            "download.prompt_for_download", false,
            "download.directory_upgrade", true,
            "safebrowsing.enabled", true));

    Configuration.browserCapabilities = chromeOptions;
  }

  @AfterAll
  static void tearDownBrowser() {
    closeWebDriver();
  }

  private record SolverCase(String value, String label) {
    @Override
    public @NonNull String toString() {
      return value;
    }
  }

  private static List<SolverCase> solvers() {
    return List.of(
        new SolverCase("numpy_exact_solver", "NumPy Exact Solver"),
        new SolverCase("numpy_lstsq_solver", "NumPy Least Squares"),
        new SolverCase("numpy_pinv_solver", "NumPy Pseudo-Inverse"),
        new SolverCase("petsc_solver", "PETSc Solver"),
        new SolverCase("scipy_gmres_solver", "SciPy GMRES Solver"));
  }

  @ParameterizedTest(name = "Solve SLAE via Web UI using {0}")
  @MethodSource("solvers")
  void testSolveSlaeViaWebUiAndDownloadSolution(SolverCase solverCase) throws Exception {
    var matrixFile = getFileFromResources("e2e/ui/matrix.csv");
    var rhsFile = getFileFromResources("e2e/ui/rhs.csv");
    var expectedFile = getPathFromResources("e2e/ui/expected-solution.json");

    Files.deleteIfExists(DOWNLOAD_DIR.resolve("solution.json"));

    open("/");

    selectSolver(solverCase);

    $("[data-testid='matrix-file-input']").uploadFile(matrixFile);
    $("[data-testid='rhs-file-input']").uploadFile(rhsFile);
    $("[data-testid='solve-button']").shouldBe(enabled).click();
    $("[data-testid='download-solution-button']").shouldBe(enabled, Duration.ofSeconds(45)).click();

    var downloadedFile = waitForDownloadedFile(Duration.ofSeconds(10));

    var expected = readDoubleList(expectedFile);
    var actual = readDoubleList(downloadedFile);

    assertDoubleListsEqual(expected, actual);
  }

  private static void selectSolver(SolverCase solverCase) {
    $("[data-testid='solver-select']").click();
    $("[data-testid='solver-option-" + solverCase.value() + "']")
        .shouldHave(text(solverCase.label()))
        .click();
  }

  private static Path waitForDownloadedFile(Duration timeout) {
    var target = DOWNLOAD_DIR.resolve("solution.json");

    await("Downloaded file " + target + " should exist and be non-empty")
        .atMost(timeout)
        .pollInterval(Duration.ofMillis(200))
        .until(() -> isExistingNonEmptyFile(target));

    return target;
  }

  private static boolean isExistingNonEmptyFile(Path path) {
    try {
      return Files.isRegularFile(path) && Files.size(path) > 0;
    } catch (Exception e) {
      return false;
    }
  }

  private static List<Double> readDoubleList(Path path) throws Exception {
    return OBJECT_MAPPER.readValue(path.toFile(), new TypeReference<List<Double>>() {});
  }

  private static void assertDoubleListsEqual(List<Double> expected, List<Double> actual) {
    assertEquals(expected.size(), actual.size(), "Different solution vector size");

    for (int i = 0; i < expected.size(); i++) {
      var diff = Math.abs(expected.get(i) - actual.get(i));
      assertTrue(
          diff <= SOLUTION_TOLERANCE,
          "Different value at index "
              + i
              + ": expected="
              + expected.get(i)
              + ", actual="
              + actual.get(i));
    }
  }

  private static File getFileFromResources(String fileName) {
    var resourceUrl = TestSolverUiE2EIT.class.getClassLoader().getResource(fileName);
    assertThat(resourceUrl).isNotNull();

    try {
      return new File(resourceUrl.toURI());
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  private static Path getPathFromResources(String fileName) {
    var resourceUrl = TestSolverUiE2EIT.class.getClassLoader().getResource(fileName);
    assertThat(resourceUrl).isNotNull();

    try {
      return Paths.get(resourceUrl.toURI());
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }
}
