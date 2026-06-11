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
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openqa.selenium.chrome.ChromeOptions;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestSolverUiE2EIT {
  private static final double SOLUTION_TOLERANCE = 1.0E-7;
  private static final int SELENIUM_PORT = 4444;
  private static final String WEB_SERVER_SERVICE = "web-server-1";
  private static final int WEB_SERVER_PORT = 8000;

  private static final String REMOTE_DOWNLOAD_DIR = "/home/seluser/Downloads";
  private static final String SOLUTION_FILE_NAME = "solution.json";
  private static final String REMOTE_SOLUTION_FILE = REMOTE_DOWNLOAD_DIR + "/" + SOLUTION_FILE_NAME;

  private static ContainerState selenium;
  private static final String SELENIUM_SERVICE = "selenium-1";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Path DOWNLOAD_DIR = Path.of("target", "selenide-downloads").toAbsolutePath();

  @Container
  static final ComposeContainer ENVIRONMENT =
      new ComposeContainer(getFileFromResources("e2e/ui/docker-compose.yml"))
          .withExposedService(
              WEB_SERVER_SERVICE,
              WEB_SERVER_PORT,
              Wait.forHttp("/health").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
          .withExposedService(
              SELENIUM_SERVICE,
              SELENIUM_PORT,
              Wait.forHttp("/status").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));

  @BeforeAll
  static void setup() throws Exception {
    Files.createDirectories(DOWNLOAD_DIR);
    Files.deleteIfExists(DOWNLOAD_DIR.resolve(SOLUTION_FILE_NAME));

    var seleniumHost = ENVIRONMENT.getServiceHost(SELENIUM_SERVICE, SELENIUM_PORT);
    var seleniumPort = ENVIRONMENT.getServicePort(SELENIUM_SERVICE, SELENIUM_PORT);

    selenium =
        ENVIRONMENT
            .getContainerByServiceName(SELENIUM_SERVICE)
            .orElseThrow(() -> new IllegalStateException("Selenium container not found"));

    Configuration.remote = "http://" + seleniumHost + ":" + seleniumPort + "/wd/hub";

    // Important: this URL is opened by Chrome inside the compose network.
    Configuration.baseUrl = "http://web-server:" + WEB_SERVER_PORT;

    Configuration.browser = "chrome";
    Configuration.browserCapabilities = createChromeOptions();
    Configuration.timeout = 60_000;
    Configuration.downloadsFolder = DOWNLOAD_DIR.toString();

    log.info("Selenium remote: {}", Configuration.remote);
    log.info("Browser baseUrl: {}", Configuration.baseUrl);
    log.info("Local download dir: {}", DOWNLOAD_DIR);
    log.info("Remote download dir: {}", REMOTE_DOWNLOAD_DIR);
  }

  @BeforeEach
  void setupBrowser() throws Exception {
    closeWebDriver();

    Files.deleteIfExists(DOWNLOAD_DIR.resolve(SOLUTION_FILE_NAME));

    var result = selenium.execInContainer("rm", "-f", REMOTE_SOLUTION_FILE);
    if (result.getExitCode() != 0) {
      throw new AssertionError("Failed to clean remote downloaded file: " + result.getStderr());
    }
  }

  @AfterEach
  void tearDownBrowser() {
    closeWebDriver();
  }

  @AfterAll
  static void tearDownAll() {
    closeWebDriver();
  }

  private static ChromeOptions createChromeOptions() {
    var chromeOptions = new ChromeOptions();

    chromeOptions.addArguments("--headless=new");
    chromeOptions.addArguments("--disable-dev-shm-usage");
    chromeOptions.addArguments("--no-sandbox");
    chromeOptions.addArguments("--disable-gpu");
    chromeOptions.addArguments("--window-size=1920,1080");
    chromeOptions.addArguments("--remote-allow-origins=*");

    chromeOptions.setExperimentalOption(
        "prefs",
        java.util.Map.of(
            "download.default_directory", REMOTE_DOWNLOAD_DIR,
            "download.prompt_for_download", false,
            "download.directory_upgrade", true,
            "safebrowsing.enabled", true));

    return chromeOptions;
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
    var target = DOWNLOAD_DIR.resolve(SOLUTION_FILE_NAME);

    await("Downloaded file " + target + " should exist and be non-empty")
        .atMost(timeout)
        .pollInterval(Duration.ofMillis(200))
        .until(() -> copyDownloadedFileFromBrowserContainer(target));

    return target;
  }

  private static boolean copyDownloadedFileFromBrowserContainer(Path target) {
    try {
      var checkResult = selenium.execInContainer("test", "-s", REMOTE_SOLUTION_FILE);
      if (checkResult.getExitCode() != 0) {
        return false;
      }

      Files.createDirectories(target.getParent());
      selenium.copyFileFromContainer(REMOTE_SOLUTION_FILE, target.toString());

      return isExistingNonEmptyFile(target);
    } catch (Exception e) {
      return false;
    }
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
