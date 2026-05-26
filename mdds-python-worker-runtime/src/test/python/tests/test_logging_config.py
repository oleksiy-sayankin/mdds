# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import json
import logging

from mdds_worker_runtime import logging_config
from mdds_worker_runtime.logging_config import JsonLogFormatter, setup_logging


def flush_log_handlers() -> None:
    for handler in logging.getLogger().handlers:
        handler.flush()


def test_setup_logging_writes_json_log_to_file(tmp_path):
    # In pytest, tmp_path is a built-in fixture that provides a
    # temporary directory unique to each test function.
    log_file = tmp_path / "mdds-test.log"

    logging_config.setup_logging(
        service_name="test-service",
        log_file=str(log_file),
        level=logging.DEBUG,
    )

    logger = logging.getLogger("mdds_worker_runtime.tests.logging_config")
    logger.info(
        "Test logging message.",
        extra={
            "event": "test_event",
            "jobId": "job-1",
            "workerId": "worker-1",
            "status": "IN_PROGRESS",
            "progress": 42,
        },
    )

    flush_log_handlers()

    assert log_file.exists()
    assert log_file.is_file()

    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert lines
    payload = json.loads(lines[-1])

    assert payload["service"] == "test-service"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "mdds_worker_runtime.tests.logging_config"
    assert payload["message"] == "Test logging message."
    assert payload["event"] == "test_event"
    assert payload["jobId"] == "job-1"
    assert payload["workerId"] == "worker-1"
    assert payload["status"] == "IN_PROGRESS"
    assert payload["progress"] == 42

    assert "ts" in payload
    assert payload["ts"].endswith("Z")
    assert "thread" in payload
    assert "process" in payload
    assert "pid" in payload
    assert "module" in payload
    assert "function" in payload
    assert "line" in payload


def test_setup_logging_writes_json_log_to_stdout(capsys):
    # In pytest, capsys is a built-in fixture used to capture text written to
    # standard output (stdout) and standard error (stderr).
    logging_config.setup_logging(
        service_name="test-service",
        log_file=None,
        level=logging.INFO,
    )

    logger = logging.getLogger("mdds_worker_runtime.tests.stdout")
    logger.info("Hello stdout.", extra={"event": "stdout_test"})

    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())

    assert payload["service"] == "test-service"
    assert payload["logger"] == "mdds_worker_runtime.tests.stdout"
    assert payload["message"] == "Hello stdout."
    assert payload["event"] == "stdout_test"


def test_json_log_contains_exception(tmp_path):
    log_file = tmp_path / "exception.log"

    logging_config.setup_logging(
        service_name="test-service",
        log_file=str(log_file),
        level=logging.INFO,
    )

    logger = logging.getLogger("mdds_worker_runtime.tests.exception")

    try:
        raise RuntimeError("Boom")
    except RuntimeError:
        logger.exception("Failed to process job.", extra={"jobId": "job-1"})

    flush_log_handlers()

    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert lines
    payload = json.loads(lines[-1])

    assert payload["message"] == "Failed to process job."
    assert payload["jobId"] == "job-1"
    assert "exception" in payload
    assert "RuntimeError: Boom" in payload["exception"]


def test_setup_logging_uses_default_service_name(capsys):
    logging_config.setup_logging(log_file=None, level=logging.INFO)

    logger = logging.getLogger("mdds_worker_runtime.tests.default_service")
    logger.info("Default service name.")

    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())

    assert payload["service"] == logging_config.SERVICE_NAME


def test_json_log_preserves_unicode(capsys):
    logging_config.setup_logging(service_name="test-service", log_file=None)

    logger = logging.getLogger("mdds_worker_runtime.tests.unicode")
    message = "Échec de l’exécution: entrée invalide à Noël."
    logger.info(message)

    captured = capsys.readouterr()
    assert message in captured.out

    payload = json.loads(captured.out.strip())
    assert payload["message"] == message


def test_json_log_formatter_includes_extra_fields():
    formatter = JsonLogFormatter(service_name="test-service")

    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.jobId = "job-1"
    record.workerId = "worker-1"
    record.event = "manifest_loaded"
    record.component = "manifest_loader"
    record.manifestObjectKey = "jobs/123/job-1/manifest.json"

    actual = json.loads(formatter.format(record))

    assert actual["service"] == "test-service"
    assert actual["level"] == "INFO"
    assert actual["message"] == "Test message"
    assert actual["jobId"] == "job-1"
    assert actual["workerId"] == "worker-1"
    assert actual["event"] == "manifest_loaded"
    assert actual["component"] == "manifest_loader"
    assert actual["manifestObjectKey"] == "jobs/123/job-1/manifest.json"


def test_setup_logging_configures_json_stdout(capsys):
    setup_logging(service_name="test-service", level=logging.INFO)

    logger = logging.getLogger("test.logger")
    logger.info("Hello", extra={"event": "test_event"})

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["service"] == "test-service"
    assert payload["message"] == "Hello"
    assert payload["event"] == "test_event"
