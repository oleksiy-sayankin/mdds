# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from dataclasses import replace
from pathlib import Path

import pytest

from mdds_worker_runtime import config


def set_required_worker_env(monkeypatch, **overrides):
    env = {
        "MDDS_WORKER_JOB_TYPE": "solving_slae",
        "MDDS_RABBITMQ_HOST": "rabbitmq",
        "MDDS_RABBITMQ_USER": "mdds",
        "MDDS_RABBITMQ_PASSWORD": "secret",
        "MDDS_OBJECT_STORAGE_INTERNAL_ENDPOINT": "http://minio:9000",
        "MDDS_OBJECT_STORAGE_BUCKET": "mdds",
        "MDDS_OBJECT_STORAGE_ACCESS_KEY": "minioadmin",
        "MDDS_OBJECT_STORAGE_SECRET_KEY": "minioadmin",
        "MDDS_WORKER_HANDLER": "mdds_slae_worker.handler:SlaeJobHandler",
    }

    env.update(overrides)

    for key, value in env.items():
        monkeypatch.setenv(key, value)


def test_load_config_with_required_env_and_defaults(monkeypatch):
    set_required_worker_env(monkeypatch, MDDS_WORKER_ID="test-worker-id")

    worker_config = config.load_config()

    assert worker_config.worker_id == "test-worker-id"
    assert worker_config.worker_job_type == "solving_slae"

    assert worker_config.worker_job_queue_name == "queue-solving_slae"
    assert worker_config.worker_cancel_queue_name == "cancel.queue-test-worker-id"
    assert worker_config.worker_status_queue_name == "mdds_status_queue"

    assert worker_config.rabbitmq_host == "rabbitmq"
    assert worker_config.rabbitmq_port == 5672
    assert worker_config.rabbitmq_user == "mdds"
    assert worker_config.rabbitmq_password == "secret"

    assert worker_config.object_storage_endpoint_url == "http://minio:9000"
    assert worker_config.object_storage_bucket == "mdds"
    assert worker_config.object_storage_access_key == "minioadmin"
    assert worker_config.object_storage_secret_key == "minioadmin"
    assert worker_config.object_storage_region == "us-east-1"
    assert worker_config.object_storage_path_style_access_enabled is True

    assert worker_config.worker_handler == "mdds_slae_worker.handler:SlaeJobHandler"
    assert worker_config.worker_job_timeout_seconds == 3600
    assert worker_config.worker_cleanup_interval_seconds == 1
    assert worker_config.worker_progress_interval_seconds == 5
    assert worker_config.worker_local_root == Path("/opt/mdds")
    assert worker_config.jobs_root == Path("/opt/mdds/jobs")


def test_load_config_with_overridden_optional_values(monkeypatch):
    set_required_worker_env(
        monkeypatch,
        MDDS_WORKER_ID="worker-1",
        MDDS_WORKER_JOB_QUEUE_NAME="custom.job.queue",
        MDDS_WORKER_CANCEL_QUEUE_NAME="custom.cancel.queue",
        MDDS_WORKER_STATUS_QUEUE_NAME="custom.status.queue",
        MDDS_RABBITMQ_PORT="5673",
        MDDS_OBJECT_STORAGE_REGION="eu-central-1",
        MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED="false",
        MDDS_WORKER_JOB_TIMEOUT_SECONDS="100",
        MDDS_WORKER_CLEANUP_INTERVAL_SECONDS="2",
        MDDS_WORKER_PROGRESS_INTERVAL_SECONDS="3",
    )

    worker_config = config.load_config()

    assert worker_config.worker_job_queue_name == "custom.job.queue"
    assert worker_config.worker_cancel_queue_name == "custom.cancel.queue"
    assert worker_config.worker_status_queue_name == "custom.status.queue"
    assert worker_config.rabbitmq_port == 5673
    assert worker_config.object_storage_region == "eu-central-1"
    assert worker_config.object_storage_path_style_access_enabled is False
    assert worker_config.worker_job_timeout_seconds == 100
    assert worker_config.worker_cleanup_interval_seconds == 2
    assert worker_config.worker_progress_interval_seconds == 3


def test_load_config_generates_worker_id_when_missing(monkeypatch):
    set_required_worker_env(monkeypatch)
    monkeypatch.delenv("MDDS_WORKER_ID", raising=False)

    worker_config = config.load_config()

    assert worker_config.worker_id.startswith("worker-")
    assert worker_config.worker_cancel_queue_name.startswith("cancel.queue-worker-")


@pytest.mark.parametrize(
    "env_name",
    [
        "MDDS_WORKER_JOB_TYPE",
        "MDDS_RABBITMQ_HOST",
        "MDDS_RABBITMQ_USER",
        "MDDS_RABBITMQ_PASSWORD",
        "MDDS_OBJECT_STORAGE_INTERNAL_ENDPOINT",
        "MDDS_OBJECT_STORAGE_BUCKET",
        "MDDS_OBJECT_STORAGE_ACCESS_KEY",
        "MDDS_OBJECT_STORAGE_SECRET_KEY",
        "MDDS_WORKER_HANDLER",
    ],
)
def test_load_config_fails_when_required_env_is_missing(monkeypatch, env_name):
    set_required_worker_env(monkeypatch)
    monkeypatch.delenv(env_name, raising=False)

    with pytest.raises(config.WorkerConfigError) as error:
        config.load_config()

    assert f"Required environment variable '{env_name}' is missing." == str(error.value)


@pytest.mark.parametrize(
    "env_name",
    [
        "MDDS_WORKER_JOB_TYPE",
        "MDDS_RABBITMQ_HOST",
        "MDDS_RABBITMQ_USER",
        "MDDS_RABBITMQ_PASSWORD",
        "MDDS_OBJECT_STORAGE_INTERNAL_ENDPOINT",
        "MDDS_OBJECT_STORAGE_BUCKET",
        "MDDS_OBJECT_STORAGE_ACCESS_KEY",
        "MDDS_OBJECT_STORAGE_SECRET_KEY",
        "MDDS_WORKER_HANDLER",
    ],
)
def test_load_config_fails_when_required_env_is_blank(monkeypatch, env_name):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv(env_name, "   ")

    with pytest.raises(config.WorkerConfigError) as error:
        config.load_config()

    assert f"Required environment variable '{env_name}' is missing." == str(error.value)


@pytest.mark.parametrize(
    "env_name",
    [
        "MDDS_RABBITMQ_PORT",
        "MDDS_WORKER_JOB_TIMEOUT_SECONDS",
        "MDDS_WORKER_CLEANUP_INTERVAL_SECONDS",
        "MDDS_WORKER_PROGRESS_INTERVAL_SECONDS",
    ],
)
def test_load_config_fails_when_integer_env_is_invalid(monkeypatch, env_name):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv(env_name, "not-an-int")

    with pytest.raises(config.WorkerConfigError) as error:
        config.load_config()

    assert f"Environment variable '{env_name}' must be an integer" in str(error.value)


@pytest.mark.parametrize(
    "env_name",
    [
        "MDDS_WORKER_JOB_TIMEOUT_SECONDS",
        "MDDS_WORKER_CLEANUP_INTERVAL_SECONDS",
        "MDDS_WORKER_PROGRESS_INTERVAL_SECONDS",
    ],
)
@pytest.mark.parametrize("value", ["0", "-1"])
def test_load_config_fails_when_positive_interval_is_not_positive(
    monkeypatch, env_name, value
):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv(env_name, value)

    with pytest.raises(config.WorkerConfigError):
        config.load_config()


@pytest.mark.parametrize("value", ["0", "-1", "65536"])
def test_load_config_fails_when_rabbitmq_port_is_out_of_range(monkeypatch, value):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv("MDDS_RABBITMQ_PORT", value)

    with pytest.raises(config.WorkerConfigError) as error:
        config.load_config()

    assert "Illegal RabbitMQ port" in str(error.value)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("true", True),
        ("1", True),
        ("yes", True),
        ("y", True),
        ("on", True),
        ("false", False),
        ("0", False),
        ("no", False),
        ("n", False),
        ("off", False),
    ],
)
def test_load_config_parses_path_style_access_bool(monkeypatch, raw, expected):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv("MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED", raw)

    worker_config = config.load_config()

    assert worker_config.object_storage_path_style_access_enabled is expected


def test_load_config_fails_when_bool_env_is_invalid(monkeypatch):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv("MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED", "maybe")

    with pytest.raises(config.WorkerConfigError) as error:
        config.load_config()

    assert (
        "Environment variable 'MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED' "
        "must be boolean"
    ) in str(error.value)


def valid_config() -> config.WorkerConfig:
    return config.WorkerConfig(
        worker_id="worker-1",
        worker_job_type="solving_slae",
        worker_job_queue_name="queue-solving_slae",
        worker_cancel_queue_name="cancel.queue-worker-1",
        worker_status_queue_name="mdds_status_queue",
        rabbitmq_host="rabbitmq",
        rabbitmq_port=5672,
        rabbitmq_user="mdds",
        rabbitmq_password="secret",
        object_storage_endpoint_url="http://minio:9000",
        object_storage_bucket="mdds",
        object_storage_access_key="minioadmin",
        object_storage_secret_key="minioadmin",
        object_storage_region="us-east-1",
        object_storage_path_style_access_enabled=True,
        worker_handler="mdds_slae_worker.handler:SlaeJobHandler",
        worker_job_timeout_seconds=3600,
        worker_cleanup_interval_seconds=1,
        worker_progress_interval_seconds=5,
        worker_local_root=Path("/opt/mdds"),
    )


def test_validate_config_accepts_valid_config():
    config.validate_config(valid_config())


@pytest.mark.parametrize(
    "field_name",
    [
        "worker_id",
        "worker_job_type",
        "worker_job_queue_name",
        "worker_cancel_queue_name",
        "worker_status_queue_name",
        "rabbitmq_host",
        "rabbitmq_user",
        "rabbitmq_password",
        "object_storage_endpoint_url",
        "object_storage_bucket",
        "object_storage_access_key",
        "object_storage_secret_key",
        "object_storage_region",
        "worker_handler",
    ],
)
def test_validate_config_rejects_blank_string_fields(field_name):
    worker_config = valid_config()
    broken_config = replace(worker_config, **{field_name: "   "})

    with pytest.raises(config.WorkerConfigError) as error:
        config.validate_config(broken_config)

    assert f"Worker config field '{field_name}' must not be null or blank." == str(
        error.value
    )


def test_validate_config_rejects_none():
    with pytest.raises(config.WorkerConfigError) as error:
        config.validate_config(None)

    assert str(error.value) == "Worker config must not be null."


def test_load_config_uses_default_worker_local_root(monkeypatch):
    set_required_worker_env(monkeypatch)
    worker_config = config.load_config()

    assert worker_config.worker_local_root == Path("/opt/mdds")
    assert worker_config.jobs_root == Path("/opt/mdds/jobs")


def test_load_config_reads_worker_local_root(monkeypatch):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv("MDDS_WORKER_LOCAL_ROOT", "/var/lib/mdds-worker")

    worker_config = config.load_config()

    assert worker_config.worker_local_root == Path("/var/lib/mdds-worker")
    assert worker_config.jobs_root == Path("/var/lib/mdds-worker/jobs")


def test_load_config_rejects_relative_worker_local_root(monkeypatch):
    set_required_worker_env(monkeypatch)
    monkeypatch.setenv("MDDS_WORKER_LOCAL_ROOT", "relative/path")

    with pytest.raises(
        config.WorkerConfigError,
        match="MDDS_WORKER_LOCAL_ROOT.*absolute path",
    ):
        config.load_config()


def test_validate_config_rejects_filesystem_root_as_worker_local_root(monkeypatch):
    set_required_worker_env(monkeypatch)
    worker_config = config.load_config()

    invalid_config = replace(worker_config, worker_local_root=Path("/"))

    with pytest.raises(config.WorkerConfigError, match="must not be filesystem root"):
        config.validate_config(invalid_config)


def test_validate_config_rejects_relative_worker_local_root() -> None:
    invalid_config = replace(
        valid_config(),
        worker_local_root=Path("relative/path"),
    )

    with pytest.raises(config.WorkerConfigError, match="absolute path"):
        config.validate_config(invalid_config)


def test_validate_config_rejects_null_worker_local_root() -> None:
    invalid_config = replace(valid_config(), worker_local_root=None)

    with pytest.raises(
        config.WorkerConfigError, match="worker_local_root.*must not be null"
    ):
        config.validate_config(invalid_config)
