# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from dataclasses import dataclass, fields
from typing import get_type_hints
import os
import socket
import uuid


class WorkerConfigError(RuntimeError):
    """Invalid worker configuration."""


@dataclass(frozen=True)
class WorkerConfig:
    """Configuration for worker."""

    worker_id: str
    worker_job_type: str
    worker_job_queue_name: str
    worker_cancel_queue_name: str
    worker_status_queue_name: str

    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_user: str
    rabbitmq_password: str

    object_storage_endpoint_url: str
    object_storage_bucket: str
    object_storage_access_key: str
    object_storage_secret_key: str
    object_storage_region: str
    object_storage_path_style_access_enabled: bool

    worker_handler: str
    worker_job_timeout_seconds: int
    worker_cleanup_interval_seconds: int
    worker_progress_interval_seconds: int


def load_config() -> WorkerConfig:
    """Read Worker configuration from environment variables."""
    worker_id = _env_str("MDDS_WORKER_ID", default_factory=_generate_worker_id)
    worker_job_type = _env_str("MDDS_WORKER_JOB_TYPE")

    config = WorkerConfig(
        worker_id=worker_id,
        worker_job_type=worker_job_type,
        worker_job_queue_name=_env_str(
            "MDDS_WORKER_JOB_QUEUE_NAME",
            default=f"queue-{worker_job_type}",
        ),
        worker_cancel_queue_name=_env_str(
            "MDDS_WORKER_CANCEL_QUEUE_NAME",
            default=f"cancel.queue-{worker_id}",
        ),
        worker_status_queue_name=_env_str(
            "MDDS_WORKER_STATUS_QUEUE_NAME",
            default="mdds_status_queue",
        ),
        rabbitmq_host=_env_str("MDDS_RABBITMQ_HOST"),
        rabbitmq_port=_env_int("MDDS_RABBITMQ_PORT", default=5672),
        rabbitmq_user=_env_str("MDDS_RABBITMQ_USER"),
        rabbitmq_password=_env_str("MDDS_RABBITMQ_PASSWORD"),
        object_storage_endpoint_url=_env_str("MDDS_OBJECT_STORAGE_INTERNAL_ENDPOINT"),
        object_storage_bucket=_env_str("MDDS_OBJECT_STORAGE_BUCKET"),
        object_storage_access_key=_env_str("MDDS_OBJECT_STORAGE_ACCESS_KEY"),
        object_storage_secret_key=_env_str("MDDS_OBJECT_STORAGE_SECRET_KEY"),
        object_storage_region=_env_str(
            "MDDS_OBJECT_STORAGE_REGION", default="us-east-1"
        ),
        object_storage_path_style_access_enabled=_env_bool(
            "MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED",
            default=True,
        ),
        worker_handler=_env_str("MDDS_WORKER_HANDLER"),
        worker_job_timeout_seconds=_env_int(
            "MDDS_WORKER_JOB_TIMEOUT_SECONDS",
            default=3600,
        ),
        worker_cleanup_interval_seconds=_env_int(
            "MDDS_WORKER_CLEANUP_INTERVAL_SECONDS",
            default=1,
        ),
        worker_progress_interval_seconds=_env_int(
            "MDDS_WORKER_PROGRESS_INTERVAL_SECONDS",
            default=5,
        ),
    )

    validate_config(config)
    return config


def validate_config(config: WorkerConfig) -> None:
    """Validate fully resolved Worker configuration."""
    if config is None:
        raise WorkerConfigError("Worker config must not be null.")

    _validate_non_blank_string_fields(config)

    if not 1 <= config.rabbitmq_port <= 65535:
        raise WorkerConfigError(f"Illegal RabbitMQ port: {config.rabbitmq_port}.")

    if config.worker_job_timeout_seconds <= 0:
        raise WorkerConfigError(
            f"Illegal worker job timeout: {config.worker_job_timeout_seconds}."
        )

    if config.worker_cleanup_interval_seconds <= 0:
        raise WorkerConfigError(
            "Illegal worker cleanup interval timeout: "
            f"{config.worker_cleanup_interval_seconds}."
        )

    if config.worker_progress_interval_seconds <= 0:
        raise WorkerConfigError(
            "Illegal worker progress interval timeout: "
            f"{config.worker_progress_interval_seconds}."
        )


def _validate_non_blank_string_fields(config: WorkerConfig) -> None:
    type_hints = get_type_hints(WorkerConfig)

    for field in fields(config):
        if type_hints.get(field.name) is str:
            value = getattr(config, field.name)
            if value is None or value.strip() == "":
                raise WorkerConfigError(
                    f"Worker config field '{field.name}' must not be null or blank."
                )


def _env_str(
    name: str,
    default: str | None = None,
    default_factory=None,
) -> str:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        if default is not None:
            return default
        if default_factory is not None:
            return default_factory()
        raise WorkerConfigError(f"Required environment variable '{name}' is missing.")

    return value.strip()


def _env_int(name: str, default: int | None = None) -> int:
    raw = _env_str(name, default=None if default is None else str(default))

    try:
        return int(raw)
    except ValueError as exc:
        raise WorkerConfigError(
            f"Environment variable '{name}' must be an integer, got '{raw}'."
        ) from exc


def _env_bool(name: str, default: bool | None = None) -> bool:
    raw_default = None if default is None else str(default).lower()
    raw = _env_str(name, default=raw_default).lower()

    if raw in {"true", "1", "yes", "y", "on"}:
        return True

    if raw in {"false", "0", "no", "n", "off"}:
        return False

    raise WorkerConfigError(
        f"Environment variable '{name}' must be boolean, got '{raw}'."
    )


def _generate_worker_id() -> str:
    hostname = socket.gethostname()
    return f"worker-{hostname}-{uuid.uuid4()}"
