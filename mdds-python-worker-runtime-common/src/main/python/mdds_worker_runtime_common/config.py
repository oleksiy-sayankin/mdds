# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
import os
from dataclasses import dataclass, fields
from typing import get_type_hints

logger = logging.getLogger(__name__)


class WorkerConfigError(RuntimeError):
    """Invalid worker configuration."""


@dataclass(frozen=True)
class WorkerConfig:
    worker_name: str
    worker_version: str
    worker_handler: str
    argo_retry_index: int


def load_config() -> WorkerConfig:
    """Read Worker configuration from environment variables."""
    logger.info("Loading Worker configuration from environment variables...")
    config = WorkerConfig(
        worker_name=_env_str("MDDS_WORKER_NAME"),
        worker_version=_env_str("MDDS_WORKER_VERSION"),
        worker_handler=_env_str("MDDS_WORKER_HANDLER"),
        argo_retry_index=_env_int("MDDS_ARGO_RETRY_INDEX"),
    )
    logger.info("Validating Worker configuration...")
    validate_config(config)
    return config


def validate_config(config: WorkerConfig) -> None:
    """Validate fully resolved Worker configuration."""
    if config is None:
        raise WorkerConfigError("Worker config must not be null.")

    _validate_non_blank_string_fields(config)

    if config.argo_retry_index < 0:
        raise WorkerConfigError(f"Illegal argo retry index: {config.argo_retry_index}.")


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
) -> str:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        if default is not None:
            return default

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
