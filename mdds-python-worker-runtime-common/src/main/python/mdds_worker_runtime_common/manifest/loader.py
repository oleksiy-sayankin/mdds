# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import logging
from pathlib import Path

from pydantic import ValidationError

from mdds_worker_runtime_common.domain.manifest import WorkerManifest
from mdds_worker_runtime_common.manifest.mapper import (
    WorkerManifestMappingError,
    to_domain,
)
from mdds_worker_runtime_common.manifest.models import WorkerManifestDTO

logger = logging.getLogger(__name__)

CANONICAL_WORKER_MANIFEST_PATH = Path("/opt/mdds/config/worker-manifest.json")


class WorkerManifestLoadError(RuntimeError):
    """Worker Manifest cannot be read, validated, or mapped."""


def load_worker_manifest(
    path: str | Path = CANONICAL_WORKER_MANIFEST_PATH,
) -> WorkerManifest:
    """Load, validate, and map a Worker Manifest to the domain model."""
    manifest_path = Path(path)
    logger.info(
        "Loading Worker Manifest from '%s'...",
        manifest_path,
    )

    try:
        raw_manifest = manifest_path.read_bytes()
    except OSError as exc:
        raise WorkerManifestLoadError(
            f"Cannot read Worker Manifest from '{manifest_path}'."
        ) from exc

    try:
        dto = WorkerManifestDTO.model_validate_json(raw_manifest)
    except ValidationError as exc:
        raise WorkerManifestLoadError(
            _validation_error_message(manifest_path, exc)
        ) from exc

    try:
        manifest = to_domain(dto)
    except WorkerManifestMappingError as exc:
        raise WorkerManifestLoadError(
            f"Worker Manifest at '{manifest_path}' violates the domain "
            f"contract: {exc}"
        ) from exc

    logger.info(
        "Worker Manifest loaded.",
        extra={
            "userId": manifest.execution.user_id,
            "dagRunId": manifest.execution.dag_run_id,
            "nodeId": manifest.execution.node_id,
            "component": "manifest-loader",
        },
    )
    return manifest


def _validation_error_message(
    path: Path,
    error: ValidationError,
) -> str:
    problems = []
    for item in error.errors():
        location = ".".join(str(part) for part in item["loc"]) or "<root>"
        problems.append(f"{location}: {item['msg']}")

    details = "; ".join(problems)
    return f"Invalid Worker Manifest at '{path}': {details}"
