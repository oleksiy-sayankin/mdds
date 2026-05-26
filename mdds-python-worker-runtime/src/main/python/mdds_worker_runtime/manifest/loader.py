# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import json
import logging

from botocore.exceptions import ClientError
from pydantic import ValidationError

from mdds_worker_runtime.domain.artifact_format import UnknownArtifactFormatError
from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.manifest.errors import (
    ManifestDomainMappingError,
    ManifestLoadingError,
    ManifestSchemaValidationError,
)
from mdds_worker_runtime.manifest.mapper import to_domain
from mdds_worker_runtime.manifest.models import JobManifestDTO
from mdds_worker_runtime.storage.s3_client import S3Storage

logger = logging.getLogger(__name__)


class ManifestLoader:
    """Loads a raw JSON manifest from storage and converts it to DTO/domain models."""

    def __init__(self, storage: S3Storage):
        self._storage = storage

    def load_dto(self, key: str) -> JobManifestDTO:
        """Returns JobManifestDTO object from raw JSON file."""
        logger.info(
            "Loading manifest object from storage.",
            extra={
                "component": "manifest_loader",
                "event": "manifest_load_started",
                "manifestObjectKey": key,
            },
        )
        try:
            raw_manifest = self._storage.get_json(key)
        except ClientError as error:
            raise self._to_manifest_loading_error(key, error) from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ManifestLoadingError(
                key=key,
                message=f"Manifest object '{key}' contains invalid JSON.",
            ) from error

        logger.info(
            "Manifest object loaded from storage.",
            extra={
                "component": "manifest_loader",
                "event": "manifest_loaded",
                "manifestObjectKey": key,
            },
        )

        logger.info(
            "Validating manifest schema.",
            extra={
                "component": "manifest_loader",
                "event": "manifest_schema_validation_started",
                "manifestObjectKey": key,
            },
        )

        try:
            dto = JobManifestDTO.model_validate(raw_manifest)
        except ValidationError as error:
            raise ManifestSchemaValidationError(
                key=key,
                message=f"Manifest object '{key}' does not match manifest schema.",
            ) from error
        logger.info(
            "Manifest schema validation completed.",
            extra={
                "component": "manifest_loader",
                "event": "manifest_schema_validation_completed",
                "manifestObjectKey": key,
                "jobId": dto.job_id,
                "userId": dto.user_id,
                "jobType": dto.job_type,
            },
        )
        return dto

    def load(self, key: str) -> JobManifest:
        """Returns JobManifest object from DTO object."""
        dto = self.load_dto(key)

        logger.info(
            "Converting manifest DTO to domain model.",
            extra={
                "component": "manifest_loader",
                "event": "manifest_domain_mapping_started",
                "manifestObjectKey": key,
                "jobId": dto.job_id,
                "userId": dto.user_id,
                "jobType": dto.job_type,
            },
        )

        try:
            manifest = to_domain(dto)
        except UnknownArtifactFormatError as error:
            raise ManifestDomainMappingError(
                key=key,
                message=f"Manifest object '{key}' cannot be converted to domain model: {error}",
            ) from error

        logger.info(
            "Manifest domain mapping completed.",
            extra={
                "component": "manifest_loader",
                "event": "manifest_domain_mapping_completed",
                "manifestObjectKey": key,
                "jobId": manifest.job_id,
                "userId": manifest.user_id,
                "jobType": manifest.job_type,
            },
        )
        return manifest

    @staticmethod
    def _to_manifest_loading_error(
        key: str, error: ClientError
    ) -> ManifestLoadingError:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")

        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return ManifestLoadingError(
                key=key,
                message=f"Key '{key}' not found in storage.",
                error_code=error_code,
            )

        return ManifestLoadingError(
            key=key,
            message=f"Failed to load manifest object '{key}' from storage. Error code: {error_code}.",
            error_code=error_code,
        )
