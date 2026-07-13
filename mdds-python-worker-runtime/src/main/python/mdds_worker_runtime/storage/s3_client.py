# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class S3StorageReadinessError(RuntimeError):
    """Raised when S3-compatible storage is not ready for Worker Runtime startup."""


class S3Storage:
    """Class to handle S3 storage."""

    def __init__(self, client, bucket: str):
        if client is None:
            raise ValueError("client cannot be null.")
        if bucket is None or bucket.strip() == "":
            raise ValueError("bucket cannot be null or blank.")
        self._client = client
        self._bucket = bucket.strip()

    def check_readiness(self) -> None:
        """Check that configured S3-compatible bucket is reachable and accessible."""
        logger.info(
            "Checking S3-compatible storage bucket readiness.",
            extra={
                "component": "s3_storage",
                "event": "s3_bucket_readiness_check_started",
                "bucket": self._bucket,
            },
        )

        try:
            self._client.head_bucket(Bucket=self._bucket)
        except Exception as exc:
            logger.exception(
                "S3-compatible storage bucket readiness check failed.",
                extra={
                    "component": "s3_storage",
                    "event": "s3_bucket_readiness_check_failed",
                    "bucket": self._bucket,
                },
            )
            raise S3StorageReadinessError(
                "S3-compatible storage bucket is not ready: "
                f"bucket='{self._bucket}'."
            ) from exc

        logger.info(
            "S3-compatible storage bucket readiness check completed.",
            extra={
                "component": "s3_storage",
                "event": "s3_bucket_readiness_check_completed",
                "bucket": self._bucket,
            },
        )

    def get_json(self, key: str) -> Any:
        """Gets JSON from S3 storage."""
        if key is None or key.strip() == "":
            raise ValueError(_KEY_CANNOT_BE_NULL_OR_BLANK)
        logger.debug(
            "Loading JSON object from S3-compatible storage.",
            extra={
                "component": "s3_storage",
                "event": "s3_json_load_started",
                "bucket": self._bucket,
                "objectKey": key,
            },
        )
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        result = json.loads(content)

        logger.debug(
            "JSON object loaded from S3-compatible storage.",
            extra={
                "component": "s3_storage",
                "event": "s3_json_load_completed",
                "bucket": self._bucket,
                "objectKey": key,
            },
        )

        return result

    def download_file(self, key: str, destination: Path) -> None:
        """Download object from S3-compatible storage to a local file."""
        if key is None or key.strip() == "":
            raise ValueError(_KEY_CANNOT_BE_NULL_OR_BLANK)
        if destination is None:
            raise ValueError("destination cannot be null.")

        destination.parent.mkdir(parents=True, exist_ok=True)

        logger.debug(
            "Downloading object from S3-compatible storage.",
            extra={
                "component": "s3_storage",
                "event": "s3_file_download_started",
                "bucket": self._bucket,
                "objectKey": key,
                "destination": str(destination),
            },
        )

        self._client.download_file(
            Bucket=self._bucket,
            Key=key,
            Filename=str(destination),
        )

        logger.debug(
            "Object downloaded from S3-compatible storage.",
            extra={
                "component": "s3_storage",
                "event": "s3_file_download_completed",
                "bucket": self._bucket,
                "objectKey": key,
                "destination": str(destination),
            },
        )

    def upload_file(self, key: str, local_path: Path) -> None:
        """Upload local file to S3-compatible storage."""
        if key is None or key.strip() == "":
            raise ValueError(_KEY_CANNOT_BE_NULL_OR_BLANK)
        if local_path is None:
            raise ValueError("local_path cannot be null.")

        logger.debug(
            "Uploading object to S3-compatible storage.",
            extra={
                "component": "s3_storage",
                "event": "s3_file_upload_started",
                "bucket": self._bucket,
                "objectKey": key,
                "localPath": str(local_path),
            },
        )

        self._client.upload_file(
            str(local_path),
            self._bucket,
            key,
        )

        logger.debug(
            "Object uploaded to S3-compatible storage.",
            extra={
                "component": "s3_storage",
                "event": "s3_file_upload_completed",
                "bucket": self._bucket,
                "objectKey": key,
                "localPath": str(local_path),
            },
        )


_KEY_CANNOT_BE_NULL_OR_BLANK = "key cannot be null or blank."
