# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class S3Storage:
    """Class to handle S3 storage."""

    def __init__(self, client, bucket: str):
        self._client = client
        self._bucket = bucket

    def get_json(self, key: str) -> Any:
        """Gets JSON from S3 storage."""
        if key is None or key.strip() == "":
            raise ValueError("key cannot be null or blank.")
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
            raise ValueError("key cannot be null or blank.")
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
