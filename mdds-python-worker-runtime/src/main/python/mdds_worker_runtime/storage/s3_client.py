# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class S3Storage:
    """Class to handle S3 storage."""

    def __init__(self, client, bucket: str):
        self._client = client
        self._bucket = bucket

    def get_json(self, key: str) -> Any:
        """Gets JSON from S3 storage."""
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
