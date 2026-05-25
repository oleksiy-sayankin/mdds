# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import json
from typing import Any


class S3Storage:
    """Class to handle S3 storage."""

    def __init__(self, client, bucket: str):
        self._client = client
        self._bucket = bucket

    def get_json(self, key: str) -> Any:
        """Gets JSON from S3 storage"""
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
