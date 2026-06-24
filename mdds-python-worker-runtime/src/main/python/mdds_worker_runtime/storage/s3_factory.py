# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config


@dataclass(frozen=True)
class S3Properties:
    """S3 connection properties."""

    endpoint_url: str
    bucket: str
    access_key: str
    secret_key: str
    region: str
    path_style_access_enabled: bool


class Boto3S3ClientFactory:
    """Creates configured boto3 S3 clients."""

    def __init__(self, s3_properties: S3Properties) -> None:
        if s3_properties is None:
            raise ValueError("s3_properties cannot be null.")
        self._s3_properties = s3_properties

    def create(self) -> Any:
        """Create configured boto3 S3 client."""
        addressing_style = (
            "path" if self._s3_properties.path_style_access_enabled else "auto"
        )

        return boto3.client(
            "s3",
            endpoint_url=self._s3_properties.endpoint_url,
            aws_access_key_id=self._s3_properties.access_key,
            aws_secret_access_key=self._s3_properties.secret_key,
            region_name=self._s3_properties.region,
            config=Config(s3={"addressing_style": addressing_style}),
        )
