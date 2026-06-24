# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import patch

import pytest
from botocore.config import Config

from mdds_worker_runtime.storage.s3_factory import (
    Boto3S3ClientFactory,
    S3Properties,
)


def _s3_properties(path_style_access_enabled: bool = True) -> S3Properties:
    return S3Properties(
        endpoint_url="http://minio:9000",
        bucket="mdds",
        access_key="access-key",
        secret_key="secret-key",
        region="us-east-1",
        path_style_access_enabled=path_style_access_enabled,
    )


@pytest.mark.parametrize(
    ("path_style_access_enabled", "expected_addressing_style"),
    [
        (True, "path"),
        (False, "auto"),
    ],
)
def test_boto3_s3_client_factory_creates_configured_client(
    path_style_access_enabled: bool,
    expected_addressing_style: str,
) -> None:
    properties = _s3_properties(
        path_style_access_enabled=path_style_access_enabled,
    )
    expected_client = object()

    with patch("mdds_worker_runtime.storage.s3_factory.boto3.client") as boto3_client:
        boto3_client.return_value = expected_client

        actual_client = Boto3S3ClientFactory(properties).create()

    assert actual_client is expected_client

    boto3_client.assert_called_once()
    args, kwargs = boto3_client.call_args

    assert args == ("s3",)
    assert kwargs["endpoint_url"] == "http://minio:9000"
    assert kwargs["aws_access_key_id"] == "access-key"
    assert kwargs["aws_secret_access_key"] == "secret-key"
    assert kwargs["region_name"] == "us-east-1"

    config = kwargs["config"]
    assert isinstance(config, Config)
    assert config.s3["addressing_style"] == expected_addressing_style


def test_boto3_s3_client_factory_rejects_null_properties() -> None:
    with pytest.raises(ValueError, match="s3_properties cannot be null"):
        Boto3S3ClientFactory(None)


def test_s3_properties_are_immutable() -> None:
    properties = _s3_properties()

    with pytest.raises(FrozenInstanceError):
        setattr(properties, "bucket", "another-bucket")
