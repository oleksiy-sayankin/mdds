# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.storage.s3_client import S3Storage, S3StorageReadinessError


def test_get_json_loads_json_object_from_s3() -> None:
    client = MagicMock()
    client.get_object.return_value = {"Body": BytesIO(b"""
            {
              "manifestVersion": 1,
              "userId": 42,
              "jobId": "job-1",
              "jobType": "solving_slae"
            }
            """)}
    storage = S3Storage(client, "mdds")

    result = storage.get_json("jobs/42/job-1/manifest.json")

    assert result == {
        "manifestVersion": 1,
        "userId": 42,
        "jobId": "job-1",
        "jobType": "solving_slae",
    }
    client.get_object.assert_called_once_with(
        Bucket="mdds",
        Key="jobs/42/job-1/manifest.json",
    )


def test_get_json_loads_nested_json_object_from_s3() -> None:
    client = MagicMock()
    client.get_object.return_value = {"Body": BytesIO(b"""
            {
              "manifestVersion": 1,
              "userId": 42,
              "jobId": "job-1",
              "jobType": "solving_slae",
              "inputs": {
                "matrix": {
                  "objectKey": "jobs/42/job-1/in/matrix.csv",
                  "format": "csv"
                },
                "rhs": {
                  "objectKey": "jobs/42/job-1/in/rhs.csv",
                  "format": "csv"
                }
              },
              "params": {
                "solvingMethod": "numpy_exact_solver"
              },
              "outputs": {
                "solution": {
                  "objectKey": "jobs/42/job-1/out/solution.csv",
                  "format": "csv"
                }
              }
            }
            """)}
    storage = S3Storage(client, "mdds")

    result = storage.get_json("jobs/42/job-1/manifest.json")

    assert result["manifestVersion"] == 1
    assert result["userId"] == 42
    assert result["jobId"] == "job-1"
    assert result["jobType"] == "solving_slae"
    assert result["inputs"]["matrix"]["objectKey"] == "jobs/42/job-1/in/matrix.csv"
    assert result["inputs"]["rhs"]["objectKey"] == "jobs/42/job-1/in/rhs.csv"
    assert result["params"]["solvingMethod"] == "numpy_exact_solver"
    assert (
        result["outputs"]["solution"]["objectKey"] == "jobs/42/job-1/out/solution.csv"
    )
    client.get_object.assert_called_once_with(
        Bucket="mdds",
        Key="jobs/42/job-1/manifest.json",
    )


@pytest.mark.parametrize("key", [None, "", "   "])
def test_get_json_rejects_null_or_blank_key(key: str | None) -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    with pytest.raises(ValueError, match="key cannot be null or blank"):
        storage.get_json(key)

    client.get_object.assert_not_called()


def test_download_file_downloads_object_to_destination(tmp_path: Path) -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    destination = tmp_path / "jobs" / "42" / "job-1" / "in" / "matrix.csv"

    storage.download_file("jobs/42/job-1/in/matrix.csv", destination)

    assert destination.parent.is_dir()
    client.download_file.assert_called_once_with(
        Bucket="mdds",
        Key="jobs/42/job-1/in/matrix.csv",
        Filename=str(destination),
    )


@pytest.mark.parametrize("key", [None, "", "   "])
def test_download_file_rejects_null_or_blank_key(
    tmp_path: Path,
    key: str | None,
) -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")
    destination = tmp_path / "matrix.csv"

    with pytest.raises(ValueError, match="key cannot be null or blank"):
        storage.download_file(key, destination)

    client.download_file.assert_not_called()


def test_download_file_rejects_null_destination() -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    with pytest.raises(ValueError, match="destination cannot be null"):
        storage.download_file("jobs/42/job-1/in/matrix.csv", None)

    client.download_file.assert_not_called()


def test_s3_storage_upload_file_uploads_local_file_to_s3() -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    local_path = Path("/tmp/solution.csv")

    storage.upload_file("jobs/42/job-1/out/solution.csv", local_path)

    client.upload_file.assert_called_once_with(
        str(local_path),
        "mdds",
        "jobs/42/job-1/out/solution.csv",
    )


@pytest.mark.parametrize("key", [None, "", " "])
def test_s3_storage_upload_file_rejects_null_or_blank_key(key) -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    with pytest.raises(ValueError, match="key cannot be null or blank."):
        storage.upload_file(key, Path("/tmp/solution.csv"))

    client.upload_file.assert_not_called()


def test_s3_storage_upload_file_rejects_null_local_path() -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    with pytest.raises(ValueError, match="local_path cannot be null."):
        storage.upload_file("jobs/42/job-1/out/solution.csv", None)

    client.upload_file.assert_not_called()


def test_s3_storage_rejects_null_client() -> None:
    with pytest.raises(ValueError, match="client cannot be null."):
        S3Storage(None, "mdds")


@pytest.mark.parametrize("bucket", [None, "", "   "])
def test_s3_storage_rejects_null_or_blank_bucket(bucket: str | None) -> None:
    with pytest.raises(ValueError, match="bucket cannot be null or blank."):
        S3Storage(MagicMock(), bucket)


def test_s3_storage_trims_bucket_before_using_it() -> None:
    client = MagicMock()
    storage = S3Storage(client, "  mdds  ")

    storage.check_readiness()

    client.head_bucket.assert_called_once_with(Bucket="mdds")


def test_s3_storage_check_readiness_calls_head_bucket() -> None:
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    storage.check_readiness()

    client.head_bucket.assert_called_once_with(Bucket="mdds")


def test_s3_storage_check_readiness_returns_normally_when_head_bucket_succeeds() -> (
    None
):
    client = MagicMock()
    storage = S3Storage(client, "mdds")

    storage.check_readiness()

    client.head_bucket.assert_called_once_with(Bucket="mdds")


def test_s3_storage_check_readiness_logs_started_and_completed_events_on_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(
        logging.INFO,
        logger="mdds_worker_runtime.storage.s3_client",
    )

    client = MagicMock()
    storage = S3Storage(client, "mdds")

    storage.check_readiness()

    assert any(
        record.message == "Checking S3-compatible storage bucket readiness."
        and getattr(record, "component", None) == "s3_storage"
        and getattr(record, "event", None) == "s3_bucket_readiness_check_started"
        and getattr(record, "bucket", None) == "mdds"
        for record in caplog.records
    )
    assert any(
        record.message == "S3-compatible storage bucket readiness check completed."
        and getattr(record, "component", None) == "s3_storage"
        and getattr(record, "event", None) == "s3_bucket_readiness_check_completed"
        and getattr(record, "bucket", None) == "mdds"
        for record in caplog.records
    )


def test_s3_storage_check_readiness_raises_readiness_error_when_head_bucket_fails() -> (
    None
):
    original_error = RuntimeError("head bucket failed")

    client = MagicMock()
    client.head_bucket.side_effect = original_error

    storage = S3Storage(client, "mdds")

    with pytest.raises(
        S3StorageReadinessError,
        match="S3-compatible storage bucket is not ready: bucket='mdds'.",
    ):
        storage.check_readiness()

    client.head_bucket.assert_called_once_with(Bucket="mdds")


def test_s3_storage_check_readiness_preserves_original_exception_as_cause() -> None:
    original_error = RuntimeError("head bucket failed")

    client = MagicMock()
    client.head_bucket.side_effect = original_error

    storage = S3Storage(client, "mdds")

    with pytest.raises(S3StorageReadinessError) as error:
        storage.check_readiness()

    assert error.value.__cause__ is original_error


def test_s3_storage_check_readiness_logs_failed_event_with_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(
        logging.INFO,
        logger="mdds_worker_runtime.storage.s3_client",
    )

    client = MagicMock()
    client.head_bucket.side_effect = RuntimeError("head bucket failed")

    storage = S3Storage(client, "mdds")

    with pytest.raises(S3StorageReadinessError):
        storage.check_readiness()

    assert any(
        record.message == "S3-compatible storage bucket readiness check failed."
        and record.exc_info is not None
        and getattr(record, "component", None) == "s3_storage"
        and getattr(record, "event", None) == "s3_bucket_readiness_check_failed"
        and getattr(record, "bucket", None) == "mdds"
        for record in caplog.records
    )
