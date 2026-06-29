# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import json
from pathlib import Path
import queue
import time
from uuid import uuid4

import boto3
from botocore.config import Config
import pika
import pytest
from testcontainers.minio import MinioContainer
from testcontainers.rabbitmq import RabbitMqContainer

from mdds_worker_runtime import main as worker_main
from mdds_worker_runtime.dto.messages import JobMessageDTO, JobStatusUpdateDTO
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.queue.queue_client import QueueMessage
from mdds_worker_runtime.rabbitmq.rabbitmq_queue_client import (
    RabbitMqProperties,
    RabbitMqQueueClient,
)

RABBITMQ_IMAGE = "rabbitmq:3.12-management"

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
S3_BUCKET = "mdds-test"

WORKER_ID = "worker-two-numbers-sum-it"
JOB_TYPE = "two_numbers_sum"
USER_ID = 123

NUMBER_A_VALUE = "40\n"
NUMBER_B_VALUE = "2\n"
EXPECTED_SUM = "42"


class StatusCollectingHandler:
    def __init__(self) -> None:
        self.received: queue.Queue[JobStatusUpdateDTO] = queue.Queue()

    def handle(self, message, ack) -> None:
        self.received.put(message.payload)
        ack.ack()


@pytest.fixture(scope="session")
def rabbitmq_container():
    with RabbitMqContainer(
        RABBITMQ_IMAGE,
        username="guest",
        password="guest",
    ) as container:
        yield container


@pytest.fixture(scope="session")
def minio_container():
    with MinioContainer(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    ) as container:
        yield container


@pytest.fixture
def rabbitmq_properties(rabbitmq_container) -> RabbitMqProperties:
    connection_params = rabbitmq_container.get_connection_params()

    return RabbitMqProperties(
        host=connection_params.host,
        port=connection_params.port,
        user="guest",
        password="guest",
        connection_timeout_seconds=20.0,
    )


@pytest.fixture
def s3_client_and_endpoint(minio_container):
    config = minio_container.get_config()

    endpoint = (
        config.get("endpoint")
        if isinstance(config, dict)
        else getattr(config, "endpoint")
    )

    if not endpoint.startswith(("http://", "https://")):
        endpoint = "http://" + endpoint

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

    _create_bucket_if_absent(s3_client, S3_BUCKET)

    yield s3_client, endpoint


def test_worker_runtime_processes_two_numbers_sum_happy_path(
    rabbitmq_properties: RabbitMqProperties,
    s3_client_and_endpoint,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    s3_client, s3_endpoint = s3_client_and_endpoint

    test_id = str(uuid4())
    job_id = f"job-{test_id}"

    job_queue_name = f"test.job.two_numbers_sum.{test_id}"
    status_queue_name = f"test.status.two_numbers_sum.{test_id}"
    cancel_queue_name = f"test.cancel.two_numbers_sum.{test_id}"

    manifest_key = f"jobs/{USER_ID}/{job_id}/manifest.json"
    number_a_key = f"jobs/{USER_ID}/{job_id}/in/number_a.csv"
    number_b_key = f"jobs/{USER_ID}/{job_id}/in/number_b.csv"
    sum_key = f"jobs/{USER_ID}/{job_id}/out/sum.csv"

    job_workspace = tmp_path / "jobs" / str(USER_ID) / job_id

    _declare_queue(rabbitmq_properties, job_queue_name)
    _declare_queue(rabbitmq_properties, status_queue_name)
    _declare_queue(rabbitmq_properties, cancel_queue_name)

    _put_json(
        s3_client=s3_client,
        bucket=S3_BUCKET,
        key=manifest_key,
        value=_manifest(
            job_id=job_id,
            number_a_key=number_a_key,
            number_b_key=number_b_key,
            sum_key=sum_key,
        ),
    )
    _put_bytes(
        s3_client=s3_client,
        bucket=S3_BUCKET,
        key=number_a_key,
        body=NUMBER_A_VALUE.encode("utf-8"),
    )
    _put_bytes(
        s3_client=s3_client,
        bucket=S3_BUCKET,
        key=number_b_key,
        body=NUMBER_B_VALUE.encode("utf-8"),
    )

    _configure_worker_runtime_environment(
        monkeypatch=monkeypatch,
        rabbitmq_properties=rabbitmq_properties,
        s3_endpoint=s3_endpoint,
        jobs_root=tmp_path,
        job_queue_name=job_queue_name,
        status_queue_name=status_queue_name,
        cancel_queue_name=cancel_queue_name,
    )

    status_handler = StatusCollectingHandler()
    runtime = None

    with RabbitMqQueueClient(rabbitmq_properties, prefetch_count=1) as status_client:
        status_subscription = status_client.subscribe(
            status_queue_name,
            JobStatusUpdateDTO,
            status_handler,
        )

        try:
            runtime = worker_main.build_worker_runtime_from_environment()
            runtime.start()

            with RabbitMqQueueClient(rabbitmq_properties) as publisher_client:
                publisher_client.publish(
                    job_queue_name,
                    QueueMessage(
                        payload=JobMessageDTO(
                            manifestObjectKey=manifest_key,
                        ),
                    ),
                )

            done_status = _wait_for_done_status(status_handler, job_id)
            statuses = _drain_statuses(status_handler)

            assert _contains_status_before(
                statuses=statuses,
                earlier_status=WorkerJobStatus.IN_PROGRESS.value,
                later_status=WorkerJobStatus.DONE.value,
                job_id=job_id,
            )

            assert done_status.jobId == job_id
            assert done_status.job_id == job_id
            assert done_status.workerId == WORKER_ID
            assert done_status.worker_id == WORKER_ID
            assert done_status.status == WorkerJobStatus.DONE.value
            assert done_status.progress == 100

            output = s3_client.get_object(Bucket=S3_BUCKET, Key=sum_key)
            output_body = output["Body"].read().decode("utf-8").strip()

            assert output_body == EXPECTED_SUM

            _wait_until(lambda: not job_workspace.exists())

        finally:
            if runtime is not None:
                runtime.stop()

            status_subscription.close()

    _wait_for_ack_to_be_processed()

    assert _basic_get(rabbitmq_properties, job_queue_name) is None

    _delete_queue_if_exists(rabbitmq_properties, job_queue_name)
    _delete_queue_if_exists(rabbitmq_properties, status_queue_name)
    _delete_queue_if_exists(rabbitmq_properties, cancel_queue_name)


def _manifest(
    *,
    job_id: str,
    number_a_key: str,
    number_b_key: str,
    sum_key: str,
) -> dict:
    return {
        "manifestVersion": 1,
        "userId": USER_ID,
        "jobId": job_id,
        "jobType": JOB_TYPE,
        "inputs": {
            "number_a": {
                "objectKey": number_a_key,
                "format": "csv",
            },
            "number_b": {
                "objectKey": number_b_key,
                "format": "csv",
            },
        },
        "params": {},
        "outputs": {
            "sum": {
                "objectKey": sum_key,
                "format": "csv",
            },
        },
    }


def _configure_worker_runtime_environment(
    *,
    monkeypatch: pytest.MonkeyPatch,
    rabbitmq_properties: RabbitMqProperties,
    s3_endpoint: str,
    jobs_root: Path,
    job_queue_name: str,
    status_queue_name: str,
    cancel_queue_name: str,
) -> None:
    monkeypatch.setenv("MDDS_WORKER_ID", WORKER_ID)
    monkeypatch.setenv("MDDS_WORKER_JOB_TYPE", JOB_TYPE)
    monkeypatch.setenv("MDDS_WORKER_JOB_QUEUE_NAME", job_queue_name)
    monkeypatch.setenv("MDDS_WORKER_STATUS_QUEUE_NAME", status_queue_name)
    monkeypatch.setenv("MDDS_WORKER_CANCEL_QUEUE_NAME", cancel_queue_name)
    monkeypatch.setenv(
        "MDDS_WORKER_HANDLER",
        "tests.fixtures.job_handlers:TwoNumbersSumJobHandler",
    )

    monkeypatch.setenv("MDDS_RABBITMQ_HOST", rabbitmq_properties.host)
    monkeypatch.setenv("MDDS_RABBITMQ_PORT", str(rabbitmq_properties.port))
    monkeypatch.setenv("MDDS_RABBITMQ_USER", rabbitmq_properties.user)
    monkeypatch.setenv("MDDS_RABBITMQ_PASSWORD", rabbitmq_properties.password)

    monkeypatch.setenv("MDDS_OBJECT_STORAGE_BUCKET", S3_BUCKET)
    monkeypatch.setenv("MDDS_OBJECT_STORAGE_INTERNAL_ENDPOINT", s3_endpoint)
    monkeypatch.setenv("MDDS_OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("MDDS_OBJECT_STORAGE_ACCESS_KEY", MINIO_ACCESS_KEY)
    monkeypatch.setenv("MDDS_OBJECT_STORAGE_SECRET_KEY", MINIO_SECRET_KEY)
    monkeypatch.setenv("MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED", "true")

    monkeypatch.setenv("MDDS_WORKER_LOCAL_ROOT", str(jobs_root))
    monkeypatch.setenv("MDDS_WORKER_JOB_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv("MDDS_WORKER_PROGRESS_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("MDDS_WORKER_CLEANUP_INTERVAL_SECONDS", "1")


def _wait_for_done_status(
    status_handler: StatusCollectingHandler,
    job_id: str,
) -> JobStatusUpdateDTO:
    deadline = time.monotonic() + 30.0

    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())

        try:
            status = status_handler.received.get(timeout=min(1.0, remaining))
        except queue.Empty:
            continue

        status_handler.received.put(status)

        if status.jobId == job_id and status.status == WorkerJobStatus.DONE.value:
            return status

    raise AssertionError(f"DONE status was not published for jobId='{job_id}'.")


def _drain_statuses(
    status_handler: StatusCollectingHandler,
) -> list[JobStatusUpdateDTO]:
    statuses: list[JobStatusUpdateDTO] = []

    while True:
        try:
            statuses.append(status_handler.received.get_nowait())
        except queue.Empty:
            return statuses


def _contains_status_before(
    *,
    statuses: list[JobStatusUpdateDTO],
    earlier_status: str,
    later_status: str,
    job_id: str,
) -> bool:
    earlier_index = _index_of_status(statuses, earlier_status, job_id)
    later_index = _index_of_status(statuses, later_status, job_id)

    return (
        earlier_index is not None
        and later_index is not None
        and earlier_index < later_index
    )


def _index_of_status(
    statuses: list[JobStatusUpdateDTO],
    status: str,
    job_id: str,
) -> int | None:
    for index, status_update in enumerate(statuses):
        if status_update.jobId == job_id and status_update.status == status:
            return index

    return None


def _put_json(
    *,
    s3_client,
    bucket: str,
    key: str,
    value: object,
) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(value).encode("utf-8"),
        ContentType="application/json",
    )


def _put_bytes(
    *,
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/csv",
    )


def _create_bucket_if_absent(s3_client, bucket: str) -> None:
    existing_buckets = s3_client.list_buckets()["Buckets"]
    existing_bucket_names = {bucket_info["Name"] for bucket_info in existing_buckets}

    if bucket not in existing_bucket_names:
        s3_client.create_bucket(Bucket=bucket)


def _wait_until(
    condition,
    *,
    timeout_seconds: float = 15.0,
    interval_seconds: float = 0.2,
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if condition():
            return

        time.sleep(interval_seconds)

    raise AssertionError("Condition was not satisfied before timeout.")


def _wait_for_ack_to_be_processed() -> None:
    # RabbitMqAcknowledger schedules ack/nack via connection.add_callback_threadsafe(...).
    # Give the consumer thread one more process_data_events(...) cycle before checking redelivery.
    time.sleep(0.5)


def _declare_queue(properties: RabbitMqProperties, queue_name: str) -> None:
    connection = pika.BlockingConnection(_connection_parameters(properties))
    try:
        channel = connection.channel()
        channel.queue_declare(
            queue=queue_name,
            durable=False,
            exclusive=False,
            auto_delete=False,
        )
    finally:
        connection.close()


def _basic_get(
    properties: RabbitMqProperties,
    queue_name: str,
):
    connection = pika.BlockingConnection(_connection_parameters(properties))
    try:
        channel = connection.channel()
        method_frame, _header_frame, body = channel.basic_get(
            queue=queue_name,
            auto_ack=True,
        )
        return None if method_frame is None else body
    finally:
        connection.close()


def _delete_queue_if_exists(
    properties: RabbitMqProperties,
    queue_name: str,
) -> None:
    connection = pika.BlockingConnection(_connection_parameters(properties))
    try:
        channel = connection.channel()
        channel.queue_delete(queue=queue_name)
    finally:
        connection.close()


def _connection_parameters(properties: RabbitMqProperties) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=properties.host,
        port=properties.port,
        credentials=pika.PlainCredentials(properties.user, properties.password),
        heartbeat=60,
        blocked_connection_timeout=properties.connection_timeout_seconds,
        connection_attempts=1,
    )
