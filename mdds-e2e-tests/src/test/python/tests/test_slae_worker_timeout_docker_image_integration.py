# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Timeout e2e tests for SLAE Python Worker Docker image."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import queue
import threading
import time
from typing import Any
import urllib.error
import urllib.request
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pika
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

_RABBITMQ_IMAGE = "rabbitmq:3.13-management"
_SLAE_WORKER_IMAGE = "mddsproject/python-worker-solving-slae:0.1.0"

_RABBITMQ_NETWORK_ALIAS = "rabbitmq"
_RABBITMQ_MANAGEMENT_PORT = 15672
_MINIO_NETWORK_ALIAS = "minio"

_RABBITMQ_INTERNAL_PORT = 5672
_RABBITMQ_USER = "guest"
_RABBITMQ_PASSWORD = "guest"

_S3_INTERNAL_ENDPOINT = f"http://{_MINIO_NETWORK_ALIAS}:9000"
_S3_BUCKET = "mdds"
_S3_REGION = "us-east-1"
_S3_ACCESS_KEY = "minioadmin"
_S3_SECRET_KEY = "minioadmin"

_WORKER_HEALTH_PORT = 12457
_WORKER_ID = "slae-worker-timeout-local"
_HANGING_JOB_HANDLER = (
    "mdds_python_worker_solving_slae.testing_handlers:HangingJobHandler"
)
_WORKER_JOB_TIMEOUT_SECONDS = "1"

_USER_ID = 123
_JOB_TYPE = "solving_slae"

_MATRIX_INPUT_SLOT = "matrix"
_RHS_INPUT_SLOT = "rhs"
_SOLUTION_OUTPUT_SLOT = "solution"
_SOLVING_METHOD_PARAM = "solvingMethod"

_MATRIX_CSV = "2,1\n1,3\n"
_RHS_CSV = "1\n2\n"
_SOLVING_METHOD = "numpy_exact_solver"

_INFRASTRUCTURE_TIMEOUT_SECONDS = 60.0
_WORKER_HEALTH_TIMEOUT_SECONDS = 60.0
_STATUS_TIMEOUT_SECONDS = 60.0
_QUEUE_DRAIN_TIMEOUT_SECONDS = 10.0
_POST_TERMINAL_OBSERVATION_SECONDS = 1.0

_TERMINAL_STATUSES = {"DONE", "ERROR", "CANCELLED"}

StatusMessage = dict[str, Any]


@dataclass(frozen=True)
class RabbitMqEndpoint:
    host: str
    port: int
    user: str
    password: str


def test_slae_worker_docker_image_publishes_error_when_running_job_times_out() -> None:
    """Verify Dockerized SLAE worker reports ERROR for a timed-out job.

    Test chain:

    Docker image
      -> explicit MDDS_WORKER_HANDLER override
      -> HangingJobHandler
      -> RabbitMQ submitted job message
      -> supervised process remains alive
      -> timeout watcher terminates the supervised process
      -> ERROR
      -> submitted message acknowledgement
    """
    test_id = str(uuid4())

    job_queue_name = f"mdds.local.job.solving_slae.timeout.{test_id}"
    status_queue_name = f"mdds.local.status.solving_slae.timeout.{test_id}"
    cancel_queue_name = f"mdds.local.cancel.solving_slae.timeout.{test_id}"

    queue_names = [
        job_queue_name,
        status_queue_name,
        cancel_queue_name,
    ]

    job_id = f"job-timeout-{test_id}"

    manifest_key = f"jobs/{_USER_ID}/{job_id}/manifest.json"
    matrix_key = f"jobs/{_USER_ID}/{job_id}/in/matrix.csv"
    rhs_key = f"jobs/{_USER_ID}/{job_id}/in/rhs.csv"
    solution_key = f"jobs/{_USER_ID}/{job_id}/out/solution.csv"

    with Network() as e2e_network:
        with _new_rabbitmq_container(e2e_network) as rabbitmq_container:
            rabbitmq_endpoint = _rabbitmq_endpoint(rabbitmq_container)
            _wait_for_rabbitmq(rabbitmq_endpoint)

            _declare_queues(rabbitmq_endpoint, queue_names)

            try:
                with _new_minio_container(e2e_network) as minio_container:
                    s3_client, _s3_endpoint = _create_s3_client_and_endpoint(
                        minio_container
                    )
                    _create_bucket_if_absent(s3_client, _S3_BUCKET)
                    _wait_for_s3_bucket(s3_client)

                    _put_bytes(
                        s3_client=s3_client,
                        key=matrix_key,
                        body=_MATRIX_CSV.encode("utf-8"),
                        content_type="text/csv",
                    )
                    _put_bytes(
                        s3_client=s3_client,
                        key=rhs_key,
                        body=_RHS_CSV.encode("utf-8"),
                        content_type="text/csv",
                    )
                    _put_json(
                        s3_client=s3_client,
                        key=manifest_key,
                        value=_manifest(
                            job_id=job_id,
                            matrix_key=matrix_key,
                            rhs_key=rhs_key,
                            solution_key=solution_key,
                        ),
                    )

                    assert _object_exists(s3_client=s3_client, key=manifest_key)
                    assert _object_exists(s3_client=s3_client, key=matrix_key)
                    assert _object_exists(s3_client=s3_client, key=rhs_key)
                    assert not _object_exists(s3_client=s3_client, key=solution_key)

                    with _new_timeout_slae_worker_container(
                        e2e_network=e2e_network,
                        job_queue_name=job_queue_name,
                        status_queue_name=status_queue_name,
                        cancel_queue_name=cancel_queue_name,
                    ) as worker_container:
                        worker_health_url = _worker_health_url(worker_container)
                        _wait_for_worker_health(worker_health_url, worker_container)

                        with StatusQueueConsumer(
                            rabbitmq_endpoint,
                            status_queue_name,
                        ) as status_consumer:
                            statuses: list[StatusMessage] = []

                            _publish_job_message(
                                rabbitmq_endpoint=rabbitmq_endpoint,
                                queue_name=job_queue_name,
                                manifest_key=manifest_key,
                            )

                            error_status = _wait_for_job_status(
                                status_consumer=status_consumer,
                                job_id=job_id,
                                expected_status="ERROR",
                                received_statuses=statuses,
                            )

                            statuses.extend(
                                _collect_statuses_for_observation_window(
                                    status_consumer=status_consumer,
                                    observation_seconds=(
                                        _POST_TERMINAL_OBSERVATION_SECONDS
                                    ),
                                )
                            )
                            statuses.extend(status_consumer.drain())

                            assert (
                                _status_field(
                                    error_status,
                                    "workerId",
                                    "worker_id",
                                )
                                == _WORKER_ID
                            )

                            _assert_status_absent(statuses, job_id, "DONE")
                            _assert_status_absent(statuses, job_id, "CANCELLED")

                            terminal_statuses = _terminal_statuses_for_job(
                                statuses=statuses,
                                job_id=job_id,
                            )

                            assert [
                                _status_name(status) for status in terminal_statuses
                            ] == ["ERROR"]

                            assert not _object_exists(
                                s3_client=s3_client,
                                key=solution_key,
                            )

                            _wait_until(
                                lambda: _basic_get_without_consuming(
                                    rabbitmq_endpoint,
                                    job_queue_name,
                                )
                                is None,
                                description=(
                                    "Job queue to become empty after "
                                    "terminal timeout ack"
                                ),
                                timeout_seconds=_QUEUE_DRAIN_TIMEOUT_SECONDS,
                            )
            finally:
                for queue_name in queue_names:
                    _delete_queue_if_exists(rabbitmq_endpoint, queue_name)


class StatusQueueConsumer:
    """Consumes raw status messages from RabbitMQ status queue."""

    def __init__(
        self,
        rabbitmq_endpoint: RabbitMqEndpoint,
        queue_name: str,
    ) -> None:
        self._rabbitmq_endpoint = rabbitmq_endpoint
        self._queue_name = queue_name
        self._messages: queue.Queue[StatusMessage] = queue.Queue()
        self._stop_requested = threading.Event()
        self._ready = threading.Event()
        self._error: Exception | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> StatusQueueConsumer:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._consume_loop,
            name=f"status-consumer-{self._queue_name}",
            daemon=True,
        )
        self._thread.start()

        if not self._ready.wait(timeout=10.0):
            raise AssertionError("Status queue consumer did not start before timeout.")

        if self._error is not None:
            raise AssertionError(
                "Status queue consumer failed to start."
            ) from self._error

    def stop(self) -> None:
        self._stop_requested.set()

        if self._thread is not None:
            self._thread.join(timeout=5.0)

            if self._thread.is_alive():
                raise AssertionError(
                    "Status queue consumer did not stop before timeout."
                )

    def get(self, timeout: float) -> StatusMessage:
        return self._messages.get(timeout=timeout)

    def drain(self) -> list[StatusMessage]:
        messages: list[StatusMessage] = []

        while True:
            try:
                messages.append(self._messages.get_nowait())
            except queue.Empty:
                return messages

    def _consume_loop(self) -> None:
        connection = None

        try:
            connection = pika.BlockingConnection(
                _rabbitmq_connection_parameters(self._rabbitmq_endpoint)
            )
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=self._handle_delivery,
            )

            self._ready.set()

            while not self._stop_requested.is_set():
                connection.process_data_events(time_limit=0.2)

        except Exception as error:  # pragma: no cover - visible through start/stop.
            self._error = error
            self._ready.set()
        finally:
            if connection is not None and connection.is_open:
                connection.close()

    def _handle_delivery(
        self,
        channel: Any,
        method_frame: Any,
        _properties: Any,
        body: bytes,
    ) -> None:
        self._messages.put(_decode_json_message(body))
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def _new_rabbitmq_container(e2e_network: Network) -> DockerContainer:
    return (
        DockerContainer(_RABBITMQ_IMAGE)
        .with_network(e2e_network)
        .with_network_aliases(_RABBITMQ_NETWORK_ALIAS)
        .with_env("RABBITMQ_DEFAULT_USER", _RABBITMQ_USER)
        .with_env("RABBITMQ_DEFAULT_PASS", _RABBITMQ_PASSWORD)
        .with_exposed_ports(
            _RABBITMQ_INTERNAL_PORT,
            _RABBITMQ_MANAGEMENT_PORT,
        )
        .waiting_for(HttpWaitStrategy(_RABBITMQ_MANAGEMENT_PORT).for_status_code(200))
    )


def _new_minio_container(e2e_network: Network) -> MinioContainer:
    return (
        MinioContainer(
            access_key=_S3_ACCESS_KEY,
            secret_key=_S3_SECRET_KEY,
        )
        .with_network(e2e_network)
        .with_network_aliases(_MINIO_NETWORK_ALIAS)
    )


def _new_timeout_slae_worker_container(
    *,
    e2e_network: Network,
    job_queue_name: str,
    status_queue_name: str,
    cancel_queue_name: str,
) -> DockerContainer:
    return (
        DockerContainer(_SLAE_WORKER_IMAGE)
        .with_network(e2e_network)
        .with_env("MDDS_WORKER_ID", _WORKER_ID)
        .with_env("MDDS_WORKER_JOB_TYPE", _JOB_TYPE)
        .with_env("MDDS_WORKER_JOB_QUEUE_NAME", job_queue_name)
        .with_env("MDDS_WORKER_STATUS_QUEUE_NAME", status_queue_name)
        .with_env("MDDS_WORKER_CANCEL_QUEUE_NAME", cancel_queue_name)
        .with_env("MDDS_WORKER_HANDLER", _HANGING_JOB_HANDLER)
        .with_env("MDDS_RABBITMQ_HOST", _RABBITMQ_NETWORK_ALIAS)
        .with_env("MDDS_RABBITMQ_PORT", str(_RABBITMQ_INTERNAL_PORT))
        .with_env("MDDS_RABBITMQ_USER", _RABBITMQ_USER)
        .with_env("MDDS_RABBITMQ_PASSWORD", _RABBITMQ_PASSWORD)
        .with_env("MDDS_OBJECT_STORAGE_BUCKET", _S3_BUCKET)
        .with_env("MDDS_OBJECT_STORAGE_INTERNAL_ENDPOINT", _S3_INTERNAL_ENDPOINT)
        .with_env("MDDS_OBJECT_STORAGE_REGION", _S3_REGION)
        .with_env("MDDS_OBJECT_STORAGE_ACCESS_KEY", _S3_ACCESS_KEY)
        .with_env("MDDS_OBJECT_STORAGE_SECRET_KEY", _S3_SECRET_KEY)
        .with_env("MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED", "true")
        .with_env("MDDS_WORKER_LOCAL_ROOT", "/tmp/mdds-worker/jobs")
        .with_env("MDDS_WORKER_JOB_TIMEOUT_SECONDS", _WORKER_JOB_TIMEOUT_SECONDS)
        .with_env("MDDS_WORKER_PROGRESS_INTERVAL_SECONDS", "1")
        .with_env("MDDS_WORKER_CLEANUP_INTERVAL_SECONDS", "1")
        .with_env("MDDS_WORKER_HEALTH_HOST", "0.0.0.0")
        .with_env("MDDS_WORKER_HEALTH_PORT", str(_WORKER_HEALTH_PORT))
        .with_exposed_ports(_WORKER_HEALTH_PORT)
    )


def _rabbitmq_endpoint(rabbitmq_container: DockerContainer) -> RabbitMqEndpoint:
    return RabbitMqEndpoint(
        host=rabbitmq_container.get_container_host_ip(),
        port=int(rabbitmq_container.get_exposed_port(_RABBITMQ_INTERNAL_PORT)),
        user=_RABBITMQ_USER,
        password=_RABBITMQ_PASSWORD,
    )


def _worker_health_url(worker_container: DockerContainer) -> str:
    host = worker_container.get_container_host_ip()
    port = worker_container.get_exposed_port(_WORKER_HEALTH_PORT)

    return f"http://{host}:{port}/health"


def _wait_for_rabbitmq(rabbitmq_endpoint: RabbitMqEndpoint) -> None:
    def rabbitmq_is_ready() -> bool:
        connection = None

        try:
            connection = pika.BlockingConnection(
                _rabbitmq_connection_parameters(rabbitmq_endpoint)
            )
            return connection.is_open
        except pika.exceptions.AMQPError:
            return False
        except OSError:
            return False
        finally:
            if connection is not None and connection.is_open:
                connection.close()

    _wait_until(
        rabbitmq_is_ready,
        description="RabbitMQ readiness",
        timeout_seconds=_INFRASTRUCTURE_TIMEOUT_SECONDS,
    )


def _wait_for_s3_bucket(s3_client: Any) -> None:
    def bucket_exists() -> bool:
        try:
            s3_client.head_bucket(Bucket=_S3_BUCKET)
            return True
        except ClientError:
            return False

    _wait_until(
        bucket_exists,
        description="S3 bucket readiness",
        timeout_seconds=_INFRASTRUCTURE_TIMEOUT_SECONDS,
    )


def _wait_for_worker_health(
    worker_health_url: str,
    worker_container: DockerContainer,
) -> None:
    def worker_is_healthy() -> bool:
        try:
            with urllib.request.urlopen(
                worker_health_url,
                timeout=2,
            ) as response:
                return response.status == 200 and response.read() == b"OK\n"
        except (OSError, urllib.error.URLError):
            return False

    try:
        _wait_until(
            worker_is_healthy,
            description="SLAE Worker /health readiness",
            timeout_seconds=_WORKER_HEALTH_TIMEOUT_SECONDS,
        )
    except AssertionError as error:
        raise AssertionError(
            f"{error}\n\nSLAE Worker container logs:\n"
            f"{_container_logs(worker_container)}"
        ) from error


def _declare_queues(
    rabbitmq_endpoint: RabbitMqEndpoint,
    queue_names: list[str],
) -> None:
    connection = pika.BlockingConnection(
        _rabbitmq_connection_parameters(rabbitmq_endpoint)
    )

    try:
        channel = connection.channel()

        for queue_name in queue_names:
            channel.queue_declare(
                queue=queue_name,
                durable=False,
                exclusive=False,
                auto_delete=False,
            )
    finally:
        connection.close()


def _delete_queue_if_exists(
    rabbitmq_endpoint: RabbitMqEndpoint,
    queue_name: str,
) -> None:
    connection = pika.BlockingConnection(
        _rabbitmq_connection_parameters(rabbitmq_endpoint)
    )

    try:
        channel = connection.channel()
        channel.queue_delete(queue=queue_name)
    finally:
        connection.close()


def _publish_job_message(
    *,
    rabbitmq_endpoint: RabbitMqEndpoint,
    queue_name: str,
    manifest_key: str,
) -> None:
    connection = pika.BlockingConnection(
        _rabbitmq_connection_parameters(rabbitmq_endpoint)
    )

    try:
        channel = connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps({"manifestObjectKey": manifest_key}).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=1,
            ),
        )
    finally:
        connection.close()


def _basic_get_without_consuming(
    rabbitmq_endpoint: RabbitMqEndpoint,
    queue_name: str,
) -> bytes | None:
    connection = pika.BlockingConnection(
        _rabbitmq_connection_parameters(rabbitmq_endpoint)
    )

    try:
        channel = connection.channel()
        method_frame, _header_frame, body = channel.basic_get(
            queue=queue_name,
            auto_ack=False,
        )

        if method_frame is None:
            return None

        channel.basic_nack(
            delivery_tag=method_frame.delivery_tag,
            requeue=True,
        )
        return body
    finally:
        connection.close()


def _rabbitmq_connection_parameters(
    rabbitmq_endpoint: RabbitMqEndpoint,
) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=rabbitmq_endpoint.host,
        port=rabbitmq_endpoint.port,
        credentials=pika.PlainCredentials(
            rabbitmq_endpoint.user,
            rabbitmq_endpoint.password,
        ),
        heartbeat=60,
        blocked_connection_timeout=20,
        connection_attempts=1,
    )


def _create_s3_client_and_endpoint(minio_container: MinioContainer) -> tuple[Any, str]:
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
        aws_access_key_id=_S3_ACCESS_KEY,
        aws_secret_access_key=_S3_SECRET_KEY,
        region_name=_S3_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )

    return s3_client, endpoint


def _create_bucket_if_absent(
    s3_client: Any,
    bucket: str,
) -> None:
    existing_buckets = s3_client.list_buckets()["Buckets"]
    existing_bucket_names = {bucket_info["Name"] for bucket_info in existing_buckets}

    if bucket not in existing_bucket_names:
        s3_client.create_bucket(Bucket=bucket)


def _put_json(
    *,
    s3_client: Any,
    key: str,
    value: object,
) -> None:
    _put_bytes(
        s3_client=s3_client,
        key=key,
        body=json.dumps(value).encode("utf-8"),
        content_type="application/json",
    )


def _put_bytes(
    *,
    s3_client: Any,
    key: str,
    body: bytes,
    content_type: str,
) -> None:
    s3_client.put_object(
        Bucket=_S3_BUCKET,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def _object_exists(
    *,
    s3_client: Any,
    key: str,
) -> bool:
    try:
        s3_client.head_object(Bucket=_S3_BUCKET, Key=key)
        return True
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        http_status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if error_code in {"404", "NoSuchKey", "NotFound"} or http_status == 404:
            return False

        raise


def _manifest(
    *,
    job_id: str,
    matrix_key: str,
    rhs_key: str,
    solution_key: str,
) -> dict[str, Any]:
    return {
        "manifestVersion": 1,
        "userId": _USER_ID,
        "jobId": job_id,
        "jobType": _JOB_TYPE,
        "inputs": {
            _MATRIX_INPUT_SLOT: {
                "objectKey": matrix_key,
                "format": "csv",
            },
            _RHS_INPUT_SLOT: {
                "objectKey": rhs_key,
                "format": "csv",
            },
        },
        "params": {
            _SOLVING_METHOD_PARAM: _SOLVING_METHOD,
        },
        "outputs": {
            _SOLUTION_OUTPUT_SLOT: {
                "objectKey": solution_key,
                "format": "csv",
            },
        },
    }


def _wait_for_job_status(
    *,
    status_consumer: StatusQueueConsumer,
    job_id: str,
    expected_status: str,
    received_statuses: list[StatusMessage],
) -> StatusMessage:
    deadline = time.monotonic() + _STATUS_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())

        try:
            status = status_consumer.get(timeout=min(1.0, remaining))
        except queue.Empty:
            continue

        if _status_job_id(status) != job_id:
            continue

        received_statuses.append(status)
        status_name = _status_name(status)

        if status_name == expected_status:
            return status

        if status_name in _TERMINAL_STATUSES:
            raise AssertionError(
                "Unexpected terminal status was published before expected status: "
                f"jobId='{job_id}', expectedStatus='{expected_status}', "
                f"actualStatus='{status_name}', message='{status.get('message')}'."
            )

    raise AssertionError(
        "Expected status was not published before timeout: "
        f"jobId='{job_id}', expectedStatus='{expected_status}', "
        f"receivedStatuses={[_status_name(status) for status in received_statuses]}."
    )


def _collect_statuses_for_observation_window(
    *,
    status_consumer: StatusQueueConsumer,
    observation_seconds: float,
) -> list[StatusMessage]:
    deadline = time.monotonic() + observation_seconds
    statuses: list[StatusMessage] = []

    while time.monotonic() < deadline:
        remaining = max(0.0, deadline - time.monotonic())

        try:
            statuses.append(
                status_consumer.get(timeout=min(0.1, remaining)),
            )
        except queue.Empty:
            continue

    return statuses


def _assert_status_absent(
    statuses: list[StatusMessage],
    job_id: str,
    status_name: str,
) -> None:
    assert _index_of_status(statuses, job_id, status_name) is None


def _terminal_statuses_for_job(
    *,
    statuses: list[StatusMessage],
    job_id: str,
) -> list[StatusMessage]:
    return [
        status
        for status in statuses
        if _status_job_id(status) == job_id
        and _status_name(status) in _TERMINAL_STATUSES
    ]


def _index_of_status(
    statuses: list[StatusMessage],
    job_id: str,
    status_name: str,
) -> int | None:
    for index, status in enumerate(statuses):
        if _status_job_id(status) == job_id and _status_name(status) == status_name:
            return index

    return None


def _status_job_id(status: StatusMessage) -> str:
    return _status_field(status, "jobId", "job_id")


def _status_name(status: StatusMessage) -> str:
    return str(status.get("status", ""))


def _status_field(
    status: StatusMessage,
    camel_case_name: str,
    snake_case_name: str,
) -> str:
    value = status.get(camel_case_name, status.get(snake_case_name, ""))

    return str(value)


def _decode_json_message(body: bytes) -> StatusMessage:
    decoded = json.loads(body.decode("utf-8"))

    if not isinstance(decoded, dict):
        raise AssertionError(f"Expected JSON object message, got: {decoded!r}")

    return decoded


def _container_logs(container: DockerContainer) -> str:
    try:
        stdout, stderr = container.get_logs()
    except Exception as error:  # pragma: no cover - diagnostic fallback only.
        return f"Cannot read container logs: {error!r}"

    stdout_text = stdout.decode("utf-8", errors="replace")
    stderr_text = stderr.decode("utf-8", errors="replace")

    return f"STDOUT:\n{stdout_text}\nSTDERR:\n{stderr_text}"


def _wait_until(
    condition: Callable[[], bool],
    *,
    description: str,
    timeout_seconds: float,
    interval_seconds: float = 0.2,
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if condition():
            return

        time.sleep(interval_seconds)

    raise AssertionError(f"{description} was not satisfied before timeout.")
