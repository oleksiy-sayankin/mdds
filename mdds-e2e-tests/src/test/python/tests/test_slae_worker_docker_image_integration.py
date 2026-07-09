# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""End-to-end tests for SLAE Python Worker Docker image."""

from __future__ import annotations

from collections.abc import Callable, Iterator
import csv
from dataclasses import dataclass
import io
import json
import math
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
import pytest
from testcontainers.core.network import Network
from testcontainers.minio import MinioContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

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
_WORKER_ID = "slae-worker-local"

_JOB_QUEUE_NAME = "mdds.local.job.solving_slae"
_STATUS_QUEUE_NAME = "mdds.local.status.solving_slae"
_CANCEL_QUEUE_NAME = "mdds.local.cancel.solving_slae"

_RABBITMQ_QUEUE_NAMES = [
    _JOB_QUEUE_NAME,
    _STATUS_QUEUE_NAME,
    _CANCEL_QUEUE_NAME,
]

_USER_ID = 123
_JOB_TYPE = "solving_slae"

_MATRIX_INPUT_SLOT = "matrix"
_RHS_INPUT_SLOT = "rhs"
_SOLUTION_OUTPUT_SLOT = "solution"
_SOLVING_METHOD_PARAM = "solvingMethod"

_SOLVING_METHODS = [
    "numpy_exact_solver",
    "numpy_lstsq_solver",
    "numpy_pinv_solver",
    "petsc_solver",
    "scipy_gmres_solver",
]

_SINGULAR_MATRIX_SOLVING_METHODS = {
    "numpy_exact_solver": {
        "expected_message_fragments": (
            "singular",
            "determinant",
            "linearly dependent",
            "linear dependent",
            "infinite",
            "unique solution",
            "no unique solution",
        ),
    },
}

_MATRIX_CSV = "2,1\n1,3\n"
_RHS_CSV = "1\n2\n"
_EXPECTED_SOLUTION = [0.2, 0.6]

_INFRASTRUCTURE_TIMEOUT_SECONDS = 60.0
_WORKER_HEALTH_TIMEOUT_SECONDS = 60.0
_STATUS_TIMEOUT_SECONDS = 60.0


StatusMessage = dict[str, Any]


@dataclass(frozen=True)
class RabbitMqEndpoint:
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class SlaeWorkerCluster:
    rabbitmq: RabbitMqEndpoint
    s3_client: Any
    worker_health_url: str


@pytest.fixture(scope="module")
def e2e_network() -> Iterator[Network]:
    """Create shared Docker network for RabbitMQ, MinIO, and SLAE worker."""
    with Network() as network:
        yield network


@pytest.fixture(scope="module")
def rabbitmq_container(e2e_network: Network) -> Iterator[DockerContainer]:
    """Start RabbitMQ in the shared e2e Docker network."""
    with (
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
    ) as container:
        yield container


@pytest.fixture(scope="module")
def minio_container(e2e_network: Network) -> Iterator[MinioContainer]:
    """Start MinIO in the shared e2e Docker network."""
    with (
        MinioContainer(
            access_key=_S3_ACCESS_KEY,
            secret_key=_S3_SECRET_KEY,
        )
        .with_network(e2e_network)
        .with_network_aliases(_MINIO_NETWORK_ALIAS)
    ) as container:
        yield container


@pytest.fixture(scope="module")
def slae_worker_cluster(
    e2e_network: Network,
    rabbitmq_container: DockerContainer,
    minio_container: MinioContainer,
) -> Iterator[SlaeWorkerCluster]:
    """Start SLAE Worker Docker image once for all solver-method test cases."""
    rabbitmq_endpoint = _rabbitmq_endpoint(rabbitmq_container)
    _wait_for_rabbitmq(rabbitmq_endpoint)

    s3_client, _s3_endpoint = _create_s3_client_and_endpoint(minio_container)
    _create_bucket_if_absent(s3_client, _S3_BUCKET)
    _wait_for_s3_bucket(s3_client)

    _declare_queues(rabbitmq_endpoint, _RABBITMQ_QUEUE_NAMES)

    with _new_slae_worker_container(e2e_network) as worker_container:
        worker_health_url = _worker_health_url(worker_container)
        _wait_for_worker_health(worker_health_url, worker_container)

        yield SlaeWorkerCluster(
            rabbitmq=rabbitmq_endpoint,
            s3_client=s3_client,
            worker_health_url=worker_health_url,
        )


@pytest.fixture()
def clean_rabbitmq_queues(
    slae_worker_cluster: SlaeWorkerCluster,
) -> Iterator[None]:
    """Keep RabbitMQ queues isolated between parameterized solver test cases."""
    _purge_queues(slae_worker_cluster.rabbitmq, _RABBITMQ_QUEUE_NAMES)

    yield

    _purge_queues(slae_worker_cluster.rabbitmq, _RABBITMQ_QUEUE_NAMES)


@pytest.mark.parametrize("solving_method", _SOLVING_METHODS)
def test_slae_worker_docker_image_processes_real_job_through_rabbitmq_and_s3(
    slae_worker_cluster: SlaeWorkerCluster,
    clean_rabbitmq_queues: None,
    solving_method: str,
) -> None:
    """Verify concrete SLAE Docker image through real RabbitMQ and S3 flow.

    Test chain:

    Docker image
      -> inherited runtime entrypoint
      -> image-level default handler/job type
      -> RabbitMQ startup readiness
      -> S3 bucket readiness
      -> /health
      -> real job message
      -> SLAE handler
      -> solution.csv in S3
    """
    del clean_rabbitmq_queues

    s3_client = slae_worker_cluster.s3_client
    rabbitmq_endpoint = slae_worker_cluster.rabbitmq
    job_id = f"job-{solving_method}-{uuid4()}"

    manifest_key = f"jobs/{_USER_ID}/{job_id}/manifest.json"
    matrix_key = f"jobs/{_USER_ID}/{job_id}/in/matrix.csv"
    rhs_key = f"jobs/{_USER_ID}/{job_id}/in/rhs.csv"
    solution_key = f"jobs/{_USER_ID}/{job_id}/out/solution.csv"

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
            solving_method=solving_method,
        ),
    )

    with StatusQueueConsumer(rabbitmq_endpoint, _STATUS_QUEUE_NAME) as status_consumer:
        _publish_job_message(rabbitmq_endpoint, manifest_key)

        statuses = _wait_for_status_sequence(
            status_consumer=status_consumer,
            job_id=job_id,
            expected_statuses=[
                "INPUTS_PREPARED",
                "IN_PROGRESS",
                "DONE",
            ],
        )
        statuses.extend(status_consumer.drain())

    _assert_status_order(
        statuses=statuses,
        job_id=job_id,
        earlier_status="INPUTS_PREPARED",
        later_status="IN_PROGRESS",
    )
    _assert_status_order(
        statuses=statuses,
        job_id=job_id,
        earlier_status="IN_PROGRESS",
        later_status="DONE",
    )
    _assert_status_absent(statuses, job_id, "ERROR")
    _assert_status_absent(statuses, job_id, "CANCELLED")

    done_status = _status_by_name(statuses, job_id, "DONE")
    assert done_status is not None
    assert _status_field(done_status, "workerId", "worker_id") == _WORKER_ID
    assert int(done_status["progress"]) == 100

    solution = _read_solution_from_s3(
        s3_client=s3_client,
        key=solution_key,
    )

    assert _vectors_are_close(solution, _EXPECTED_SOLUTION)

    _wait_until(
        lambda: _basic_get_without_consuming(rabbitmq_endpoint, _JOB_QUEUE_NAME)
        is None,
        description="Job queue to become empty after terminal ack",
        timeout_seconds=10.0,
    )


def test_slae_worker_docker_image_publishes_error_when_rhs_input_is_missing_in_s3(
    slae_worker_cluster: SlaeWorkerCluster,
    clean_rabbitmq_queues: None,
) -> None:
    """Verify SLAE worker publishes ERROR when declared rhs input is absent in S3."""
    del clean_rabbitmq_queues

    s3_client = slae_worker_cluster.s3_client
    rabbitmq_endpoint = slae_worker_cluster.rabbitmq
    job_id = f"job-missing-rhs-{uuid4()}"

    manifest_key = f"jobs/{_USER_ID}/{job_id}/manifest.json"
    matrix_key = f"jobs/{_USER_ID}/{job_id}/in/matrix.csv"
    rhs_key = f"jobs/{_USER_ID}/{job_id}/in/rhs.csv"
    solution_key = f"jobs/{_USER_ID}/{job_id}/out/solution.csv"

    _put_bytes(
        s3_client=s3_client,
        key=matrix_key,
        body=_MATRIX_CSV.encode("utf-8"),
        content_type="text/csv",
    )

    # Intentionally do not upload rhs_key.
    _put_json(
        s3_client=s3_client,
        key=manifest_key,
        value=_manifest(
            job_id=job_id,
            matrix_key=matrix_key,
            rhs_key=rhs_key,
            solution_key=solution_key,
            solving_method="numpy_exact_solver",
        ),
    )

    statuses: list[StatusMessage] = []

    with StatusQueueConsumer(rabbitmq_endpoint, _STATUS_QUEUE_NAME) as status_consumer:
        _publish_job_message(rabbitmq_endpoint, manifest_key)

        deadline = time.monotonic() + _STATUS_TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())

            try:
                status = status_consumer.get(timeout=min(1.0, remaining))
            except queue.Empty:
                continue

            if _status_job_id(status) != job_id:
                continue

            statuses.append(status)

            if _status_name(status) == "ERROR":
                break

        statuses.extend(status_consumer.drain())

    error_status = _status_by_name(statuses, job_id, "ERROR")
    assert error_status is not None, (
        "ERROR status was not published for job with missing rhs input. "
        f"receivedStatuses={[_status_name(status) for status in statuses]}"
    )

    assert _status_field(error_status, "workerId", "worker_id") == _WORKER_ID
    assert str(error_status.get("message", "")).strip() != ""

    _assert_status_absent(statuses, job_id, "INPUTS_PREPARED")
    _assert_status_absent(statuses, job_id, "IN_PROGRESS")
    _assert_status_absent(statuses, job_id, "DONE")
    _assert_status_absent(statuses, job_id, "CANCELLED")

    _wait_until(
        lambda: _basic_get_without_consuming(rabbitmq_endpoint, _JOB_QUEUE_NAME)
        is None,
        description="Job queue to become empty after terminal error ack",
        timeout_seconds=10.0,
    )


def test_slae_worker_docker_image_publishes_error_when_matrix_contains_not_number(
    slae_worker_cluster: SlaeWorkerCluster,
    clean_rabbitmq_queues: None,
) -> None:
    """Verify SLAE worker publishes ERROR when matrix CSV contains non-numeric value."""
    del clean_rabbitmq_queues

    s3_client = slae_worker_cluster.s3_client
    rabbitmq_endpoint = slae_worker_cluster.rabbitmq
    job_id = f"job-invalid-matrix-number-{uuid4()}"

    manifest_key = f"jobs/{_USER_ID}/{job_id}/manifest.json"
    matrix_key = f"jobs/{_USER_ID}/{job_id}/in/matrix.csv"
    rhs_key = f"jobs/{_USER_ID}/{job_id}/in/rhs.csv"
    solution_key = f"jobs/{_USER_ID}/{job_id}/out/solution.csv"

    invalid_matrix_csv = "2,not-a-number\n1,3\n"

    _put_bytes(
        s3_client=s3_client,
        key=matrix_key,
        body=invalid_matrix_csv.encode("utf-8"),
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
            solving_method="numpy_exact_solver",
        ),
    )

    statuses: list[StatusMessage] = []

    with StatusQueueConsumer(rabbitmq_endpoint, _STATUS_QUEUE_NAME) as status_consumer:
        _publish_job_message(rabbitmq_endpoint, manifest_key)

        deadline = time.monotonic() + _STATUS_TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())

            try:
                status = status_consumer.get(timeout=min(1.0, remaining))
            except queue.Empty:
                continue

            if _status_job_id(status) != job_id:
                continue

            statuses.append(status)

            if _status_name(status) == "ERROR":
                break

        statuses.extend(status_consumer.drain())

    error_status = _status_by_name(statuses, job_id, "ERROR")
    assert error_status is not None, (
        "ERROR status was not published for job with invalid matrix number. "
        f"receivedStatuses={[_status_name(status) for status in statuses]}"
    )

    error_message = str(error_status.get("message", "")).strip()

    assert _status_field(error_status, "workerId", "worker_id") == _WORKER_ID
    assert error_message != ""
    assert (
        "Input artifact 'matrix' contains a non-numeric value in row 1" in error_message
    )

    _assert_status_order(
        statuses=statuses,
        job_id=job_id,
        earlier_status="INPUTS_PREPARED",
        later_status="IN_PROGRESS",
    )
    _assert_status_order(
        statuses=statuses,
        job_id=job_id,
        earlier_status="IN_PROGRESS",
        later_status="ERROR",
    )

    _assert_status_absent(statuses, job_id, "DONE")
    _assert_status_absent(statuses, job_id, "CANCELLED")

    _wait_until(
        lambda: _basic_get_without_consuming(rabbitmq_endpoint, _JOB_QUEUE_NAME)
        is None,
        description="Job queue to become empty after terminal error ack",
        timeout_seconds=10.0,
    )


@pytest.mark.parametrize("solving_method", _SINGULAR_MATRIX_SOLVING_METHODS.keys())
def test_slae_worker_docker_image_publishes_error_when_matrix_is_singular(
    slae_worker_cluster: SlaeWorkerCluster,
    clean_rabbitmq_queues: None,
    solving_method: str,
) -> None:
    """Verify SLAE worker publishes ERROR for singular matrix."""
    del clean_rabbitmq_queues

    s3_client = slae_worker_cluster.s3_client
    rabbitmq_endpoint = slae_worker_cluster.rabbitmq
    job_id = f"job-singular-matrix-{solving_method}-{uuid4()}"

    manifest_key = f"jobs/{_USER_ID}/{job_id}/manifest.json"
    matrix_key = f"jobs/{_USER_ID}/{job_id}/in/matrix.csv"
    rhs_key = f"jobs/{_USER_ID}/{job_id}/in/rhs.csv"
    solution_key = f"jobs/{_USER_ID}/{job_id}/out/solution.csv"

    singular_matrix_csv = "0,0\n0,0\n"
    zero_rhs_csv = "0\n0\n"

    _put_bytes(
        s3_client=s3_client,
        key=matrix_key,
        body=singular_matrix_csv.encode("utf-8"),
        content_type="text/csv",
    )
    _put_bytes(
        s3_client=s3_client,
        key=rhs_key,
        body=zero_rhs_csv.encode("utf-8"),
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
            solving_method=solving_method,
        ),
    )

    statuses: list[StatusMessage] = []

    with StatusQueueConsumer(rabbitmq_endpoint, _STATUS_QUEUE_NAME) as status_consumer:
        _publish_job_message(rabbitmq_endpoint, manifest_key)

        deadline = time.monotonic() + _STATUS_TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())

            try:
                status = status_consumer.get(timeout=min(1.0, remaining))
            except queue.Empty:
                continue

            if _status_job_id(status) != job_id:
                continue

            statuses.append(status)

            if _status_name(status) == "ERROR":
                break

        statuses.extend(status_consumer.drain())

    error_status = _status_by_name(statuses, job_id, "ERROR")
    assert error_status is not None, (
        "ERROR status was not published for singular SLAE matrix. "
        f"solvingMethod={solving_method!r}, "
        f"receivedStatuses={[_status_name(status) for status in statuses]}"
    )

    error_message = str(error_status.get("message", "")).strip()
    expected_message_fragments = _SINGULAR_MATRIX_SOLVING_METHODS[solving_method][
        "expected_message_fragments"
    ]

    assert _status_field(error_status, "workerId", "worker_id") == _WORKER_ID
    assert error_message != ""
    assert any(
        fragment in error_message.lower() for fragment in expected_message_fragments
    ), (
        "ERROR message does not explain singular matrix / non-unique solution. "
        f"solvingMethod={solving_method!r}, message={error_message!r}"
    )

    _assert_status_order(
        statuses=statuses,
        job_id=job_id,
        earlier_status="INPUTS_PREPARED",
        later_status="IN_PROGRESS",
    )
    _assert_status_order(
        statuses=statuses,
        job_id=job_id,
        earlier_status="IN_PROGRESS",
        later_status="ERROR",
    )

    _assert_status_absent(statuses, job_id, "DONE")
    _assert_status_absent(statuses, job_id, "CANCELLED")

    _wait_until(
        lambda: _basic_get_without_consuming(rabbitmq_endpoint, _JOB_QUEUE_NAME)
        is None,
        description="Job queue to become empty after terminal error ack",
        timeout_seconds=10.0,
    )


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


def _new_slae_worker_container(e2e_network: Network) -> DockerContainer:
    return (
        DockerContainer(_SLAE_WORKER_IMAGE)
        .with_network(e2e_network)
        .with_env("MDDS_WORKER_ID", _WORKER_ID)
        .with_env("MDDS_WORKER_JOB_QUEUE_NAME", _JOB_QUEUE_NAME)
        .with_env("MDDS_WORKER_STATUS_QUEUE_NAME", _STATUS_QUEUE_NAME)
        .with_env("MDDS_WORKER_CANCEL_QUEUE_NAME", _CANCEL_QUEUE_NAME)
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
        .with_env("MDDS_WORKER_JOB_TIMEOUT_SECONDS", "30")
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


def _purge_queues(
    rabbitmq_endpoint: RabbitMqEndpoint,
    queue_names: list[str],
) -> None:
    connection = pika.BlockingConnection(
        _rabbitmq_connection_parameters(rabbitmq_endpoint)
    )

    try:
        channel = connection.channel()

        for queue_name in queue_names:
            channel.queue_purge(queue=queue_name)
    finally:
        connection.close()


def _publish_job_message(
    rabbitmq_endpoint: RabbitMqEndpoint,
    manifest_key: str,
) -> None:
    connection = pika.BlockingConnection(
        _rabbitmq_connection_parameters(rabbitmq_endpoint)
    )

    try:
        channel = connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key=_JOB_QUEUE_NAME,
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


def _read_solution_from_s3(
    *,
    s3_client: Any,
    key: str,
) -> list[float]:
    response = s3_client.get_object(
        Bucket=_S3_BUCKET,
        Key=key,
    )
    content = response["Body"].read().decode("utf-8")

    return _read_csv_vector(content)


def _read_csv_vector(content: str) -> list[float]:
    values: list[float] = []

    for row in csv.reader(io.StringIO(content)):
        if len(row) == 0 or all(cell.strip() == "" for cell in row):
            continue

        if len(row) != 1:
            raise AssertionError(f"Expected one value per solution row, got: {row}")

        values.append(float(row[0]))

    return values


def _manifest(
    *,
    job_id: str,
    matrix_key: str,
    rhs_key: str,
    solution_key: str,
    solving_method: str,
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
            _SOLVING_METHOD_PARAM: solving_method,
        },
        "outputs": {
            _SOLUTION_OUTPUT_SLOT: {
                "objectKey": solution_key,
                "format": "csv",
            },
        },
    }


def _wait_for_status_sequence(
    *,
    status_consumer: StatusQueueConsumer,
    job_id: str,
    expected_statuses: list[str],
) -> list[StatusMessage]:
    deadline = time.monotonic() + _STATUS_TIMEOUT_SECONDS
    received: list[StatusMessage] = []
    expected_index = 0

    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())

        try:
            status = status_consumer.get(timeout=min(1.0, remaining))
        except queue.Empty:
            continue

        if _status_job_id(status) != job_id:
            continue

        received.append(status)

        status_name = _status_name(status)

        if status_name in {"ERROR", "CANCELLED"}:
            raise AssertionError(
                "Unexpected terminal status was published: "
                f"jobId='{job_id}', status='{status_name}', "
                f"message='{status.get('message')}'."
            )

        if status_name == expected_statuses[expected_index]:
            expected_index += 1

            if expected_index == len(expected_statuses):
                return received

    raise AssertionError(
        "Expected status sequence was not published before timeout: "
        f"jobId='{job_id}', expectedStatuses={expected_statuses}, "
        f"receivedStatuses={[_status_name(status) for status in received]}."
    )


def _assert_status_order(
    *,
    statuses: list[StatusMessage],
    job_id: str,
    earlier_status: str,
    later_status: str,
) -> None:
    earlier_index = _index_of_status(statuses, job_id, earlier_status)
    later_index = _index_of_status(statuses, job_id, later_status)

    assert earlier_index is not None, f"{earlier_status} status was not published."
    assert later_index is not None, f"{later_status} status was not published."
    assert earlier_index < later_index


def _assert_status_absent(
    statuses: list[StatusMessage],
    job_id: str,
    status_name: str,
) -> None:
    assert _index_of_status(statuses, job_id, status_name) is None


def _status_by_name(
    statuses: list[StatusMessage],
    job_id: str,
    status_name: str,
) -> StatusMessage | None:
    index = _index_of_status(statuses, job_id, status_name)

    if index is None:
        return None

    return statuses[index]


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


def _vectors_are_close(
    actual: list[float],
    expected: list[float],
    *,
    abs_tol: float = 1e-8,
) -> bool:
    return len(actual) == len(expected) and all(
        math.isclose(actual_value, expected_value, abs_tol=abs_tol)
        for actual_value, expected_value in zip(actual, expected)
    )


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
