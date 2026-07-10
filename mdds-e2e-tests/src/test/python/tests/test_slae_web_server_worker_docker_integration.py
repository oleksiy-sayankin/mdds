# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Docker e2e tests for SLAE REST v1 Web Server-to-Worker flow."""

from __future__ import annotations

from collections.abc import Callable, Iterator
import csv
from dataclasses import dataclass
import io
import json
import math
import socket
import time
from typing import Any
from urllib.parse import ParseResult, urlparse
import urllib.error
import urllib.request
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pika
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

_RABBITMQ_IMAGE = "rabbitmq:3.13-management"
_POSTGRES_IMAGE = "postgres:18"
_REDIS_IMAGE = "redis:7.4"
_WEB_SERVER_IMAGE = "mddsproject/web-server:0.1.0"
_SLAE_WORKER_IMAGE = "mddsproject/python-worker-solving-slae:0.1.0"
_MINIO_IMAGE = "minio/minio:RELEASE.2022-12-02T19-19-22Z"

_WEB_SERVER_NETWORK_ALIAS = "web-server"
_RABBITMQ_NETWORK_ALIAS = "rabbitmq"
_POSTGRES_NETWORK_ALIAS = "postgres"
_REDIS_NETWORK_ALIAS = "redis"
_MINIO_NETWORK_ALIAS = "minio"

_WEB_SERVER_INTERNAL_PORT = 8000
_WORKER_HEALTH_PORT = 12457
_RABBITMQ_INTERNAL_PORT = 5672
_RABBITMQ_MANAGEMENT_PORT = 15672
_POSTGRES_INTERNAL_PORT = 5432
_REDIS_INTERNAL_PORT = 6379

_RABBITMQ_USER = "guest"
_RABBITMQ_PASSWORD = "guest"

_POSTGRES_DB = "mdds"
_POSTGRES_USER = "mdds"
_POSTGRES_PASSWORD = "mdds123"

_S3_INTERNAL_ENDPOINT = f"http://{_MINIO_NETWORK_ALIAS}:9000"
_S3_BUCKET = "mdds"
_S3_REGION = "us-east-1"
_S3_ACCESS_KEY = "minioadmin"
_S3_SECRET_KEY = "minioadmin123"

_WORKER_ID = "slae-worker-rest-v1-local"

_JOB_QUEUE_NAME = "queue-solving_slae"
_STATUS_QUEUE_NAME = "mdds_status_queue"
_CANCEL_QUEUE_NAME = f"cancel.queue-{_WORKER_ID}"

_RABBITMQ_QUEUE_NAMES = [
    _JOB_QUEUE_NAME,
    _STATUS_QUEUE_NAME,
    _CANCEL_QUEUE_NAME,
]

_USER_LOGIN = "guest"
_JOB_TYPE = "solving_slae"
_MATRIX_INPUT_SLOT = "matrix"
_RHS_INPUT_SLOT = "rhs"
_SOLUTION_OUTPUT_SLOT = "solution"
_SOLVING_METHODS = [
    "numpy_exact_solver",
    "numpy_lstsq_solver",
    "numpy_pinv_solver",
    "petsc_solver",
    "scipy_gmres_solver",
]


_MATRIX_CSV = "2,1\n1,3\n"
_RHS_CSV = "1\n2\n"
_EXPECTED_SOLUTION = [0.2, 0.6]

_INFRASTRUCTURE_TIMEOUT_SECONDS = 60.0
_WEB_SERVER_HEALTH_TIMEOUT_SECONDS = 60.0
_WORKER_HEALTH_TIMEOUT_SECONDS = 60.0
_JOB_DONE_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True)
class RabbitMqEndpoint:
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class HostPortEndpoint:
    host: str
    port: int


@dataclass(frozen=True)
class E2eEnvironment:
    web_server_url: str
    web_server_container: DockerContainer
    minio_container: MinioContainer


@pytest.fixture(scope="module")
def e2e_environment() -> Iterator[E2eEnvironment]:
    """Start shared Docker infrastructure once for all solver-method cases."""
    with Network() as e2e_network:
        with _new_rabbitmq_container(e2e_network) as rabbitmq_container:
            rabbitmq_endpoint = _rabbitmq_endpoint(rabbitmq_container)
            _wait_for_rabbitmq(rabbitmq_endpoint)
            _declare_queues(rabbitmq_endpoint, _RABBITMQ_QUEUE_NAMES)

            with _new_postgres_container(e2e_network) as postgres_container:
                _wait_for_tcp_endpoint(
                    _host_port_endpoint(postgres_container, _POSTGRES_INTERNAL_PORT),
                    description="PostgreSQL TCP readiness",
                    timeout_seconds=_INFRASTRUCTURE_TIMEOUT_SECONDS,
                )

                with _new_redis_container(e2e_network) as redis_container:
                    _wait_for_tcp_endpoint(
                        _host_port_endpoint(redis_container, _REDIS_INTERNAL_PORT),
                        description="Redis TCP readiness",
                        timeout_seconds=_INFRASTRUCTURE_TIMEOUT_SECONDS,
                    )

                    with _new_minio_container(e2e_network) as minio_container:
                        s3_client, _s3_endpoint = _create_s3_client_and_endpoint(
                            minio_container
                        )
                        _create_bucket_if_absent(s3_client, _S3_BUCKET)
                        _wait_for_s3_bucket(s3_client)

                        with _new_web_server_container(
                            e2e_network
                        ) as web_server_container:
                            web_server_url = _web_server_url(web_server_container)
                            _wait_for_web_server_health(
                                web_server_url,
                                web_server_container,
                            )

                            with _new_slae_worker_container(
                                e2e_network
                            ) as worker_container:
                                worker_health_url = _worker_health_url(worker_container)
                                _wait_for_worker_health(
                                    worker_health_url,
                                    worker_container,
                                )

                                yield E2eEnvironment(
                                    web_server_url=web_server_url,
                                    web_server_container=web_server_container,
                                    minio_container=minio_container,
                                )


@pytest.mark.parametrize("solving_method", _SOLVING_METHODS, ids=_SOLVING_METHODS)
def test_slae_rest_v1_web_server_to_worker_flow_solves_job(
    e2e_environment: E2eEnvironment,
    solving_method: str,
) -> None:
    """Verify SLAE REST v1 flow through Web Server, RabbitMQ, S3, and Worker."""

    _solve_job(
        e2e_environment=e2e_environment,
        solving_method=solving_method,
    )


def _solve_job(
    *,
    e2e_environment: E2eEnvironment,
    solving_method: str,
) -> None:
    upload_session_id = f"session-{solving_method}-{uuid4()}"
    web_server_url = e2e_environment.web_server_url
    job_id = _create_job(
        web_server_url=web_server_url,
        upload_session_id=upload_session_id,
    )

    matrix_upload_url = _request_input_upload_url(
        web_server_url=web_server_url,
        job_id=job_id,
        input_slot=_MATRIX_INPUT_SLOT,
    )
    rhs_upload_url = _request_input_upload_url(
        web_server_url=web_server_url,
        job_id=job_id,
        input_slot=_RHS_INPUT_SLOT,
    )

    _put_presigned_url(
        url=matrix_upload_url,
        body=_MATRIX_CSV.encode("utf-8"),
        content_type="text/csv",
        minio_container=e2e_environment.minio_container,
    )
    _put_presigned_url(
        url=rhs_upload_url,
        body=_RHS_CSV.encode("utf-8"),
        content_type="text/csv",
        minio_container=e2e_environment.minio_container,
    )

    _patch_job_params(
        web_server_url=web_server_url,
        job_id=job_id,
        solving_method=solving_method,
    )

    _submit_job(
        web_server_url=web_server_url,
        job_id=job_id,
        web_server_container=e2e_environment.web_server_container,
    )

    done_status = _wait_for_job_done(
        web_server_url=web_server_url,
        job_id=job_id,
    )

    assert str(done_status["jobId"]) == job_id

    if "jobType" in done_status:
        assert done_status["jobType"] == _JOB_TYPE

    if "progress" in done_status:
        assert int(done_status["progress"]) == 100

    solution_download_url = _request_output_download_url(
        web_server_url=web_server_url,
        job_id=job_id,
        output_slot=_SOLUTION_OUTPUT_SLOT,
    )

    solution_csv = _get_presigned_url(
        url=solution_download_url,
        minio_container=e2e_environment.minio_container,
    ).decode("utf-8")

    solution = _read_csv_vector(solution_csv)

    assert _vectors_are_close(
        solution,
        _EXPECTED_SOLUTION,
    )


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


def _new_postgres_container(e2e_network: Network) -> DockerContainer:
    return (
        DockerContainer(_POSTGRES_IMAGE)
        .with_network(e2e_network)
        .with_network_aliases(_POSTGRES_NETWORK_ALIAS)
        .with_env("POSTGRES_DB", _POSTGRES_DB)
        .with_env("POSTGRES_USER", _POSTGRES_USER)
        .with_env("POSTGRES_PASSWORD", _POSTGRES_PASSWORD)
        .with_env("TZ", "UTC")
        .with_exposed_ports(_POSTGRES_INTERNAL_PORT)
    )


def _new_redis_container(e2e_network: Network) -> DockerContainer:
    return (
        DockerContainer(_REDIS_IMAGE)
        .with_network(e2e_network)
        .with_network_aliases(_REDIS_NETWORK_ALIAS)
        .with_exposed_ports(_REDIS_INTERNAL_PORT)
    )


def _new_minio_container(e2e_network: Network) -> MinioContainer:
    return (
        MinioContainer(
            image=_MINIO_IMAGE,
            access_key=_S3_ACCESS_KEY,
            secret_key=_S3_SECRET_KEY,
        )
        .with_network(e2e_network)
        .with_network_aliases(_MINIO_NETWORK_ALIAS)
    )


def _new_web_server_container(e2e_network: Network) -> DockerContainer:
    return (
        DockerContainer(_WEB_SERVER_IMAGE)
        .with_network(e2e_network)
        .with_network_aliases(_WEB_SERVER_NETWORK_ALIAS)
        .with_command("java -jar /opt/mdds/mdds-web-server/mdds-web-server.jar")
        .with_env("MDDS_SERVER_HOST", _WEB_SERVER_NETWORK_ALIAS)
        .with_env("MDDS_SERVER_PORT", str(_WEB_SERVER_INTERNAL_PORT))
        .with_env(
            "MDDS_SERVER_WEBAPP_DIR_LOCATION",
            "/opt/mdds/mdds-web-server/web-app",
        )
        .with_env("MDDS_RABBITMQ_HOST", _RABBITMQ_NETWORK_ALIAS)
        .with_env("MDDS_RABBITMQ_PORT", str(_RABBITMQ_INTERNAL_PORT))
        .with_env("MDDS_RABBITMQ_USER", _RABBITMQ_USER)
        .with_env("MDDS_RABBITMQ_PASSWORD", _RABBITMQ_PASSWORD)
        .with_env("MDDS_REDIS_HOST", _REDIS_NETWORK_ALIAS)
        .with_env("MDDS_REDIS_PORT", str(_REDIS_INTERNAL_PORT))
        .with_env(
            "MDDS_METADATA_STORAGE_JDBC_URL",
            (
                f"jdbc:postgresql://{_POSTGRES_NETWORK_ALIAS}:"
                f"{_POSTGRES_INTERNAL_PORT}/{_POSTGRES_DB}"
            ),
        )
        .with_env("MDDS_METADATA_STORAGE_USER", _POSTGRES_USER)
        .with_env("MDDS_METADATA_STORAGE_PASSWORD", _POSTGRES_PASSWORD)
        .with_env("MDDS_OBJECT_STORAGE_BUCKET", _S3_BUCKET)
        .with_env("MDDS_OBJECT_STORAGE_INTERNAL_ENDPOINT", _S3_INTERNAL_ENDPOINT)
        .with_env("MDDS_OBJECT_STORAGE_REGION", _S3_REGION)
        .with_env("MDDS_OBJECT_STORAGE_ACCESS_KEY", _S3_ACCESS_KEY)
        .with_env("MDDS_OBJECT_STORAGE_SECRET_KEY", _S3_SECRET_KEY)
        .with_env("MDDS_OBJECT_STORAGE_PATH_STYLE_ACCESS_ENABLED", "true")
        .with_exposed_ports(_WEB_SERVER_INTERNAL_PORT)
    )


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


def _host_port_endpoint(
    container: DockerContainer,
    internal_port: int,
) -> HostPortEndpoint:
    return HostPortEndpoint(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(internal_port)),
    )


def _web_server_url(web_server_container: DockerContainer) -> str:
    endpoint = _host_port_endpoint(web_server_container, _WEB_SERVER_INTERNAL_PORT)
    return f"http://{endpoint.host}:{endpoint.port}"


def _worker_health_url(worker_container: DockerContainer) -> str:
    endpoint = _host_port_endpoint(worker_container, _WORKER_HEALTH_PORT)
    return f"http://{endpoint.host}:{endpoint.port}/health"


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


def _wait_for_tcp_endpoint(
    endpoint: HostPortEndpoint,
    *,
    description: str,
    timeout_seconds: float,
) -> None:
    def tcp_endpoint_is_ready() -> bool:
        try:
            with socket.create_connection(
                (endpoint.host, endpoint.port),
                timeout=1.0,
            ):
                return True
        except OSError:
            return False

    _wait_until(
        tcp_endpoint_is_ready,
        description=description,
        timeout_seconds=timeout_seconds,
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


def _wait_for_web_server_health(
    web_server_url: str,
    web_server_container: DockerContainer,
) -> None:
    health_url = f"{web_server_url}/health"

    try:
        _wait_for_http_ok(
            url=health_url,
            description="Web Server /health readiness",
            timeout_seconds=_WEB_SERVER_HEALTH_TIMEOUT_SECONDS,
        )
    except AssertionError as error:
        raise AssertionError(
            f"{error}\n\nWeb Server container logs:\n"
            f"{_container_logs(web_server_container)}"
        ) from error


def _wait_for_worker_health(
    worker_health_url: str,
    worker_container: DockerContainer,
) -> None:
    try:
        _wait_for_http_ok(
            url=worker_health_url,
            description="SLAE Worker /health readiness",
            timeout_seconds=_WORKER_HEALTH_TIMEOUT_SECONDS,
        )
    except AssertionError as error:
        raise AssertionError(
            f"{error}\n\nSLAE Worker container logs:\n"
            f"{_container_logs(worker_container)}"
        ) from error


def _wait_for_http_ok(
    *,
    url: str,
    description: str,
    timeout_seconds: float,
) -> None:
    def http_is_ok() -> bool:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                return response.status == 200
        except (OSError, urllib.error.URLError):
            return False

    _wait_until(
        http_is_ok,
        description=description,
        timeout_seconds=timeout_seconds,
    )


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


def _create_job(
    *,
    web_server_url: str,
    upload_session_id: str,
) -> str:
    response = _request_json(
        method="POST",
        url=f"{web_server_url}/jobs",
        headers={
            "X-MDDS-User-Login": _USER_LOGIN,
            "X-MDDS-Upload-Session-Id": upload_session_id,
            "Content-Type": "application/json",
        },
        body={
            "jobType": _JOB_TYPE,
        },
        expected_statuses={200, 201},
    )

    job_id = str(response.get("jobId", "")).strip()

    assert job_id != ""

    return job_id


def _request_input_upload_url(
    *,
    web_server_url: str,
    job_id: str,
    input_slot: str,
) -> str:
    response = _request_json(
        method="POST",
        url=f"{web_server_url}/jobs/{job_id}/inputs",
        headers={
            "X-MDDS-User-Login": _USER_LOGIN,
            "Content-Type": "application/json",
        },
        body={
            "inputSlot": input_slot,
        },
        expected_statuses={200},
    )

    assert str(response.get("jobId", "")) == job_id

    upload_url = str(response.get("uploadUrl", "")).strip()
    expires_at = str(response.get("expiresAt", "")).strip()

    assert upload_url != ""
    assert expires_at != ""

    return upload_url


def _patch_job_params(
    *,
    web_server_url: str,
    job_id: str,
    solving_method: str,
) -> None:
    _request_json(
        method="PATCH",
        url=f"{web_server_url}/jobs/{job_id}/params",
        headers={
            "X-MDDS-User-Login": _USER_LOGIN,
            "Content-Type": "application/merge-patch+json",
        },
        body={
            "solvingMethod": solving_method,
        },
        expected_statuses={200},
        allow_empty_body=True,
    )


def _submit_job(
    *,
    web_server_url: str,
    job_id: str,
    web_server_container: DockerContainer,
) -> None:
    try:
        response = _request_json(
            method="POST",
            url=f"{web_server_url}/jobs/{job_id}/submit",
            headers={
                "X-MDDS-User-Login": _USER_LOGIN,
            },
            body=None,
            expected_statuses={202},
        )
    except AssertionError as error:
        raise AssertionError(
            f"{error}\n\nWeb Server container logs:\n"
            f"{_container_logs(web_server_container)}"
        ) from error

    assert str(response.get("jobId", "")) == job_id
    assert response.get("status") == "SUBMITTED"


def _wait_for_job_done(
    *,
    web_server_url: str,
    job_id: str,
) -> dict[str, Any]:
    last_status: dict[str, Any] | None = None
    deadline = time.monotonic() + _JOB_DONE_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        status = _request_json(
            method="GET",
            url=f"{web_server_url}/jobs/{job_id}/status",
            headers={
                "X-MDDS-User-Login": _USER_LOGIN,
            },
            body=None,
            expected_statuses={200},
        )

        last_status = status

        if str(status.get("jobId", "")) != job_id:
            raise AssertionError(
                "Unexpected jobId returned by status endpoint: "
                f"expected={job_id!r}, actual={status.get('jobId')!r}"
            )

        status_name = str(status.get("status", ""))

        if status_name == "DONE":
            return status

        if status_name in {"ERROR", "CANCELLED"}:
            raise AssertionError(
                "Job reached unexpected terminal status: "
                f"jobId={job_id!r}, status={status_name!r}, "
                f"message={status.get('message')!r}"
            )

        time.sleep(0.5)

    raise AssertionError(
        "Job did not reach DONE before timeout: "
        f"jobId={job_id!r}, lastStatus={last_status!r}"
    )


def _request_output_download_url(
    *,
    web_server_url: str,
    job_id: str,
    output_slot: str,
) -> str:
    response = _request_json(
        method="GET",
        url=f"{web_server_url}/jobs/{job_id}/outputs?outputSlot={output_slot}",
        headers={
            "X-MDDS-User-Login": _USER_LOGIN,
        },
        body=None,
        expected_statuses={200},
    )

    assert str(response.get("jobId", "")) == job_id

    download_url = str(response.get("downloadUrl", "")).strip()

    assert download_url != ""

    return download_url


def _request_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    body: object | None,
    expected_statuses: set[int],
    allow_empty_body: bool = False,
) -> dict[str, Any]:
    request_body = None

    if body is not None:
        request_body = json.dumps(body).encode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=request_body,
        headers=headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response_body = response.read()

            assert response.status in expected_statuses, (
                f"Unexpected HTTP status for {method} {url}: "
                f"actual={response.status}, expected={expected_statuses}, "
                f"body={response_body.decode('utf-8', errors='replace')!r}"
            )

            if allow_empty_body and len(response_body) == 0:
                return {}

            decoded = json.loads(response_body.decode("utf-8"))

            if not isinstance(decoded, dict):
                raise AssertionError(f"Expected JSON object, got: {decoded!r}")

            return decoded

    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise AssertionError(
            f"HTTP request failed: {method} {url}, "
            f"status={error.code}, body={error_body!r}"
        ) from error


def _put_presigned_url(
    *,
    url: str,
    body: bytes,
    content_type: str,
    minio_container: MinioContainer,
) -> None:
    request_url, host_header = _host_accessible_presigned_url(
        url=url,
        minio_container=minio_container,
    )

    headers = {
        "Content-Type": content_type,
    }

    if host_header is not None:
        headers["Host"] = host_header

    request = urllib.request.Request(
        url=request_url,
        data=body,
        headers=headers,
        method="PUT",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status in {200, 201, 204}
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise AssertionError(
            f"Presigned PUT failed: status={error.code}, body={error_body!r}"
        ) from error


def _get_presigned_url(
    *,
    url: str,
    minio_container: MinioContainer,
) -> bytes:
    request_url, host_header = _host_accessible_presigned_url(
        url=url,
        minio_container=minio_container,
    )

    headers = {}

    if host_header is not None:
        headers["Host"] = host_header

    request = urllib.request.Request(
        url=request_url,
        headers=headers,
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status == 200
            return response.read()
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise AssertionError(
            f"Presigned GET failed: status={error.code}, body={error_body!r}"
        ) from error


def _host_accessible_presigned_url(
    *,
    url: str,
    minio_container: MinioContainer,
) -> tuple[str, str | None]:
    parsed = urlparse(url)

    if parsed.hostname not in {
        _MINIO_NETWORK_ALIAS,
        "localhost",
        "127.0.0.1",
    }:
        return url, None

    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)

    rewritten = _replace_url_netloc(
        parsed,
        new_netloc=f"{host}:{port}",
    )

    # Preserve the original Host header because SigV4 signs the host value.
    return rewritten, parsed.netloc


def _replace_url_netloc(
    parsed: ParseResult,
    *,
    new_netloc: str,
) -> str:
    return parsed._replace(netloc=new_netloc).geturl()


def _read_csv_vector(content: str) -> list[float]:
    values: list[float] = []

    for row in csv.reader(io.StringIO(content)):
        if len(row) == 0 or all(cell.strip() == "" for cell in row):
            continue

        if len(row) != 1:
            raise AssertionError(f"Expected one value per solution row, got: {row}")

        values.append(float(row[0]))

    return values


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
