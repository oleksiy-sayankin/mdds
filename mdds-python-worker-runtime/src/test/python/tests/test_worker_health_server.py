# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Unit tests for WorkerHealthServer."""

from __future__ import annotations

import http.client
from unittest.mock import Mock

import pytest

from mdds_worker_runtime.worker_health_server import WorkerHealthServer

_HOST = "127.0.0.1"
_REQUEST_TIMEOUT_SECONDS = 2


def test_get_health_returns_200_and_ok_body() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    try:
        health_server.start()

        response_status, response_body = _request(health_server, "GET", "/health")

        assert response_status == http.client.OK
        assert response_body == b"OK\n"
    finally:
        health_server.stop()


def test_head_health_returns_200_and_empty_body() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    try:
        health_server.start()

        response_status, response_body = _request(health_server, "HEAD", "/health")

        assert response_status == http.client.OK
        assert response_body == b""
    finally:
        health_server.stop()


def test_get_unknown_returns_404_and_not_found_body() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    try:
        health_server.start()

        response_status, response_body = _request(health_server, "GET", "/unknown")

        assert response_status == http.client.NOT_FOUND
        assert response_body == b"Not Found\n"
    finally:
        health_server.stop()


def test_head_unknown_returns_404_and_empty_body() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    try:
        health_server.start()

        response_status, response_body = _request(health_server, "HEAD", "/unknown")

        assert response_status == http.client.NOT_FOUND
        assert response_body == b""
    finally:
        health_server.stop()


def test_start_is_idempotent() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    try:
        health_server.start()
        first_server = health_server._server
        first_thread = health_server._thread

        health_server.start()

        assert health_server._server is first_server
        assert health_server._thread is first_thread
    finally:
        health_server.stop()


def test_stop_before_start_is_safe() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    health_server.stop()
    health_server.stop()


def test_stop_after_start_is_safe() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    health_server.start()
    health_server.stop()
    health_server.stop()


@pytest.mark.parametrize("host", ["", "   "])
def test_constructor_rejects_blank_host(host: str) -> None:
    with pytest.raises(ValueError, match="host cannot be null or blank"):
        WorkerHealthServer(host=host, port=0)


def test_constructor_rejects_null_host() -> None:
    with pytest.raises(ValueError, match="host cannot be null or blank"):
        WorkerHealthServer(host=None, port=0)  # type: ignore[arg-type]


def test_constructor_rejects_port_below_zero() -> None:
    with pytest.raises(ValueError, match="port must be between 0 and 65535"):
        WorkerHealthServer(host=_HOST, port=-1)


def test_constructor_rejects_port_above_65535() -> None:
    with pytest.raises(ValueError, match="port must be between 0 and 65535"):
        WorkerHealthServer(host=_HOST, port=65536)


def test_stop_suppresses_shutdown_server_close_and_join_errors() -> None:
    health_server = WorkerHealthServer(host=_HOST, port=0)

    server = Mock()
    server.shutdown.side_effect = RuntimeError("shutdown failed")
    server.server_close.side_effect = RuntimeError("server close failed")

    thread = Mock()
    thread.join.side_effect = RuntimeError("join failed")

    health_server._server = server
    health_server._thread = thread
    health_server._started = True

    health_server.stop()

    server.shutdown.assert_called_once_with()
    server.server_close.assert_called_once_with()
    thread.join.assert_called_once_with(timeout=5)

    assert health_server._server is None
    assert health_server._thread is None
    assert health_server._started is False


def _request(
    health_server: WorkerHealthServer,
    method: str,
    path: str,
) -> tuple[int, bytes]:
    port = _started_server_port(health_server)

    connection = http.client.HTTPConnection(
        _HOST,
        port,
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    try:
        connection.request(method, path)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def _started_server_port(health_server: WorkerHealthServer) -> int:
    server = health_server._server

    if server is None:
        raise AssertionError("WorkerHealthServer is not started.")

    return int(server.server_address[1])
