# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""HTTP health endpoint for Python Worker Runtime.

The server is intentionally minimal and uses only the Python standard library.

It is expected to be started only after WorkerRuntime.start() completes
successfully. Therefore, HTTP 200 from /health means that the Worker Runtime
has completed startup and is ready from the process readiness perspective.
"""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)


class WorkerHealthServer:
    """Runs a small HTTP server exposing Worker Runtime health endpoint."""

    def __init__(self, *, host: str, port: int) -> None:
        if host is None or host.strip() == "":
            raise ValueError("host cannot be null or blank.")
        if port < 0 or port > 65535:
            raise ValueError("port must be between 0 and 65535.")

        self._host = host.strip()
        self._port = port

        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._started = False

    def start(self) -> None:
        """Start the health HTTP server.

        Startup is idempotent. Calling start() more than once has no effect
        after the server has successfully started.
        """
        with self._lock:
            if self._started:
                logger.info(
                    "Worker health server start ignored because it is already started.",
                    extra={
                        "component": "worker_health_server",
                        "event": "worker_health_server_start_ignored",
                        "host": self._host,
                        "port": self._port,
                    },
                )
                return

            server = _ReusableThreadingHTTPServer(
                (self._host, self._port),
                _WorkerHealthRequestHandler,
            )

            thread = threading.Thread(
                target=server.serve_forever,
                name="worker-health-server",
                daemon=True,
            )
            thread.start()

            self._server = server
            self._thread = thread
            self._started = True

        logger.info(
            "Worker health server started.",
            extra={
                "component": "worker_health_server",
                "event": "worker_health_server_started",
                "host": self._host,
                "port": self._port,
            },
        )

    def stop(self) -> None:
        """Stop the health HTTP server.

        Shutdown is idempotent and best-effort. Cleanup failures are logged but are
        not propagated because Worker Runtime shutdown must continue.
        """
        with self._lock:
            if not self._started:
                return

            server = self._server
            thread = self._thread

            self._server = None
            self._thread = None
            self._started = False

        self._shutdown_server(server)
        self._close_server(server)
        self._join_thread(thread)

        logger.info(
            "Worker health server stopped.",
            extra={
                "component": "worker_health_server",
                "event": "worker_health_server_stopped",
                "host": self._host,
                "port": self._port,
            },
        )

    def _shutdown_server(self, server: ThreadingHTTPServer | None) -> None:
        if server is None:
            return

        try:
            server.shutdown()
        except Exception:
            logger.exception(
                "Failed to shutdown Worker health server.",
                extra={
                    "component": "worker_health_server",
                    "event": "worker_health_server_shutdown_failed",
                    "host": self._host,
                    "port": self._port,
                },
            )

    def _close_server(self, server: ThreadingHTTPServer | None) -> None:
        if server is None:
            return

        try:
            server.server_close()
        except Exception:
            logger.exception(
                "Failed to close Worker health server.",
                extra={
                    "component": "worker_health_server",
                    "event": "worker_health_server_close_failed",
                    "host": self._host,
                    "port": self._port,
                },
            )

    def _join_thread(self, thread: threading.Thread | None) -> None:
        if thread is None:
            return

        try:
            thread.join(timeout=5)
        except Exception:
            logger.exception(
                "Failed to join Worker health server thread.",
                extra={
                    "component": "worker_health_server",
                    "event": "worker_health_server_thread_join_failed",
                    "host": self._host,
                    "port": self._port,
                },
            )


class _ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class _WorkerHealthRequestHandler(BaseHTTPRequestHandler):
    server_version = "MDDSWorkerHealthServer/1.0"

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path != "/health":
            self._send_not_found()
            return

        self._send_ok()

    def do_HEAD(self) -> None:
        """Handle HEAD requests."""
        if self.path != "/health":
            self._send_not_found(include_body=False)
            return

        self._send_ok(include_body=False)

    def log_message(self, format: str, *args: Any) -> None:
        """Route HTTP access logs through the Worker Runtime logger."""
        logger.info(
            format,
            *args,
            extra={
                "component": "worker_health_server",
                "event": "worker_health_server_http_request",
                "host": self.client_address[0],
                "port": self.client_address[1],
            },
        )

    def _send_ok(self, *, include_body: bool = True) -> None:
        body = b"OK\n"

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body) if include_body else 0))
        self.end_headers()

        if include_body:
            self.wfile.write(body)

    def _send_not_found(self, *, include_body: bool = True) -> None:
        body = b"Not Found\n"

        self.send_response(HTTPStatus.NOT_FOUND)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body) if include_body else 0))
        self.end_headers()

        if include_body:
            self.wfile.write(body)
