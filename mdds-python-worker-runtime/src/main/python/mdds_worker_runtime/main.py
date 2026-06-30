# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Python Worker Runtime entry point.

This module is the composition root for Python Worker Runtime.

It wires generic runtime infrastructure:

- worker configuration;
- RabbitMQ queue client;
- S3 object storage client;
- manifest loading;
- dynamic JobHandler loading;
- execution registry;
- status publishing;
- submitted job consumer;
- cancellation consumer;
- execution watcher;
- cleanup watcher;
- timeout watcher.

The module intentionally does not contain job-specific business logic.
Concrete job behavior is delegated to the dynamically loaded JobHandler.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import signal
import threading
from types import FrameType
from typing import Any

from mdds_worker_runtime.config import load_config
from mdds_worker_runtime.dto.messages import CancelJobDTO, JobMessageDTO
from mdds_worker_runtime.execution.artifacts import InputArtifactPreparer
from mdds_worker_runtime.execution.cancel_consumer import CancelConsumer
from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.execution.cleanup_watcher import CleanupWatcher
from mdds_worker_runtime.execution.context import JobExecutionContextFactory
from mdds_worker_runtime.execution.execution_watcher import ExecutionWatcher
from mdds_worker_runtime.execution.handler_loader import JobHandlerLoader
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.execution.output_artifact_uploader import (
    OutputArtifactUploader,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.supervisor import ExecutionSupervisor
from mdds_worker_runtime.execution.timeout_watcher import TimeoutWatcher
from mdds_worker_runtime.execution.validation_handler import ValidationHandler
from mdds_worker_runtime.logging_config import setup_logging
from mdds_worker_runtime.manifest.loader import ManifestLoader
from mdds_worker_runtime.rabbitmq import RabbitMqProperties, RabbitMqQueueClient
from mdds_worker_runtime.storage.s3_client import S3Storage
from mdds_worker_runtime.storage.s3_factory import (
    Boto3S3ClientFactory,
    S3Properties,
)

logger = logging.getLogger(__name__)


class WorkerRuntime:
    """Coordinates Worker Runtime lifecycle.

    WorkerRuntime owns runtime startup and shutdown ordering.

    The class is intentionally separated from main() so runtime composition can be
    unit-tested with mocks without starting real RabbitMQ or S3 containers.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        worker_job_queue_name: str,
        worker_cancel_queue_name: str,
        queue_client: Any,
        job_consumer: JobConsumer,
        cancel_consumer: CancelConsumer,
        execution_watcher: ExecutionWatcher,
        cleanup_watcher: CleanupWatcher,
        timeout_watcher: TimeoutWatcher,
        closeables: list[Any] | None = None,
    ) -> None:
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")
        if worker_job_queue_name is None or worker_job_queue_name.strip() == "":
            raise ValueError("worker_job_queue_name cannot be null or blank.")
        if worker_cancel_queue_name is None or worker_cancel_queue_name.strip() == "":
            raise ValueError("worker_cancel_queue_name cannot be null or blank.")
        if queue_client is None:
            raise ValueError("queue_client cannot be null.")
        if job_consumer is None:
            raise ValueError("job_consumer cannot be null.")
        if cancel_consumer is None:
            raise ValueError("cancel_consumer cannot be null.")
        if execution_watcher is None:
            raise ValueError("execution_watcher cannot be null.")
        if cleanup_watcher is None:
            raise ValueError("cleanup_watcher cannot be null.")
        if timeout_watcher is None:
            raise ValueError("timeout_watcher cannot be null.")

        self._worker_id = worker_id.strip()
        self._worker_job_queue_name = worker_job_queue_name.strip()
        self._worker_cancel_queue_name = worker_cancel_queue_name.strip()

        self._queue_client = queue_client
        self._job_consumer = job_consumer
        self._cancel_consumer = cancel_consumer

        self._execution_watcher = execution_watcher
        self._cleanup_watcher = cleanup_watcher
        self._timeout_watcher = timeout_watcher

        self._closeables = closeables or []

        self._job_subscription: Any | None = None
        self._cancel_subscription: Any | None = None
        self._started = False
        self._resources_closed = False

    def start(self) -> None:
        """Start Worker Runtime services and queue subscriptions.

        Startup is idempotent. Calling start() more than once has no effect after
        the runtime has successfully started.

        Cancellation subscription is intentionally created before job
        subscription. This avoids a startup window where the worker can claim a
        job but is not yet listening for targeted cancellation messages.
        """
        if self._started:
            logger.info(
                "Worker Runtime start ignored because it is already started.",
                extra={
                    "component": "worker_runtime",
                    "event": "worker_runtime_start_ignored",
                    "workerId": self._worker_id,
                },
            )
            return

        try:
            self._start_watchers()
            self._subscribe_to_cancellation_queue()
            self._subscribe_to_job_queue()
            self._started = True
        except Exception:
            logger.exception(
                "Worker Runtime startup failed; cleaning up partially started resources.",
                extra={
                    "component": "worker_runtime",
                    "event": "worker_runtime_start_failed",
                    "workerId": self._worker_id,
                },
            )
            self.stop()
            raise

        logger.info(
            "Worker Runtime started.",
            extra={
                "component": "worker_runtime",
                "event": "worker_runtime_started",
                "workerId": self._worker_id,
                "jobQueueName": self._worker_job_queue_name,
                "cancelQueueName": self._worker_cancel_queue_name,
            },
        )

    def stop(self) -> None:
        """Stop Worker Runtime subscriptions, watchers, and owned resources.

        Shutdown is idempotent and safe even if start() failed halfway through.
        """
        self._close_subscription("_job_subscription")
        self._close_subscription("_cancel_subscription")

        self._stop_runtime_service(self._timeout_watcher, "timeout_watcher")
        self._stop_runtime_service(self._cleanup_watcher, "cleanup_watcher")
        self._stop_runtime_service(self._execution_watcher, "execution_watcher")

        self._close_owned_resources_once()

        self._started = False

        logger.info(
            "Worker Runtime stopped.",
            extra={
                "component": "worker_runtime",
                "event": "worker_runtime_stopped",
                "workerId": self._worker_id,
            },
        )

    def _close_owned_resources_once(self) -> None:
        if self._resources_closed:
            return

        for closeable in self._closeables:
            _close_if_supported(closeable)

        self._resources_closed = True

    def _start_watchers(self) -> None:
        self._execution_watcher.start()
        self._cleanup_watcher.start()
        self._timeout_watcher.start()

    def _subscribe_to_cancellation_queue(self) -> None:
        self._cancel_subscription = self._queue_client.subscribe(
            self._worker_cancel_queue_name,
            CancelJobDTO,
            self._cancel_consumer,
        )

    def _subscribe_to_job_queue(self) -> None:
        self._job_subscription = self._queue_client.subscribe(
            self._worker_job_queue_name,
            JobMessageDTO,
            self._job_consumer,
        )

    def _close_subscription(self, attribute_name: str) -> None:
        subscription = getattr(self, attribute_name)
        if subscription is None:
            return

        _close_if_supported(subscription)
        setattr(self, attribute_name, None)

    @staticmethod
    def _stop_runtime_service(service: Any, service_name: str) -> None:
        try:
            service.stop()
        except Exception:
            logger.exception(
                "Failed to stop Worker Runtime service.",
                extra={
                    "component": "worker_runtime",
                    "event": "worker_runtime_service_stop_failed",
                    "service": service_name,
                },
            )


def build_worker_runtime_from_environment() -> WorkerRuntime:
    """Build WorkerRuntime using environment-backed WorkerConfig.

    This function is intentionally kept separate from main() so tests can patch
    factories and verify composition without installing signal handlers or
    blocking on shutdown events.
    """
    worker_config = load_config()

    def clock() -> datetime:
        return datetime.now(timezone.utc)

    rabbitmq_properties = RabbitMqProperties(
        host=worker_config.rabbitmq_host,
        port=worker_config.rabbitmq_port,
        user=worker_config.rabbitmq_user,
        password=worker_config.rabbitmq_password,
    )
    queue_client = RabbitMqQueueClient(rabbitmq_properties)
    queue_client.check_readiness()
    queue_client.check_messaging_readiness()

    s3_properties = S3Properties(
        endpoint_url=worker_config.object_storage_endpoint_url,
        bucket=worker_config.object_storage_bucket,
        access_key=worker_config.object_storage_access_key,
        secret_key=worker_config.object_storage_secret_key,
        region=worker_config.object_storage_region,
        path_style_access_enabled=worker_config.object_storage_path_style_access_enabled,
    )

    boto3_client = Boto3S3ClientFactory(s3_properties).create()
    storage = S3Storage(boto3_client, s3_properties.bucket)
    storage.check_readiness()

    jobs_root = worker_config.jobs_root
    handler_import_path = worker_config.worker_handler
    worker_id = worker_config.worker_id

    manifest_loader = ManifestLoader(storage)
    input_artifact_preparer = InputArtifactPreparer(storage, jobs_root)
    job_execution_context_factory = JobExecutionContextFactory(jobs_root)
    job_handler_loader = JobHandlerLoader(handler_import_path)
    job_handler_loader.validate_loadable()
    execution_supervisor = ExecutionSupervisor(jobs_root, handler_import_path)
    execution_registry = ExecutionRegistry()

    status_publisher = StatusPublisher(
        worker_status_queue_name=worker_config.worker_status_queue_name,
        queue_client=queue_client,
        clock=clock,
    )

    output_artifact_uploader = OutputArtifactUploader(storage)

    validation_handler = ValidationHandler(status_publisher, worker_id)

    job_consumer = JobConsumer(
        manifest_loader,
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
        worker_id,
    )

    cancellation_request_handler = CancellationRequestHandler(
        execution_registry=execution_registry,
        status_publisher=status_publisher,
        worker_id=worker_id,
        clock=clock,
    )
    cancel_consumer = CancelConsumer(cancellation_request_handler, worker_id)

    execution_watcher = ExecutionWatcher(
        execution_registry=execution_registry,
        output_artifact_uploader=output_artifact_uploader,
        status_publisher=status_publisher,
        worker_id=worker_id,
        job_timeout_seconds=worker_config.worker_job_timeout_seconds,
        progress_interval_seconds=worker_config.worker_progress_interval_seconds,
        clock=clock,
    )

    cleanup_watcher = CleanupWatcher(
        execution_registry=execution_registry,
        worker_id=worker_id,
        cleanup_interval_seconds=worker_config.worker_cleanup_interval_seconds,
    )

    timeout_watcher = TimeoutWatcher(
        execution_registry=execution_registry,
        status_publisher=status_publisher,
        worker_id=worker_id,
        job_timeout_seconds=worker_config.worker_job_timeout_seconds,
        clock=clock,
    )

    return WorkerRuntime(
        worker_id=worker_id,
        worker_job_queue_name=worker_config.worker_job_queue_name,
        worker_cancel_queue_name=worker_config.worker_cancel_queue_name,
        queue_client=queue_client,
        job_consumer=job_consumer,
        cancel_consumer=cancel_consumer,
        execution_watcher=execution_watcher,
        cleanup_watcher=cleanup_watcher,
        timeout_watcher=timeout_watcher,
        closeables=[
            queue_client,
            storage,
            boto3_client,
        ],
    )


def main() -> int:
    """Run Python Worker Runtime until SIGINT or SIGTERM is received."""
    setup_logging()

    runtime = build_worker_runtime_from_environment()

    try:
        runtime.start()
        _wait_until_shutdown_signal()

        logger.info(
            "Worker Runtime shutdown requested.",
            extra={
                "component": "worker_runtime",
                "event": "worker_runtime_shutdown_requested",
            },
        )
        return 0
    finally:
        runtime.stop()


def _wait_until_shutdown_signal() -> None:
    shutdown_requested = threading.Event()

    def request_shutdown(signum: int, _frame: FrameType | None) -> None:
        logger.info(
            "Shutdown signal received.",
            extra={
                "component": "worker_runtime",
                "event": "shutdown_signal_received",
                "signal": signum,
            },
        )
        shutdown_requested.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    shutdown_requested.wait()


def _close_if_supported(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if close is None:
        return

    try:
        close()
    except Exception:
        logger.exception(
            "Failed to close Worker Runtime resource.",
            extra={
                "component": "worker_runtime",
                "event": "worker_runtime_resource_close_failed",
            },
        )


if __name__ == "__main__":
    raise SystemExit(main())
