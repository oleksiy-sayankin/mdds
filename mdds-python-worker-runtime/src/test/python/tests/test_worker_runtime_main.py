# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import runpy
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, call

import pytest

from mdds_worker_runtime import main as worker_main
from mdds_worker_runtime.dto.messages import CancelJobDTO, JobMessageDTO
from mdds_worker_runtime.execution.cancel_consumer import CancelConsumer
from mdds_worker_runtime.execution.cleanup_watcher import CleanupWatcher
from mdds_worker_runtime.execution.execution_watcher import ExecutionWatcher
from mdds_worker_runtime.execution.handler_loader import JobHandlerLoadError
from mdds_worker_runtime.execution.job_consumer import JobConsumer
from mdds_worker_runtime.execution.timeout_watcher import TimeoutWatcher
from mdds_worker_runtime.main import WorkerRuntime
from mdds_worker_runtime.rabbitmq import RabbitMqConnectionError
from mdds_worker_runtime.storage.s3_client import S3StorageReadinessError

WORKER_ID = "worker-1"
JOB_QUEUE_NAME = "queue-two_numbers_sum"
CANCEL_QUEUE_NAME = "cancel.queue-worker-1"


@pytest.mark.parametrize(
    ("field_name", "field_value", "error_message"),
    [
        ("worker_id", None, "worker_id cannot be null or blank."),
        ("worker_id", "", "worker_id cannot be null or blank."),
        ("worker_id", " ", "worker_id cannot be null or blank."),
        (
            "worker_job_queue_name",
            None,
            "worker_job_queue_name cannot be null or blank.",
        ),
        (
            "worker_job_queue_name",
            "",
            "worker_job_queue_name cannot be null or blank.",
        ),
        (
            "worker_job_queue_name",
            " ",
            "worker_job_queue_name cannot be null or blank.",
        ),
        (
            "worker_cancel_queue_name",
            None,
            "worker_cancel_queue_name cannot be null or blank.",
        ),
        (
            "worker_cancel_queue_name",
            "",
            "worker_cancel_queue_name cannot be null or blank.",
        ),
        (
            "worker_cancel_queue_name",
            " ",
            "worker_cancel_queue_name cannot be null or blank.",
        ),
        ("queue_client", None, "queue_client cannot be null."),
        ("job_consumer", None, "job_consumer cannot be null."),
        ("cancel_consumer", None, "cancel_consumer cannot be null."),
        ("execution_watcher", None, "execution_watcher cannot be null."),
        ("cleanup_watcher", None, "cleanup_watcher cannot be null."),
        ("timeout_watcher", None, "timeout_watcher cannot be null."),
    ],
)
def test_worker_runtime_constructor_validation(
    field_name: str,
    field_value: Any,
    error_message: str,
) -> None:
    kwargs = _runtime_kwargs()
    kwargs[field_name] = field_value

    with pytest.raises(ValueError, match=error_message):
        WorkerRuntime(**kwargs)


def test_worker_runtime_start_starts_watchers() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()

    assert fixture.execution_watcher.start_count == 1
    assert fixture.cleanup_watcher.start_count == 1
    assert fixture.timeout_watcher.start_count == 1
    assert fixture.events[:3] == [
        "start:execution_watcher",
        "start:cleanup_watcher",
        "start:timeout_watcher",
    ]


def test_worker_runtime_start_subscribes_cancel_queue_before_job_queue() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()

    assert fixture.queue_client.subscribe_calls == [
        (CANCEL_QUEUE_NAME, CancelJobDTO, fixture.cancel_consumer),
        (JOB_QUEUE_NAME, JobMessageDTO, fixture.job_consumer),
    ]


def test_worker_runtime_start_stores_subscription_handles() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()

    assert (
        fixture.runtime._cancel_subscription
        is fixture.queue_client.subscriptions[CANCEL_QUEUE_NAME]
    )
    assert (
        fixture.runtime._job_subscription
        is fixture.queue_client.subscriptions[JOB_QUEUE_NAME]
    )


def test_worker_runtime_start_is_idempotent() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()
    fixture.runtime.start()

    assert fixture.execution_watcher.start_count == 1
    assert fixture.cleanup_watcher.start_count == 1
    assert fixture.timeout_watcher.start_count == 1
    assert len(fixture.queue_client.subscribe_calls) == 2


def test_worker_runtime_stop_closes_job_subscription() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()
    fixture.runtime.stop()

    job_subscription = fixture.queue_client.subscriptions[JOB_QUEUE_NAME]

    assert job_subscription.close_count == 1
    assert fixture.runtime._job_subscription is None


def test_worker_runtime_stop_closes_cancel_subscription() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()
    fixture.runtime.stop()

    cancel_subscription = fixture.queue_client.subscriptions[CANCEL_QUEUE_NAME]

    assert cancel_subscription.close_count == 1
    assert fixture.runtime._cancel_subscription is None


def test_worker_runtime_stop_stops_watchers() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()
    fixture.runtime.stop()

    assert fixture.timeout_watcher.stop_count == 1
    assert fixture.cleanup_watcher.stop_count == 1
    assert fixture.execution_watcher.stop_count == 1

    assert "stop:timeout_watcher" in fixture.events
    assert "stop:cleanup_watcher" in fixture.events
    assert "stop:execution_watcher" in fixture.events


def test_worker_runtime_stop_closes_owned_resources_only_once() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.start()
    fixture.runtime.stop()
    fixture.runtime.stop()

    assert fixture.queue_client.close_count == 1
    assert fixture.storage.close_count == 1
    assert fixture.boto3_client.close_count == 1


def test_worker_runtime_stop_before_start_is_safe() -> None:
    fixture = _runtime_fixture()

    fixture.runtime.stop()

    assert fixture.runtime._job_subscription is None
    assert fixture.runtime._cancel_subscription is None


def test_worker_runtime_start_rolls_back_when_cancel_subscription_fails() -> None:
    fixture = _runtime_fixture(
        queue_client=_QueueClientFake(
            fail_on_queue=CANCEL_QUEUE_NAME,
        )
    )

    with pytest.raises(RuntimeError, match="subscribe failed"):
        fixture.runtime.start()

    assert fixture.execution_watcher.start_count == 1
    assert fixture.cleanup_watcher.start_count == 1
    assert fixture.timeout_watcher.start_count == 1

    assert fixture.execution_watcher.stop_count == 1
    assert fixture.cleanup_watcher.stop_count == 1
    assert fixture.timeout_watcher.stop_count == 1

    assert fixture.runtime._cancel_subscription is None
    assert fixture.runtime._job_subscription is None
    assert fixture.runtime._started is False

    assert fixture.queue_client.close_count == 1
    assert fixture.storage.close_count == 1
    assert fixture.boto3_client.close_count == 1


def test_worker_runtime_start_rolls_back_when_job_subscription_fails() -> None:
    fixture = _runtime_fixture(
        queue_client=_QueueClientFake(
            fail_on_queue=JOB_QUEUE_NAME,
        )
    )

    with pytest.raises(RuntimeError, match="subscribe failed"):
        fixture.runtime.start()

    cancel_subscription = fixture.queue_client.subscriptions[CANCEL_QUEUE_NAME]

    assert cancel_subscription.close_count == 1
    assert fixture.runtime._cancel_subscription is None
    assert fixture.runtime._job_subscription is None
    assert fixture.runtime._started is False

    assert fixture.execution_watcher.stop_count == 1
    assert fixture.cleanup_watcher.stop_count == 1
    assert fixture.timeout_watcher.stop_count == 1

    assert fixture.queue_client.close_count == 1
    assert fixture.storage.close_count == 1
    assert fixture.boto3_client.close_count == 1


def test_build_worker_runtime_from_environment_wires_required_components(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    worker_config = SimpleNamespace(
        rabbitmq_host="rabbitmq",
        rabbitmq_port=5672,
        rabbitmq_user="mdds",
        rabbitmq_password="secret",
        object_storage_endpoint_url="http://minio:9000",
        object_storage_bucket="mdds",
        object_storage_access_key="minioadmin",
        object_storage_secret_key="minioadmin",
        object_storage_region="us-east-1",
        object_storage_path_style_access_enabled=True,
        jobs_root=tmp_path,
        worker_handler="tests.fixtures.job_handlers:TwoNumbersSumJobHandler",
        worker_id=WORKER_ID,
        worker_status_queue_name="mdds_status_queue",
        worker_job_queue_name=JOB_QUEUE_NAME,
        worker_cancel_queue_name=CANCEL_QUEUE_NAME,
        worker_job_timeout_seconds=3600,
        worker_progress_interval_seconds=5,
        worker_cleanup_interval_seconds=1,
    )

    queue_client = MagicMock(name="queue_client")
    boto3_client = MagicMock(name="boto3_client")
    storage = MagicMock(name="storage")
    factory = MagicMock(name="boto3_s3_client_factory")
    factory.create.return_value = boto3_client

    manifest_loader = MagicMock(name="manifest_loader")
    input_artifact_preparer = MagicMock(name="input_artifact_preparer")
    job_execution_context_factory = MagicMock(name="job_execution_context_factory")
    job_handler_loader = MagicMock(name="job_handler_loader")
    validation_handler = MagicMock(name="validation_handler")
    execution_supervisor = MagicMock(name="execution_supervisor")
    execution_registry = MagicMock(name="execution_registry")
    status_publisher = MagicMock(name="status_publisher")
    output_artifact_uploader = MagicMock(name="output_artifact_uploader")
    job_consumer = MagicMock(name="job_consumer")
    cancellation_request_handler = MagicMock(name="cancellation_request_handler")
    cancel_consumer = MagicMock(name="cancel_consumer")
    execution_watcher = MagicMock(name="execution_watcher")
    cleanup_watcher = MagicMock(name="cleanup_watcher")
    timeout_watcher = MagicMock(name="timeout_watcher")

    rabbitmq_properties = MagicMock(name="rabbitmq_properties")
    s3_properties = SimpleNamespace(bucket="mdds")

    load_config = MagicMock(return_value=worker_config)
    rabbitmq_properties_factory = MagicMock(return_value=rabbitmq_properties)
    queue_client_factory = MagicMock(return_value=queue_client)
    s3_properties_factory = MagicMock(return_value=s3_properties)
    s3_client_factory_factory = MagicMock(return_value=factory)
    storage_factory = MagicMock(return_value=storage)
    manifest_loader_factory = MagicMock(return_value=manifest_loader)
    input_artifact_preparer_factory = MagicMock(return_value=input_artifact_preparer)
    job_execution_context_factory_factory = MagicMock(
        return_value=job_execution_context_factory
    )
    job_handler_loader_factory = MagicMock(return_value=job_handler_loader)
    validation_handler_factory = MagicMock(return_value=validation_handler)
    execution_supervisor_factory = MagicMock(return_value=execution_supervisor)
    execution_registry_factory = MagicMock(return_value=execution_registry)
    status_publisher_factory = MagicMock(return_value=status_publisher)
    output_artifact_uploader_factory = MagicMock(return_value=output_artifact_uploader)
    job_consumer_factory = MagicMock(return_value=job_consumer)
    cancellation_request_handler_factory = MagicMock(
        return_value=cancellation_request_handler
    )
    cancel_consumer_factory = MagicMock(return_value=cancel_consumer)
    execution_watcher_factory = MagicMock(return_value=execution_watcher)
    cleanup_watcher_factory = MagicMock(return_value=cleanup_watcher)
    timeout_watcher_factory = MagicMock(return_value=timeout_watcher)

    monkeypatch.setattr(worker_main, "load_config", load_config)
    monkeypatch.setattr(
        worker_main,
        "RabbitMqProperties",
        rabbitmq_properties_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "RabbitMqQueueClient",
        queue_client_factory,
    )
    monkeypatch.setattr(worker_main, "S3Properties", s3_properties_factory)
    monkeypatch.setattr(
        worker_main,
        "Boto3S3ClientFactory",
        s3_client_factory_factory,
    )
    monkeypatch.setattr(worker_main, "S3Storage", storage_factory)
    monkeypatch.setattr(worker_main, "ManifestLoader", manifest_loader_factory)
    monkeypatch.setattr(
        worker_main,
        "InputArtifactPreparer",
        input_artifact_preparer_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "JobExecutionContextFactory",
        job_execution_context_factory_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "JobHandlerLoader",
        job_handler_loader_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "ValidationHandler",
        validation_handler_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "ExecutionSupervisor",
        execution_supervisor_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "ExecutionRegistry",
        execution_registry_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "StatusPublisher",
        status_publisher_factory,
    )
    monkeypatch.setattr(
        worker_main,
        "OutputArtifactUploader",
        output_artifact_uploader_factory,
    )
    monkeypatch.setattr(worker_main, "JobConsumer", job_consumer_factory)
    monkeypatch.setattr(
        worker_main,
        "CancellationRequestHandler",
        cancellation_request_handler_factory,
    )
    monkeypatch.setattr(worker_main, "CancelConsumer", cancel_consumer_factory)
    monkeypatch.setattr(
        worker_main,
        "ExecutionWatcher",
        execution_watcher_factory,
    )
    monkeypatch.setattr(worker_main, "CleanupWatcher", cleanup_watcher_factory)
    monkeypatch.setattr(worker_main, "TimeoutWatcher", timeout_watcher_factory)

    runtime = worker_main.build_worker_runtime_from_environment()

    assert isinstance(runtime, WorkerRuntime)
    assert runtime._worker_id == WORKER_ID
    assert runtime._worker_job_queue_name == JOB_QUEUE_NAME
    assert runtime._worker_cancel_queue_name == CANCEL_QUEUE_NAME
    assert runtime._queue_client is queue_client
    assert runtime._job_consumer is job_consumer
    assert runtime._cancel_consumer is cancel_consumer
    assert runtime._execution_watcher is execution_watcher
    assert runtime._cleanup_watcher is cleanup_watcher
    assert runtime._timeout_watcher is timeout_watcher
    assert runtime._closeables == [queue_client, storage, boto3_client]

    load_config.assert_called_once()
    rabbitmq_properties_factory.assert_called_once_with(
        host="rabbitmq",
        port=5672,
        user="mdds",
        password="secret",
    )
    queue_client_factory.assert_called_once_with(rabbitmq_properties)
    queue_client.check_readiness.assert_called_once_with()
    queue_client.check_messaging_readiness.assert_called_once_with()
    s3_properties_factory.assert_called_once_with(
        endpoint_url="http://minio:9000",
        bucket="mdds",
        access_key="minioadmin",
        secret_key="minioadmin",
        region="us-east-1",
        path_style_access_enabled=True,
    )
    s3_client_factory_factory.assert_called_once_with(s3_properties)
    factory.create.assert_called_once_with()
    storage_factory.assert_called_once_with(boto3_client, "mdds")

    manifest_loader_factory.assert_called_once_with(storage)
    input_artifact_preparer_factory.assert_called_once_with(storage, tmp_path)
    job_execution_context_factory_factory.assert_called_once_with(tmp_path)
    job_handler_loader_factory.assert_called_once_with(
        "tests.fixtures.job_handlers:TwoNumbersSumJobHandler"
    )
    execution_supervisor_factory.assert_called_once_with(
        tmp_path,
        "tests.fixtures.job_handlers:TwoNumbersSumJobHandler",
    )
    execution_registry_factory.assert_called_once_with()

    status_publisher_factory.assert_called_once()
    output_artifact_uploader_factory.assert_called_once_with(storage)

    validation_handler_factory.assert_called_once_with(
        status_publisher,
        WORKER_ID,
    )

    job_consumer_factory.assert_called_once_with(
        manifest_loader,
        input_artifact_preparer,
        job_execution_context_factory,
        job_handler_loader,
        validation_handler,
        execution_supervisor,
        execution_registry,
        status_publisher,
        WORKER_ID,
    )

    cancellation_request_handler_factory.assert_called_once()
    cancel_consumer_factory.assert_called_once_with(
        cancellation_request_handler,
        WORKER_ID,
    )

    execution_watcher_factory.assert_called_once()
    cleanup_watcher_factory.assert_called_once_with(
        execution_registry=execution_registry,
        worker_id=WORKER_ID,
        cleanup_interval_seconds=1,
    )
    timeout_watcher_factory.assert_called_once()


def test_main_delegates_to_runtime_construction_start_wait_and_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = MagicMock(name="runtime")
    setup_logging = MagicMock(name="setup_logging")
    build_runtime = MagicMock(return_value=runtime)
    wait_until_shutdown_signal = MagicMock(name="wait_until_shutdown_signal")

    monkeypatch.setattr(worker_main, "setup_logging", setup_logging)
    monkeypatch.setattr(
        worker_main,
        "build_worker_runtime_from_environment",
        build_runtime,
    )
    monkeypatch.setattr(
        worker_main,
        "_wait_until_shutdown_signal",
        wait_until_shutdown_signal,
    )

    result = worker_main.main()

    assert result == 0
    setup_logging.assert_called_once_with()
    build_runtime.assert_called_once_with()
    runtime.start.assert_called_once_with()
    wait_until_shutdown_signal.assert_called_once_with()
    runtime.stop.assert_called_once_with()


def test_worker_runtime_stop_continues_when_watcher_stop_fails() -> None:
    fixture = _runtime_fixture()
    fixture.timeout_watcher.stop_error = RuntimeError("timeout watcher stop failed")

    fixture.runtime.start()
    fixture.runtime.stop()

    assert fixture.timeout_watcher.stop_count == 1
    assert fixture.cleanup_watcher.stop_count == 1
    assert fixture.execution_watcher.stop_count == 1

    assert fixture.queue_client.close_count == 1
    assert fixture.storage.close_count == 1
    assert fixture.boto3_client.close_count == 1


def test_build_worker_runtime_from_environment_uses_working_clock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    worker_config = SimpleNamespace(
        rabbitmq_host="rabbitmq",
        rabbitmq_port=5672,
        rabbitmq_user="mdds",
        rabbitmq_password="secret",
        object_storage_endpoint_url="http://minio:9000",
        object_storage_bucket="mdds",
        object_storage_access_key="minioadmin",
        object_storage_secret_key="minioadmin",
        object_storage_region="us-east-1",
        object_storage_path_style_access_enabled=True,
        jobs_root=tmp_path,
        worker_handler="tests.fixtures.job_handlers:TwoNumbersSumJobHandler",
        worker_id=WORKER_ID,
        worker_status_queue_name="mdds_status_queue",
        worker_job_queue_name=JOB_QUEUE_NAME,
        worker_cancel_queue_name=CANCEL_QUEUE_NAME,
        worker_job_timeout_seconds=3600,
        worker_progress_interval_seconds=5,
        worker_cleanup_interval_seconds=1,
    )

    queue_client = MagicMock(name="queue_client")
    boto3_client = MagicMock(name="boto3_client")
    storage = MagicMock(name="storage")
    factory = MagicMock(name="boto3_s3_client_factory")
    factory.create.return_value = boto3_client

    monkeypatch.setattr(
        worker_main, "load_config", MagicMock(return_value=worker_config)
    )
    monkeypatch.setattr(worker_main, "RabbitMqProperties", MagicMock())
    monkeypatch.setattr(
        worker_main, "RabbitMqQueueClient", MagicMock(return_value=queue_client)
    )
    monkeypatch.setattr(
        worker_main,
        "S3Properties",
        MagicMock(return_value=SimpleNamespace(bucket="mdds")),
    )
    monkeypatch.setattr(
        worker_main, "Boto3S3ClientFactory", MagicMock(return_value=factory)
    )
    monkeypatch.setattr(worker_main, "S3Storage", MagicMock(return_value=storage))
    monkeypatch.setattr(worker_main, "ManifestLoader", MagicMock())
    monkeypatch.setattr(worker_main, "InputArtifactPreparer", MagicMock())
    monkeypatch.setattr(worker_main, "JobExecutionContextFactory", MagicMock())
    monkeypatch.setattr(worker_main, "JobHandlerLoader", MagicMock())
    monkeypatch.setattr(worker_main, "ValidationHandler", MagicMock())
    monkeypatch.setattr(worker_main, "ExecutionSupervisor", MagicMock())
    monkeypatch.setattr(worker_main, "ExecutionRegistry", MagicMock())
    monkeypatch.setattr(worker_main, "OutputArtifactUploader", MagicMock())
    monkeypatch.setattr(worker_main, "JobConsumer", MagicMock())
    monkeypatch.setattr(worker_main, "CancellationRequestHandler", MagicMock())
    monkeypatch.setattr(worker_main, "CancelConsumer", MagicMock())
    monkeypatch.setattr(worker_main, "ExecutionWatcher", MagicMock())
    monkeypatch.setattr(worker_main, "CleanupWatcher", MagicMock())
    monkeypatch.setattr(worker_main, "TimeoutWatcher", MagicMock())

    status_publisher_factory = MagicMock()
    monkeypatch.setattr(worker_main, "StatusPublisher", status_publisher_factory)

    worker_main.build_worker_runtime_from_environment()

    clock = status_publisher_factory.call_args.kwargs["clock"]
    now = clock()

    assert now.tzinfo is not None


def test_wait_until_shutdown_signal_installs_handlers_and_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered_handlers: dict[int, Any] = {}
    shutdown_event = _ShutdownEventFake(registered_handlers)

    monkeypatch.setattr(worker_main.threading, "Event", lambda: shutdown_event)
    monkeypatch.setattr(
        worker_main.signal,
        "signal",
        lambda signum, handler: registered_handlers.setdefault(signum, handler),
    )

    worker_main._wait_until_shutdown_signal()

    assert worker_main.signal.SIGINT in registered_handlers
    assert worker_main.signal.SIGTERM in registered_handlers
    assert shutdown_event.wait_count == 1
    assert shutdown_event.set_count == 1


def test_close_if_supported_ignores_resource_without_close() -> None:
    worker_main._close_if_supported(object())


def test_close_if_supported_logs_and_suppresses_close_failure() -> None:
    closeable = _CloseableFake(
        "broken_closeable",
        events=[],
        close_error=RuntimeError("close failed"),
    )

    worker_main._close_if_supported(closeable)

    assert closeable.close_count == 1


def test_module_script_guard_invokes_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mdds_worker_runtime.config as worker_config_module
    import mdds_worker_runtime.logging_config as logging_config_module

    def fail_fast_load_config() -> Any:
        raise RuntimeError("script guard reached main")

    monkeypatch.setattr(worker_config_module, "load_config", fail_fast_load_config)
    monkeypatch.setattr(logging_config_module, "setup_logging", lambda: None)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)

        with pytest.raises(RuntimeError, match="script guard reached main"):
            runpy.run_module("mdds_worker_runtime.main", run_name="__main__")


def test_build_worker_runtime_from_environment_checks_s3_storage_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = _build_runtime_composition_fixture(monkeypatch, tmp_path)

    result = worker_main.build_worker_runtime_from_environment()

    assert result is fixture.runtime
    fixture.storage.check_readiness.assert_called_once_with()
    fixture.queue_client.check_readiness.assert_called_once_with()
    fixture.queue_client.check_messaging_readiness.assert_called_once_with()
    fixture.storage.check_readiness.assert_called_once_with()


def test_build_worker_runtime_from_environment_fails_fast_when_s3_storage_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MagicMock(name="storage")
    storage.check_readiness.side_effect = S3StorageReadinessError(
        "S3-compatible storage bucket is not ready: bucket='mdds'."
    )

    fixture = _build_runtime_composition_fixture(
        monkeypatch,
        tmp_path,
        storage=storage,
    )

    with pytest.raises(
        S3StorageReadinessError,
        match="S3-compatible storage bucket is not ready: bucket='mdds'.",
    ):
        worker_main.build_worker_runtime_from_environment()

    fixture.storage.check_readiness.assert_called_once_with()

    fixture.component_factories["ManifestLoader"].assert_not_called()
    fixture.component_factories["InputArtifactPreparer"].assert_not_called()
    fixture.component_factories["JobExecutionContextFactory"].assert_not_called()
    fixture.component_factories["JobHandlerLoader"].assert_not_called()
    fixture.component_factories["ExecutionSupervisor"].assert_not_called()
    fixture.component_factories["ExecutionRegistry"].assert_not_called()
    fixture.component_factories["StatusPublisher"].assert_not_called()
    fixture.component_factories["OutputArtifactUploader"].assert_not_called()
    fixture.component_factories["ValidationHandler"].assert_not_called()
    fixture.component_factories["JobConsumer"].assert_not_called()
    fixture.component_factories["CancellationRequestHandler"].assert_not_called()
    fixture.component_factories["CancelConsumer"].assert_not_called()
    fixture.component_factories["ExecutionWatcher"].assert_not_called()
    fixture.component_factories["CleanupWatcher"].assert_not_called()
    fixture.component_factories["TimeoutWatcher"].assert_not_called()

    fixture.worker_runtime_factory.assert_not_called()


@pytest.mark.parametrize(
    ("worker_handler", "expected_message"),
    [
        (
            "tests.fixtures.job_handlers:PlainClass",
            "Handler class must inherit from JobHandler",
        ),
        (
            "tests.fixtures.job_handlers:ConstructorArgumentJobHandler",
            "Handler class must have a no-argument constructor",
        ),
        (
            "tests.fixtures.job_handlers:ConstructorRaisesJobHandler",
            "Cannot instantiate handler class while loading",
        ),
        (
            "tests.fixtures.job_handlers:ConstructorReturnsNonJobHandlerInstanceJobHandler",
            "Handler constructor must return a JobHandler instance",
        ),
    ],
)
def test_build_worker_runtime_from_environment_fails_fast_when_job_handler_is_not_loadable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    worker_handler: str,
    expected_message: str,
) -> None:
    fixture = _build_runtime_composition_fixture(
        monkeypatch,
        tmp_path,
        worker_handler=worker_handler,
        use_real_job_handler_loader=True,
    )

    with pytest.raises(JobHandlerLoadError, match=expected_message):
        worker_main.build_worker_runtime_from_environment()

    fixture.storage.check_readiness.assert_called_once_with()

    fixture.component_factories["ExecutionSupervisor"].assert_not_called()
    fixture.component_factories["ExecutionRegistry"].assert_not_called()
    fixture.component_factories["StatusPublisher"].assert_not_called()
    fixture.component_factories["OutputArtifactUploader"].assert_not_called()
    fixture.component_factories["ValidationHandler"].assert_not_called()
    fixture.component_factories["JobConsumer"].assert_not_called()
    fixture.component_factories["CancellationRequestHandler"].assert_not_called()
    fixture.component_factories["CancelConsumer"].assert_not_called()
    fixture.component_factories["ExecutionWatcher"].assert_not_called()
    fixture.component_factories["CleanupWatcher"].assert_not_called()
    fixture.component_factories["TimeoutWatcher"].assert_not_called()

    fixture.worker_runtime_factory.assert_not_called()


def test_build_worker_runtime_from_environment_checks_rabbitmq_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = _build_runtime_composition_fixture(monkeypatch, tmp_path)

    result = worker_main.build_worker_runtime_from_environment()

    assert result is fixture.runtime
    fixture.queue_client.check_readiness.assert_called_once_with()


def test_build_worker_runtime_from_environment_fails_fast_when_rabbitmq_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_client = MagicMock(name="queue_client")
    queue_client.check_readiness.side_effect = RabbitMqConnectionError(
        "RabbitMQ connection readiness check failed."
    )

    fixture = _build_runtime_composition_fixture(
        monkeypatch,
        tmp_path,
        queue_client=queue_client,
    )

    with pytest.raises(
        RabbitMqConnectionError,
        match="RabbitMQ connection readiness check failed.",
    ):
        worker_main.build_worker_runtime_from_environment()

    fixture.queue_client.check_readiness.assert_called_once_with()

    fixture.infrastructure_factories["S3Properties"].assert_not_called()
    fixture.infrastructure_factories["Boto3S3ClientFactory"].assert_not_called()
    fixture.infrastructure_factories["S3Storage"].assert_not_called()

    fixture.component_factories["ManifestLoader"].assert_not_called()
    fixture.component_factories["InputArtifactPreparer"].assert_not_called()
    fixture.component_factories["JobExecutionContextFactory"].assert_not_called()
    fixture.component_factories["JobHandlerLoader"].assert_not_called()
    fixture.component_factories["ExecutionSupervisor"].assert_not_called()
    fixture.component_factories["ExecutionRegistry"].assert_not_called()
    fixture.component_factories["StatusPublisher"].assert_not_called()
    fixture.component_factories["OutputArtifactUploader"].assert_not_called()
    fixture.component_factories["ValidationHandler"].assert_not_called()
    fixture.component_factories["JobConsumer"].assert_not_called()
    fixture.component_factories["CancellationRequestHandler"].assert_not_called()
    fixture.component_factories["CancelConsumer"].assert_not_called()
    fixture.component_factories["ExecutionWatcher"].assert_not_called()
    fixture.component_factories["CleanupWatcher"].assert_not_called()
    fixture.component_factories["TimeoutWatcher"].assert_not_called()
    fixture.queue_client.check_readiness.assert_called_once_with()
    fixture.queue_client.check_messaging_readiness.assert_not_called()

    fixture.worker_runtime_factory.assert_not_called()


def _runtime_kwargs() -> dict[str, Any]:
    fixture = _runtime_fixture()

    return {
        "worker_id": WORKER_ID,
        "worker_job_queue_name": JOB_QUEUE_NAME,
        "worker_cancel_queue_name": CANCEL_QUEUE_NAME,
        "queue_client": fixture.queue_client,
        "job_consumer": fixture.job_consumer,
        "cancel_consumer": fixture.cancel_consumer,
        "execution_watcher": fixture.execution_watcher,
        "cleanup_watcher": fixture.cleanup_watcher,
        "timeout_watcher": fixture.timeout_watcher,
        "closeables": [
            fixture.queue_client,
            fixture.storage,
            fixture.boto3_client,
        ],
    }


def _runtime_fixture(
    *,
    queue_client: "_QueueClientFake | None" = None,
) -> "_RuntimeFixture":
    events: list[str] = []

    resolved_queue_client = queue_client or _QueueClientFake(events=events)
    resolved_queue_client.events = events

    execution_watcher = _RuntimeServiceFake("execution_watcher", events)
    cleanup_watcher = _RuntimeServiceFake("cleanup_watcher", events)
    timeout_watcher = _RuntimeServiceFake("timeout_watcher", events)

    storage = _CloseableFake("storage", events)
    boto3_client = _CloseableFake("boto3_client", events)

    job_consumer = MagicMock(spec=JobConsumer)
    cancel_consumer = MagicMock(spec=CancelConsumer)

    runtime = WorkerRuntime(
        worker_id=WORKER_ID,
        worker_job_queue_name=JOB_QUEUE_NAME,
        worker_cancel_queue_name=CANCEL_QUEUE_NAME,
        queue_client=resolved_queue_client,
        job_consumer=cast(JobConsumer, job_consumer),
        cancel_consumer=cast(CancelConsumer, cancel_consumer),
        execution_watcher=cast(ExecutionWatcher, cast(object, execution_watcher)),
        cleanup_watcher=cast(CleanupWatcher, cast(object, cleanup_watcher)),
        timeout_watcher=cast(TimeoutWatcher, cast(object, timeout_watcher)),
        closeables=[
            resolved_queue_client,
            storage,
            boto3_client,
        ],
    )

    return _RuntimeFixture(
        runtime=runtime,
        events=events,
        queue_client=resolved_queue_client,
        job_consumer=cast(JobConsumer, job_consumer),
        cancel_consumer=cast(CancelConsumer, cancel_consumer),
        execution_watcher=execution_watcher,
        cleanup_watcher=cleanup_watcher,
        timeout_watcher=timeout_watcher,
        storage=storage,
        boto3_client=boto3_client,
    )


class _RuntimeFixture:
    def __init__(
        self,
        *,
        runtime: WorkerRuntime,
        events: list[str],
        queue_client: "_QueueClientFake",
        job_consumer: JobConsumer,
        cancel_consumer: CancelConsumer,
        execution_watcher: "_RuntimeServiceFake",
        cleanup_watcher: "_RuntimeServiceFake",
        timeout_watcher: "_RuntimeServiceFake",
        storage: "_CloseableFake",
        boto3_client: "_CloseableFake",
    ) -> None:
        self.runtime = runtime
        self.events = events
        self.queue_client = queue_client
        self.job_consumer = job_consumer
        self.cancel_consumer = cancel_consumer
        self.execution_watcher = execution_watcher
        self.cleanup_watcher = cleanup_watcher
        self.timeout_watcher = timeout_watcher
        self.storage = storage
        self.boto3_client = boto3_client


class _QueueClientFake:
    def __init__(
        self,
        *,
        events: list[str] | None = None,
        fail_on_queue: str | None = None,
    ) -> None:
        self.events = events if events is not None else []
        self._fail_on_queue = fail_on_queue
        self.subscribe_calls: list[tuple[str, type[Any], Any]] = []
        self.subscriptions: dict[str, _SubscriptionFake] = {}
        self.close_count = 0

    def subscribe(
        self,
        queue_name: str,
        payload_type: type[Any],
        handler: Any,
    ) -> "_SubscriptionFake":
        self.events.append(f"subscribe:{queue_name}")
        self.subscribe_calls.append((queue_name, payload_type, handler))

        if queue_name == self._fail_on_queue:
            raise RuntimeError(f"subscribe failed: {queue_name}")

        subscription = _SubscriptionFake(queue_name, self.events)
        self.subscriptions[queue_name] = subscription
        return subscription

    def close(self) -> None:
        self.close_count += 1
        self.events.append("close:queue_client")


class _SubscriptionFake:
    def __init__(self, queue_name: str, events: list[str]) -> None:
        self.queue_name = queue_name
        self._events = events
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1
        self._events.append(f"close_subscription:{self.queue_name}")


class _RuntimeServiceFake:
    def __init__(
        self,
        name: str,
        events: list[str],
        *,
        stop_error: Exception | None = None,
    ) -> None:
        self.name = name
        self._events = events
        self.stop_error = stop_error
        self.start_count = 0
        self.stop_count = 0

    def start(self) -> None:
        self.start_count += 1
        self._events.append(f"start:{self.name}")

    def stop(self) -> None:
        self.stop_count += 1
        self._events.append(f"stop:{self.name}")

        if self.stop_error is not None:
            raise self.stop_error


class _CloseableFake:
    def __init__(
        self,
        name: str,
        events: list[str],
        *,
        close_error: Exception | None = None,
    ) -> None:
        self.name = name
        self._events = events
        self.close_error = close_error
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1
        self._events.append(f"close:{self.name}")

        if self.close_error is not None:
            raise self.close_error


class _ShutdownEventFake:
    def __init__(self, registered_handlers: dict[int, Any]) -> None:
        self._registered_handlers = registered_handlers
        self.wait_count = 0
        self.set_count = 0

    def wait(self) -> None:
        self.wait_count += 1

        self._registered_handlers[worker_main.signal.SIGTERM](
            worker_main.signal.SIGTERM,
            None,
        )

    def set(self) -> None:
        self.set_count += 1


def _build_runtime_composition_fixture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    queue_client: MagicMock | None = None,
    storage: MagicMock | None = None,
    worker_handler: str = "tests.fixtures.job_handlers:TwoNumbersSumJobHandler",
    use_real_job_handler_loader: bool = False,
) -> SimpleNamespace:
    worker_config = SimpleNamespace(
        rabbitmq_host="rabbitmq",
        rabbitmq_port=5672,
        rabbitmq_user="mdds",
        rabbitmq_password="secret",
        object_storage_endpoint_url="http://minio:9000",
        object_storage_bucket="mdds",
        object_storage_access_key="minioadmin",
        object_storage_secret_key="minioadmin",
        object_storage_region="us-east-1",
        object_storage_path_style_access_enabled=True,
        jobs_root=tmp_path,
        worker_handler=worker_handler,
        worker_id=WORKER_ID,
        worker_status_queue_name="mdds_status_queue",
        worker_job_queue_name=JOB_QUEUE_NAME,
        worker_cancel_queue_name=CANCEL_QUEUE_NAME,
        worker_job_timeout_seconds=3600,
        worker_progress_interval_seconds=5,
        worker_cleanup_interval_seconds=1,
    )

    resolved_queue_client = queue_client or MagicMock(name="queue_client")
    boto3_client = MagicMock(name="boto3_client")
    resolved_storage = storage or MagicMock(name="storage")

    s3_client_factory = MagicMock(name="boto3_s3_client_factory")
    s3_client_factory.create.return_value = boto3_client

    runtime = MagicMock(name="runtime")
    worker_runtime_factory = MagicMock(
        name="worker_runtime_factory",
        return_value=runtime,
    )

    monkeypatch.setattr(
        worker_main,
        "load_config",
        MagicMock(return_value=worker_config),
    )

    rabbitmq_properties_factory = MagicMock(name="RabbitMqProperties_factory")
    queue_client_factory = MagicMock(
        name="RabbitMqQueueClient_factory",
        return_value=resolved_queue_client,
    )
    s3_properties_factory = MagicMock(
        name="S3Properties_factory",
        return_value=SimpleNamespace(bucket="mdds"),
    )
    s3_client_factory_factory = MagicMock(
        name="Boto3S3ClientFactory_factory",
        return_value=s3_client_factory,
    )
    storage_factory = MagicMock(
        name="S3Storage_factory",
        return_value=resolved_storage,
    )

    monkeypatch.setattr(worker_main, "RabbitMqProperties", rabbitmq_properties_factory)
    monkeypatch.setattr(worker_main, "RabbitMqQueueClient", queue_client_factory)
    monkeypatch.setattr(worker_main, "S3Properties", s3_properties_factory)
    monkeypatch.setattr(worker_main, "Boto3S3ClientFactory", s3_client_factory_factory)
    monkeypatch.setattr(worker_main, "S3Storage", storage_factory)

    component_factories: dict[str, MagicMock] = {}

    for component_name in [
        "ManifestLoader",
        "InputArtifactPreparer",
        "JobExecutionContextFactory",
        "JobHandlerLoader",
        "ExecutionSupervisor",
        "ExecutionRegistry",
        "StatusPublisher",
        "OutputArtifactUploader",
        "ValidationHandler",
        "JobConsumer",
        "CancellationRequestHandler",
        "CancelConsumer",
        "ExecutionWatcher",
        "CleanupWatcher",
        "TimeoutWatcher",
    ]:
        if component_name == "JobHandlerLoader" and use_real_job_handler_loader:
            continue

        component_factory = MagicMock(name=f"{component_name}_factory")
        component_factories[component_name] = component_factory
        monkeypatch.setattr(worker_main, component_name, component_factory)

    monkeypatch.setattr(worker_main, "WorkerRuntime", worker_runtime_factory)

    return SimpleNamespace(
        runtime=runtime,
        queue_client=resolved_queue_client,
        boto3_client=boto3_client,
        storage=resolved_storage,
        s3_client_factory=s3_client_factory,
        infrastructure_factories={
            "RabbitMqProperties": rabbitmq_properties_factory,
            "RabbitMqQueueClient": queue_client_factory,
            "S3Properties": s3_properties_factory,
            "Boto3S3ClientFactory": s3_client_factory_factory,
            "S3Storage": storage_factory,
        },
        component_factories=component_factories,
        worker_runtime_factory=worker_runtime_factory,
    )


def test_build_worker_runtime_from_environment_checks_rabbitmq_messaging_readiness_after_connection_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = _build_runtime_composition_fixture(monkeypatch, tmp_path)
    calls = MagicMock(name="calls")

    calls.attach_mock(fixture.queue_client.check_readiness, "check_readiness")
    calls.attach_mock(
        fixture.queue_client.check_messaging_readiness,
        "check_messaging_readiness",
    )
    calls.attach_mock(
        fixture.infrastructure_factories["S3Properties"],
        "S3Properties",
    )

    result = worker_main.build_worker_runtime_from_environment()

    assert result is fixture.runtime
    assert calls.mock_calls[:3] == [
        call.check_readiness(),
        call.check_messaging_readiness(),
        call.S3Properties(
            endpoint_url="http://minio:9000",
            bucket="mdds",
            access_key="minioadmin",
            secret_key="minioadmin",
            region="us-east-1",
            path_style_access_enabled=True,
        ),
    ]


def test_build_worker_runtime_from_environment_fails_fast_when_rabbitmq_messaging_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_client = MagicMock(name="queue_client")
    queue_client.check_messaging_readiness.side_effect = RabbitMqConnectionError(
        "RabbitMQ messaging readiness check failed."
    )

    fixture = _build_runtime_composition_fixture(
        monkeypatch,
        tmp_path,
        queue_client=queue_client,
    )

    with pytest.raises(
        RabbitMqConnectionError,
        match="RabbitMQ messaging readiness check failed.",
    ):
        worker_main.build_worker_runtime_from_environment()

    fixture.queue_client.check_readiness.assert_called_once_with()
    fixture.queue_client.check_messaging_readiness.assert_called_once_with()

    fixture.infrastructure_factories["S3Properties"].assert_not_called()
    fixture.infrastructure_factories["Boto3S3ClientFactory"].assert_not_called()
    fixture.infrastructure_factories["S3Storage"].assert_not_called()

    fixture.component_factories["ManifestLoader"].assert_not_called()
    fixture.component_factories["InputArtifactPreparer"].assert_not_called()
    fixture.component_factories["JobExecutionContextFactory"].assert_not_called()
    fixture.component_factories["JobHandlerLoader"].assert_not_called()
    fixture.component_factories["ExecutionSupervisor"].assert_not_called()
    fixture.component_factories["ExecutionRegistry"].assert_not_called()
    fixture.component_factories["StatusPublisher"].assert_not_called()
    fixture.component_factories["OutputArtifactUploader"].assert_not_called()
    fixture.component_factories["ValidationHandler"].assert_not_called()
    fixture.component_factories["JobConsumer"].assert_not_called()
    fixture.component_factories["CancellationRequestHandler"].assert_not_called()
    fixture.component_factories["CancelConsumer"].assert_not_called()
    fixture.component_factories["ExecutionWatcher"].assert_not_called()
    fixture.component_factories["CleanupWatcher"].assert_not_called()
    fixture.component_factories["TimeoutWatcher"].assert_not_called()

    fixture.worker_runtime_factory.assert_not_called()
