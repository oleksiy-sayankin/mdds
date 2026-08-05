# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import signal
from multiprocessing import get_context
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from unittest.mock import MagicMock, call

import pytest
from mdds_worker_runtime_common.execution import supervisor as supervisor_module
from mdds_worker_runtime_common.execution.handler_loader import WorkerHandlerLoadError
from mdds_worker_runtime_common.execution.supervisor import (
    MAX_WORKER_RESULT_MESSAGE_BYTES,
    TRUNCATION_SUFFIX,
    WorkerProcessSupervisor,
    WorkerTerminationRequested,
    limit_result_message,
)

HANDLER_IMPORT_PATH = "example_worker.handler:ExampleWorkerHandler"
ASYNC_HANDLER_IMPORT_PATH = "tests.fixtures.worker_handlers:AsyncExecuteWorkerHandler"


@pytest.fixture
def process_context() -> MagicMock:
    return MagicMock(spec=get_context("spawn"))


@pytest.fixture
def supervisor(process_context: MagicMock) -> WorkerProcessSupervisor:
    return WorkerProcessSupervisor(HANDLER_IMPORT_PATH, process_context)


@pytest.mark.parametrize("handler_import_path", [None, "", " \t "])
def test_rejects_null_or_blank_handler_import_path(
    handler_import_path: str | None,
    process_context: MagicMock,
) -> None:
    with pytest.raises(
        ValueError,
        match="handler_import_path cannot be null or blank",
    ):
        WorkerProcessSupervisor(
            handler_import_path,  # type: ignore[arg-type]
            process_context,
        )


def test_rejects_null_process_context() -> None:
    with pytest.raises(
        ValueError,
        match="process_context cannot be null",
    ):
        WorkerProcessSupervisor(
            HANDLER_IMPORT_PATH,
            None,  # type: ignore[arg-type]
        )


def test_context_manager_installs_and_restores_sigterm_handler(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_handler = MagicMock()
    getsignal_mock = MagicMock(return_value=previous_handler)
    signal_mock = MagicMock()
    close_mock = MagicMock()
    monkeypatch.setattr(supervisor_module.signal, "getsignal", getsignal_mock)
    monkeypatch.setattr(supervisor_module.signal, "signal", signal_mock)
    monkeypatch.setattr(supervisor, "close", close_mock)

    with supervisor as entered_supervisor:
        assert entered_supervisor is supervisor

    getsignal_mock.assert_called_once_with(signal.SIGTERM)
    assert signal_mock.call_args_list == [
        call(signal.SIGTERM, supervisor._handle_sigterm),
        call(signal.SIGTERM, previous_handler),
    ]
    close_mock.assert_called_once_with(None)


def test_restore_sigterm_handler_is_noop_when_handler_was_not_installed(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_mock = MagicMock()
    monkeypatch.setattr(supervisor_module.signal, "signal", signal_mock)

    supervisor.restore_sigterm_handler()

    signal_mock.assert_not_called()


def test_sigterm_handler_records_termination_request(
    supervisor: WorkerProcessSupervisor,
) -> None:
    assert not supervisor.is_termination_requested()

    with pytest.raises(WorkerTerminationRequested):
        supervisor._handle_sigterm(signal.SIGTERM, None)

    assert supervisor.is_termination_requested()


def test_sigterm_handler_ignores_repeated_termination_request(
    supervisor: WorkerProcessSupervisor,
) -> None:
    with pytest.raises(WorkerTerminationRequested):
        supervisor._handle_sigterm(signal.SIGTERM, None)

    supervisor._handle_sigterm(signal.SIGTERM, None)

    assert supervisor.is_termination_requested()


def test_normal_completion_critical_section_linearizes_normal_result(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_mask = {signal.SIGINT}
    pthread_sigmask_mock = MagicMock(return_value=previous_mask)
    monkeypatch.setattr(
        supervisor_module.signal,
        "pthread_sigmask",
        pthread_sigmask_mock,
    )

    with supervisor.normal_completion_critical_section():
        assert not supervisor.is_termination_requested()

    # SIGTERM is ignored once normal completion has been linearized.
    supervisor._handle_sigterm(signal.SIGTERM, None)
    assert not supervisor.is_termination_requested()
    assert pthread_sigmask_mock.call_args_list == [
        call(signal.SIG_BLOCK, {signal.SIGTERM}),
        call(signal.SIG_SETMASK, previous_mask),
    ]


def test_normal_completion_critical_section_restores_mask_after_failure(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_mask = {signal.SIGINT}
    pthread_sigmask_mock = MagicMock(return_value=previous_mask)
    monkeypatch.setattr(
        supervisor_module.signal,
        "pthread_sigmask",
        pthread_sigmask_mock,
    )

    with (
        pytest.raises(RuntimeError, match="publication failed"),
        supervisor.normal_completion_critical_section(),
    ):
        raise RuntimeError("publication failed")

    # A failed publication does not linearize normal completion.
    with pytest.raises(WorkerTerminationRequested):
        supervisor._handle_sigterm(signal.SIGTERM, None)
    assert pthread_sigmask_mock.call_args_list == [
        call(signal.SIG_BLOCK, {signal.SIGTERM}),
        call(signal.SIG_SETMASK, previous_mask),
    ]


def test_normal_completion_critical_section_rejects_pending_termination(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_mask = {signal.SIGINT}
    pthread_sigmask_mock = MagicMock(return_value=previous_mask)
    monkeypatch.setattr(
        supervisor_module.signal,
        "pthread_sigmask",
        pthread_sigmask_mock,
    )
    with pytest.raises(WorkerTerminationRequested):
        supervisor._handle_sigterm(signal.SIGTERM, None)

    with (
        pytest.raises(WorkerTerminationRequested),
        supervisor.normal_completion_critical_section(),
    ):
        pytest.fail("critical section must not be entered")

    assert pthread_sigmask_mock.call_args_list == [
        call(signal.SIG_BLOCK, {signal.SIGTERM}),
        call(signal.SIG_SETMASK, previous_mask),
    ]


def test_begin_termination_cleanup_reuses_single_deadline(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_mock = MagicMock(return_value=10.0)
    monkeypatch.setattr(supervisor_module.time, "monotonic", monotonic_mock)

    first_deadline = supervisor.begin_termination_cleanup()
    monotonic_mock.return_value = 20.0
    second_deadline = supervisor.begin_termination_cleanup()

    assert first_deadline == 11.0
    assert second_deadline == first_deadline
    monotonic_mock.assert_called_once_with()


def test_close_without_started_process_is_idempotent(
    supervisor: WorkerProcessSupervisor,
) -> None:
    supervisor.close()
    supervisor.close()


def test_close_reaps_completed_process_and_is_idempotent(
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = 123
    process.exitcode = 0
    result_connection = MagicMock(spec=Connection)
    supervisor._process = process
    supervisor._result_connection = result_connection

    supervisor.close()
    supervisor.close()

    process.kill.assert_not_called()
    process.join.assert_called_once_with()
    process.close.assert_called_once_with()
    result_connection.close.assert_called_once_with()
    assert supervisor._process is None
    assert supervisor._result_connection is None


def test_close_kills_running_process_before_reaping_it(
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = 123
    process.exitcode = None
    result_connection = MagicMock(spec=Connection)
    supervisor._process = process
    supervisor._result_connection = result_connection

    supervisor.close()

    process.kill.assert_called_once_with()
    process.join.assert_called_once_with()
    process.close.assert_called_once_with()
    result_connection.close.assert_called_once_with()


def test_close_with_deadline_closes_process_after_termination_is_confirmed(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = 123
    process.exitcode = 0
    result_connection = MagicMock(spec=Connection)
    force_terminate_mock = MagicMock()
    monkeypatch.setattr(
        WorkerProcessSupervisor,
        "force_terminate",
        force_terminate_mock,
    )
    supervisor._process = process
    supervisor._result_connection = result_connection

    supervisor.close(deadline=11.0)

    force_terminate_mock.assert_called_once_with(process, 11.0)
    process.close.assert_called_once_with()
    result_connection.close.assert_called_once_with()


def test_close_with_deadline_leaves_unconfirmed_process_object_open(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = 123
    process.exitcode = None
    result_connection = MagicMock(spec=Connection)
    force_terminate_mock = MagicMock()
    monkeypatch.setattr(
        WorkerProcessSupervisor,
        "force_terminate",
        force_terminate_mock,
    )
    supervisor._process = process
    supervisor._result_connection = result_connection

    supervisor.close(deadline=11.0)

    force_terminate_mock.assert_called_once_with(process, 11.0)
    process.close.assert_not_called()
    result_connection.close.assert_called_once_with()


def test_close_closes_result_connection_when_process_cleanup_fails(
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.exitcode = 0
    process.close.side_effect = RuntimeError("process close failed")
    result_connection = MagicMock(spec=Connection)
    supervisor._process = process
    supervisor._result_connection = result_connection

    with pytest.raises(RuntimeError, match="process close failed"):
        supervisor.close()

    result_connection.close.assert_called_once_with()
    assert supervisor._process is None
    assert supervisor._result_connection is None


def test_execute_worker_handler_reports_success(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = object()
    handler = MagicMock()
    result_connection = MagicMock(spec=Connection)
    load_context_mock = MagicMock(return_value=context)
    load_handler_mock = MagicMock(return_value=handler)
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_execution_context",
        load_context_mock,
    )
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_handler",
        load_handler_mock,
    )

    with pytest.raises(SystemExit) as raised:
        supervisor.execute_worker_handler(result_connection)

    assert raised.value.code == 0
    load_context_mock.assert_called_once_with()
    load_handler_mock.assert_called_once_with(HANDLER_IMPORT_PATH)
    handler.execute.assert_called_once_with(context)
    result_connection.send.assert_called_once_with(
        (0, "WorkerHandler execution completed.")
    )
    result_connection.close.assert_called_once_with()


def test_execute_worker_handler_reports_handler_loading_failure(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = object()
    handler = MagicMock()
    result_connection = MagicMock(spec=Connection)
    load_context_mock = MagicMock(return_value=context)
    load_handler_mock = MagicMock(
        return_value=handler,
        side_effect=WorkerHandlerLoadError("handler loading failed"),
    )
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_execution_context",
        load_context_mock,
    )
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_handler",
        load_handler_mock,
    )

    with pytest.raises(SystemExit) as raised:
        supervisor.execute_worker_handler(result_connection)

    assert raised.value.code == 2
    load_context_mock.assert_called_once_with()
    load_handler_mock.assert_called_once_with(HANDLER_IMPORT_PATH)
    handler.execute.assert_not_called()
    result_connection.send.assert_called_once_with((2, "handler loading failed"))
    result_connection.close.assert_called_once_with()


def test_execute_worker_handler_reports_async_handler_as_contract_violation(
    process_context: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = WorkerProcessSupervisor(
        ASYNC_HANDLER_IMPORT_PATH,
        process_context,
    )
    result_connection = MagicMock(spec=Connection)
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_execution_context",
        MagicMock(return_value=object()),
    )

    with pytest.raises(SystemExit) as raised:
        supervisor.execute_worker_handler(result_connection)

    assert raised.value.code == 2
    result_connection.send.assert_called_once_with(
        (
            2,
            f"Handler method execute must be synchronous: {ASYNC_HANDLER_IMPORT_PATH}",
        )
    )
    result_connection.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("error", "expected_message"),
    [
        (RuntimeError("handler failed"), "handler failed"),
        (RuntimeError(" \t "), "WorkerHandler execution failed."),
        (KeyboardInterrupt(), "WorkerHandler execution failed."),
    ],
)
def test_execute_worker_handler_reports_failure(
    error: BaseException,
    expected_message: str,
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = MagicMock()
    handler.execute.side_effect = error
    result_connection = MagicMock(spec=Connection)
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_execution_context",
        MagicMock(return_value=object()),
    )
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_handler",
        MagicMock(return_value=handler),
    )

    with pytest.raises(SystemExit) as raised:
        supervisor.execute_worker_handler(result_connection)

    assert raised.value.code == 1
    result_connection.send.assert_called_once_with((1, expected_message))
    result_connection.close.assert_called_once_with()


def test_execute_worker_handler_limits_failure_message_before_sending(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = MagicMock()
    handler.execute.side_effect = RuntimeError("я" * MAX_WORKER_RESULT_MESSAGE_BYTES)
    result_connection = MagicMock(spec=Connection)
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_execution_context",
        MagicMock(return_value=object()),
    )
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_handler",
        MagicMock(return_value=handler),
    )

    with pytest.raises(SystemExit) as raised:
        supervisor.execute_worker_handler(result_connection)

    reported_exit_code, message = result_connection.send.call_args.args[0]
    assert raised.value.code == 1
    assert reported_exit_code == 1
    assert message.endswith(TRUNCATION_SUFFIX)
    assert len(message.encode("utf-8")) <= MAX_WORKER_RESULT_MESSAGE_BYTES


def test_execute_worker_handler_closes_connection_when_send_fails(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = MagicMock()
    result_connection = MagicMock(spec=Connection)
    result_connection.send.side_effect = OSError("pipe is closed")
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_execution_context",
        MagicMock(return_value=object()),
    )
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_handler",
        MagicMock(return_value=handler),
    )

    with pytest.raises(OSError, match="pipe is closed"):
        supervisor.execute_worker_handler(result_connection)

    result_connection.close.assert_called_once_with()


def test_execute_worker_handler_reports_context_loading_failure(
    supervisor: WorkerProcessSupervisor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result_connection = MagicMock(spec=Connection)
    load_context_mock = MagicMock(
        side_effect=RuntimeError("execution context loading failed")
    )
    load_handler_mock = MagicMock()
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_execution_context",
        load_context_mock,
    )
    monkeypatch.setattr(
        supervisor_module,
        "load_worker_handler",
        load_handler_mock,
    )

    with pytest.raises(SystemExit) as raised:
        supervisor.execute_worker_handler(result_connection)

    assert raised.value.code == 1
    load_context_mock.assert_called_once_with()
    load_handler_mock.assert_not_called()
    result_connection.send.assert_called_once_with(
        (1, "execution context loading failed")
    )
    result_connection.close.assert_called_once_with()


def test_start_creates_and_starts_worker_process(
    supervisor: WorkerProcessSupervisor,
    process_context: MagicMock,
) -> None:
    receive_connection = MagicMock(spec=Connection)
    send_connection = MagicMock(spec=Connection)
    process = MagicMock(spec=BaseProcess)
    process_context.Pipe.return_value = (
        receive_connection,
        send_connection,
    )
    process_context.Process.return_value = process

    started_process = supervisor.start()

    assert started_process is process
    process_context.Pipe.assert_called_once_with(duplex=False)
    process_context.Process.assert_called_once_with(
        target=supervisor.execute_worker_handler,
        args=(send_connection,),
        name="mdds-worker-handler",
    )
    process.start.assert_called_once_with()
    send_connection.close.assert_called_once_with()
    assert supervisor._result_connection is receive_connection
    assert supervisor._process is process


def test_wait_joins_worker_process() -> None:
    process = MagicMock(spec=BaseProcess)

    WorkerProcessSupervisor.wait(process)

    process.join.assert_called_once_with()


def test_result_rejects_running_process(
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.exitcode = None

    with pytest.raises(RuntimeError, match="Worker process is still running"):
        supervisor.result(process)


def test_result_returns_reported_result_when_exit_codes_match(
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.exitcode = 0
    result_connection = MagicMock(spec=Connection)
    result_connection.poll.return_value = True
    result_connection.recv.return_value = (0, "completed")
    supervisor._result_connection = result_connection

    assert supervisor.result(process) == (0, "completed")


def test_result_rejects_inconsistent_reported_exit_code(
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.exitcode = 0
    result_connection = MagicMock(spec=Connection)
    result_connection.poll.return_value = True
    result_connection.recv.return_value = (1, "failed")
    supervisor._result_connection = result_connection

    assert supervisor.result(process) == (
        1,
        "Worker process reported an inconsistent exit code.",
    )


@pytest.mark.parametrize("connection_error", [EOFError(), OSError()])
def test_result_treats_connection_error_as_missing_result(
    connection_error: BaseException,
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.exitcode = 7
    result_connection = MagicMock(spec=Connection)
    result_connection.poll.side_effect = connection_error
    supervisor._result_connection = result_connection

    assert supervisor.result(process) == (
        7,
        "Worker process completed without reporting a result.",
    )


@pytest.mark.parametrize(
    ("exit_code", "expected_result"),
    [
        (
            -signal.SIGKILL,
            (137, "Worker process was terminated by signal 9."),
        ),
        (
            0,
            (1, "Worker process completed without reporting a result."),
        ),
        (
            7,
            (7, "Worker process completed without reporting a result."),
        ),
    ],
)
def test_result_handles_process_without_reported_result(
    exit_code: int,
    expected_result: tuple[int, str],
    supervisor: WorkerProcessSupervisor,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.exitcode = exit_code
    result_connection = MagicMock(spec=Connection)
    result_connection.poll.return_value = False
    supervisor._result_connection = result_connection

    assert supervisor.result(process) == expected_result


@pytest.mark.parametrize(
    ("exit_code", "expected"),
    [(None, False), (0, True), (1, True), (-signal.SIGKILL, True)],
)
def test_is_completed_uses_process_exit_code(
    exit_code: int | None,
    expected: bool,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.exitcode = exit_code

    assert WorkerProcessSupervisor.is_completed(process) is expected


def test_force_terminate_kills_running_process_and_waits_until_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = 123
    process.exitcode = None
    monkeypatch.setattr(supervisor_module.time, "monotonic", lambda: 10.75)

    WorkerProcessSupervisor.force_terminate(process, deadline=11.0)

    process.kill.assert_called_once_with()
    process.join.assert_called_once_with(timeout=0.25)


def test_force_terminate_only_waits_for_completed_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = 123
    process.exitcode = 0
    monkeypatch.setattr(supervisor_module.time, "monotonic", lambda: 10.75)

    WorkerProcessSupervisor.force_terminate(process, deadline=11.0)

    process.kill.assert_not_called()
    process.join.assert_called_once_with(timeout=0.25)


def test_force_terminate_ignores_process_that_was_not_started() -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = None

    WorkerProcessSupervisor.force_terminate(process, deadline=11.0)

    process.kill.assert_not_called()
    process.join.assert_not_called()


def test_force_terminate_waits_when_process_disappears_before_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = MagicMock(spec=BaseProcess)
    process.pid = 123
    process.exitcode = None
    process.kill.side_effect = ProcessLookupError
    monkeypatch.setattr(supervisor_module.time, "monotonic", lambda: 10.75)

    WorkerProcessSupervisor.force_terminate(process, deadline=11.0)

    process.kill.assert_called_once_with()
    process.join.assert_called_once_with(timeout=0.25)


@pytest.mark.parametrize(
    "message",
    [
        "short message",
        "\u044f" * 100,
        "a" * MAX_WORKER_RESULT_MESSAGE_BYTES,
    ],
)
def test_limit_result_message_preserves_messages_within_limit(
    message: str,
) -> None:
    assert limit_result_message(message) == message


def test_limit_result_message_truncates_long_ascii_message() -> None:
    suffix_size = len(TRUNCATION_SUFFIX.encode("ascii"))
    message = "a" * (MAX_WORKER_RESULT_MESSAGE_BYTES + 1)

    result = limit_result_message(message)

    assert result == (
        "a" * (MAX_WORKER_RESULT_MESSAGE_BYTES - suffix_size) + TRUNCATION_SUFFIX
    )
    assert len(result.encode("utf-8")) == MAX_WORKER_RESULT_MESSAGE_BYTES


def test_limit_result_message_does_not_split_utf8_character() -> None:
    character = "\u044f"
    character_size = len(character.encode("utf-8"))
    suffix_size = len(TRUNCATION_SUFFIX.encode("ascii"))
    expected_character_count = (
        MAX_WORKER_RESULT_MESSAGE_BYTES - suffix_size
    ) // character_size
    message = character * MAX_WORKER_RESULT_MESSAGE_BYTES

    result = limit_result_message(message)

    assert result.endswith(TRUNCATION_SUFFIX)
    assert len(result.encode("utf-8")) <= MAX_WORKER_RESULT_MESSAGE_BYTES
    assert result.removesuffix(TRUNCATION_SUFFIX) == (
        character * expected_character_count
    )


def test_limit_result_message_replaces_invalid_unicode() -> None:
    assert limit_result_message("before\ud800after") == "before?after"
