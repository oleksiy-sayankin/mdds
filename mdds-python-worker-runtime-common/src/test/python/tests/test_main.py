# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import json
import runpy
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from mdds_worker_runtime_common import config as config_module
from mdds_worker_runtime_common import logging_config as logging_config_module
from mdds_worker_runtime_common import main as main_module
from mdds_worker_runtime_common.config import WorkerConfig, WorkerConfigError
from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime_common.execution.artifacts import (
    Execution,
    InputArtifacts,
    OutputArtifacts,
    PreparedOutputArtifact,
    WorkerParameters,
)
from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.supervisor import (
    WorkerProcessSupervisor,
    WorkerTerminationRequested,
)
from mdds_worker_runtime_common.exit_codes import WorkerExitCode
from mdds_worker_runtime_common.main import (
    WorkerOutputContractError,
    WorkerRuntime,
    build_worker_runtime_from_environment,
    save_result_message,
)
from mdds_worker_runtime_common.manifest.loader import WorkerManifestLoadError

HANDLER_IMPORT_PATH = "example_worker.handler:ExampleWorkerHandler"


@pytest.fixture
def supervisor() -> MagicMock:
    return MagicMock(spec=WorkerProcessSupervisor)


def test_worker_runtime_rejects_null_supervisor() -> None:
    with pytest.raises(
        ValueError,
        match="worker_process_supervisor cannot be null",
    ):
        WorkerRuntime(
            worker_process_supervisor=None,  # type: ignore[arg-type]
            worker_execution_context=_execution_context(),
        )


def test_worker_runtime_rejects_null_execution_context(
    supervisor: MagicMock,
) -> None:
    with pytest.raises(
        ValueError,
        match="worker_execution_context cannot be null",
    ):
        WorkerRuntime(
            worker_process_supervisor=supervisor,
            worker_execution_context=None,  # type: ignore[arg-type]
        )


def test_run_executes_successful_state_sequence(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    process = MagicMock(name="process")
    events: list[str] = []
    completion_states = iter((False, True))
    termination_checks = iter(("check_before_validation", "check_before_result_save"))

    supervisor.install_sigterm_handler.side_effect = lambda: events.append(
        "install_sigterm_handler"
    )
    supervisor.start.side_effect = lambda: events.append("start") or process

    def is_completed(_process: object) -> bool:
        events.append("is_completed")
        return next(completion_states)

    supervisor.is_completed.side_effect = is_completed
    supervisor.wait.side_effect = lambda _process: events.append("wait")
    supervisor.result.side_effect = lambda _process: events.append("result") or (
        WorkerExitCode.SUCCESS,
        "WorkerHandler execution completed.",
    )
    supervisor.raise_if_termination_requested.side_effect = lambda: events.append(
        next(termination_checks)
    )

    @contextmanager
    def normal_completion_critical_section():
        events.append("normal_completion_enter")
        yield
        events.append("normal_completion_linearized")

    supervisor.normal_completion_critical_section.side_effect = (
        normal_completion_critical_section
    )
    supervisor.close.side_effect = lambda: events.append("close")
    supervisor.restore_sigterm_handler.side_effect = lambda: events.append(
        "restore_sigterm_handler"
    )

    runtime = WorkerRuntime(supervisor, _execution_context())
    validate_outputs = MagicMock(side_effect=lambda: events.append("validate_outputs"))
    monkeypatch.setattr(runtime, "validate_outputs", validate_outputs)

    result_saver = MagicMock(
        side_effect=lambda _exit_code, _message: events.append("save_result")
    )
    monkeypatch.setattr(main_module, "save_result_message", result_saver)

    exit_code = runtime.run()

    assert exit_code is WorkerExitCode.SUCCESS
    assert events == [
        "install_sigterm_handler",
        "start",
        "is_completed",
        "wait",
        "is_completed",
        "result",
        "check_before_validation",
        "validate_outputs",
        "check_before_result_save",
        "normal_completion_enter",
        "save_result",
        "normal_completion_linearized",
        "close",
        "restore_sigterm_handler",
    ]
    result_saver.assert_called_once_with(
        WorkerExitCode.SUCCESS,
        "WorkerHandler execution completed.",
    )


def test_run_does_not_validate_outputs_after_worker_execution_failure(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    process = MagicMock(name="process")
    supervisor.start.return_value = process
    supervisor.is_completed.return_value = True
    supervisor.result.return_value = (
        WorkerExitCode.EXECUTION_FAILED,
        "Handler failed.",
    )
    supervisor.normal_completion_critical_section.return_value = nullcontext()

    runtime = WorkerRuntime(supervisor, _execution_context())
    validate_outputs = MagicMock()
    monkeypatch.setattr(runtime, "validate_outputs", validate_outputs)
    result_saver = MagicMock()
    monkeypatch.setattr(main_module, "save_result_message", result_saver)

    exit_code = runtime.run()

    assert exit_code is WorkerExitCode.EXECUTION_FAILED
    validate_outputs.assert_not_called()
    result_saver.assert_called_once_with(
        WorkerExitCode.EXECUTION_FAILED,
        "Handler failed.",
    )
    supervisor.close.assert_called_once_with()
    supervisor.restore_sigterm_handler.assert_called_once_with()


def test_run_maps_invalid_outputs_to_contract_violation(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    process = MagicMock(name="process")
    supervisor.start.return_value = process
    supervisor.is_completed.return_value = True
    supervisor.result.return_value = (
        WorkerExitCode.SUCCESS,
        "WorkerHandler execution completed.",
    )
    supervisor.normal_completion_critical_section.return_value = nullcontext()

    runtime = WorkerRuntime(supervisor, _execution_context())
    monkeypatch.setattr(
        runtime,
        "validate_outputs",
        MagicMock(
            side_effect=WorkerOutputContractError("Worker output contract violation.")
        ),
    )
    result_saver = MagicMock()
    monkeypatch.setattr(main_module, "save_result_message", result_saver)

    exit_code = runtime.run()

    assert exit_code is WorkerExitCode.WORKER_CONTRACT_VIOLATION
    result_saver.assert_called_once_with(
        WorkerExitCode.WORKER_CONTRACT_VIOLATION,
        "Worker output contract violation.",
    )


def test_run_terminates_before_supervised_process_starts(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    deadline = 123.0
    supervisor.install_sigterm_handler.side_effect = WorkerTerminationRequested
    supervisor.begin_termination_cleanup.return_value = deadline
    result_saver = MagicMock()
    monkeypatch.setattr(main_module, "save_result_message", result_saver)

    exit_code = WorkerRuntime(supervisor, _execution_context()).run()

    assert exit_code is WorkerExitCode.SIGTERM_EXIT_CODE
    supervisor.start.assert_not_called()
    supervisor.force_terminate.assert_not_called()
    result_saver.assert_called_once_with(
        WorkerExitCode.SIGTERM_EXIT_CODE,
        "Terminated by SIGTERM.",
    )
    supervisor.close.assert_called_once_with()
    supervisor.restore_sigterm_handler.assert_called_once_with()


def test_run_terminates_supervised_process_when_wait_is_interrupted(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    process = MagicMock(name="process")
    deadline = 123.0
    supervisor.start.return_value = process
    supervisor.is_completed.return_value = False
    supervisor.wait.side_effect = WorkerTerminationRequested
    supervisor.begin_termination_cleanup.return_value = deadline
    result_saver = MagicMock()
    monkeypatch.setattr(main_module, "save_result_message", result_saver)

    exit_code = WorkerRuntime(supervisor, _execution_context()).run()

    assert exit_code is WorkerExitCode.SIGTERM_EXIT_CODE
    supervisor.force_terminate.assert_called_once_with(process, deadline)
    supervisor.result.assert_not_called()
    supervisor.normal_completion_critical_section.assert_not_called()
    result_saver.assert_called_once_with(
        WorkerExitCode.SIGTERM_EXIT_CODE,
        "Terminated by SIGTERM.",
    )


def test_run_skips_output_validation_when_termination_is_accepted_after_result(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    process = MagicMock(name="process")
    deadline = 123.0
    supervisor.start.return_value = process
    supervisor.is_completed.return_value = True
    supervisor.result.return_value = (
        WorkerExitCode.SUCCESS,
        "WorkerHandler execution completed.",
    )
    supervisor.raise_if_termination_requested.side_effect = WorkerTerminationRequested
    supervisor.begin_termination_cleanup.return_value = deadline

    runtime = WorkerRuntime(supervisor, _execution_context())
    validate_outputs = MagicMock()
    monkeypatch.setattr(runtime, "validate_outputs", validate_outputs)
    result_saver = MagicMock()
    monkeypatch.setattr(main_module, "save_result_message", result_saver)

    exit_code = runtime.run()

    assert exit_code is WorkerExitCode.SIGTERM_EXIT_CODE
    validate_outputs.assert_not_called()
    supervisor.force_terminate.assert_called_once_with(process, deadline)
    supervisor.normal_completion_critical_section.assert_not_called()
    result_saver.assert_called_once_with(
        WorkerExitCode.SIGTERM_EXIT_CODE,
        "Terminated by SIGTERM.",
    )


def test_run_termination_wins_immediately_before_normal_result_save(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    process = MagicMock(name="process")
    deadline = 123.0
    supervisor.start.return_value = process
    supervisor.is_completed.return_value = True
    supervisor.result.return_value = (
        WorkerExitCode.SUCCESS,
        "WorkerHandler execution completed.",
    )
    supervisor.normal_completion_critical_section.side_effect = (
        WorkerTerminationRequested
    )
    supervisor.begin_termination_cleanup.return_value = deadline

    runtime = WorkerRuntime(supervisor, _execution_context())
    monkeypatch.setattr(runtime, "validate_outputs", MagicMock())
    result_saver = MagicMock()
    monkeypatch.setattr(main_module, "save_result_message", result_saver)

    exit_code = runtime.run()

    assert exit_code is WorkerExitCode.SIGTERM_EXIT_CODE
    supervisor.raise_if_termination_requested.assert_has_calls([call(), call()])
    supervisor.force_terminate.assert_called_once_with(process, deadline)
    result_saver.assert_called_once_with(
        WorkerExitCode.SIGTERM_EXIT_CODE,
        "Terminated by SIGTERM.",
    )


def test_run_restores_sigterm_handler_when_supervisor_close_fails(
    monkeypatch: pytest.MonkeyPatch,
    supervisor: MagicMock,
) -> None:
    process = MagicMock(name="process")
    supervisor.start.return_value = process
    supervisor.is_completed.return_value = True
    supervisor.result.return_value = (
        WorkerExitCode.EXECUTION_FAILED,
        "Handler failed.",
    )
    supervisor.normal_completion_critical_section.return_value = nullcontext()
    supervisor.close.side_effect = RuntimeError("Cannot close supervisor.")
    monkeypatch.setattr(main_module, "save_result_message", MagicMock())

    with pytest.raises(RuntimeError, match="Cannot close supervisor"):
        WorkerRuntime(supervisor, _execution_context()).run()

    supervisor.restore_sigterm_handler.assert_called_once_with()


def test_validate_outputs_accepts_regular_files(
    tmp_path: Path,
    supervisor: MagicMock,
) -> None:
    first_output = tmp_path / "outputs" / "first.json"
    second_output = tmp_path / "outputs" / "second.json"
    first_output.parent.mkdir()
    first_output.write_text("1\n", encoding="utf-8")
    second_output.write_text("2\n", encoding="utf-8")

    runtime = WorkerRuntime(
        supervisor,
        _execution_context(
            first=first_output,
            second=second_output,
        ),
    )

    runtime.validate_outputs()


def test_validate_outputs_reports_all_missing_and_non_regular_outputs(
    tmp_path: Path,
    supervisor: MagicMock,
) -> None:
    missing_output = tmp_path / "outputs" / "missing.json"
    directory_output = tmp_path / "outputs" / "directory"
    directory_output.mkdir(parents=True)
    runtime = WorkerRuntime(
        supervisor,
        _execution_context(
            missing=missing_output,
            directory=directory_output,
        ),
    )

    with pytest.raises(WorkerOutputContractError) as error:
        runtime.validate_outputs()

    assert str(error.value) == (
        "Worker output contract violation: "
        f"output slot 'missing' was not produced at '{missing_output}'; "
        "output slot 'directory' does not reference a regular file: "
        f"'{directory_output}'."
    )


def test_validate_outputs_wraps_output_inspection_error(
    supervisor: MagicMock,
) -> None:
    output_path = MagicMock(spec=Path)
    output_path.__str__.return_value = "/outputs/result.json"
    output_path.stat.side_effect = PermissionError("Permission denied.")
    context = MagicMock()
    context.outputs.items.return_value = [
        ("result", SimpleNamespace(local_path=output_path))
    ]
    runtime = WorkerRuntime(supervisor, context)

    with pytest.raises(
        WorkerOutputContractError,
        match=(
            "Cannot inspect output slot 'result' at "
            "'/outputs/result.json': Permission denied"
        ),
    ) as error:
        runtime.validate_outputs()

    assert isinstance(error.value.__cause__, PermissionError)


def test_build_worker_runtime_from_environment_wires_collaborators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = WorkerConfig(
        worker_name="example-worker",
        worker_version="1.0.0",
        worker_handler=HANDLER_IMPORT_PATH,
        argo_retry_index=0,
    )
    manifest = MagicMock(name="manifest")
    context = MagicMock(spec=WorkerExecutionContext)
    process_context = MagicMock(name="process_context")
    supervisor = MagicMock(spec=WorkerProcessSupervisor)
    runtime = MagicMock(spec=WorkerRuntime)

    load_config = MagicMock(return_value=config)
    load_manifest = MagicMock(return_value=manifest)
    create_context = MagicMock(return_value=context)
    persist_context = MagicMock()
    get_context = MagicMock(return_value=process_context)
    supervisor_type = MagicMock(return_value=supervisor)
    runtime_type = MagicMock(return_value=runtime)

    monkeypatch.setattr(main_module, "load_config", load_config)
    monkeypatch.setattr(main_module, "load_worker_manifest", load_manifest)
    monkeypatch.setattr(
        main_module,
        "create_worker_execution_context",
        create_context,
    )
    monkeypatch.setattr(main_module, "persist", persist_context)
    monkeypatch.setattr(main_module.mp, "get_context", get_context)
    monkeypatch.setattr(main_module, "WorkerProcessSupervisor", supervisor_type)
    monkeypatch.setattr(main_module, "WorkerRuntime", runtime_type)

    actual_runtime = build_worker_runtime_from_environment()

    assert actual_runtime is runtime
    load_config.assert_called_once_with()
    load_manifest.assert_called_once_with()
    create_context.assert_called_once_with(manifest, config)
    persist_context.assert_called_once_with(context)
    get_context.assert_called_once_with("spawn")
    supervisor_type.assert_called_once_with(
        handler_import_path=HANDLER_IMPORT_PATH,
        process_context=process_context,
    )
    runtime_type.assert_called_once_with(
        worker_process_supervisor=supervisor,
        worker_execution_context=context,
    )


def test_save_result_message_atomically_replaces_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result_path = tmp_path / "result" / "result.json"
    result_path.parent.mkdir()
    result_path.write_text('{"old": true}\n', encoding="utf-8")
    temporary_path = result_path.with_suffix(".json.tmp")
    monkeypatch.setattr(main_module, "CANONICAL_WORKER_RESULT_PATH", result_path)

    save_result_message(
        WorkerExitCode.SUCCESS, "Ð’Ñ‹Ð¿Ð¾Ð»Ð½ÐµÐ½Ð¸Ðµ Ð·Ð°Ð²ÐµÑ€ÑˆÐµÐ½Ð¾."
    )

    assert json.loads(result_path.read_text(encoding="utf-8")) == {
        "exitCode": WorkerExitCode.SUCCESS,
        "message": "Ð’Ñ‹Ð¿Ð¾Ð»Ð½ÐµÐ½Ð¸Ðµ Ð·Ð°Ð²ÐµÑ€ÑˆÐµÐ½Ð¾.",
    }
    assert not temporary_path.exists()


def test_save_result_message_removes_temporary_file_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result_path = tmp_path / "result" / "result.json"
    result_path.parent.mkdir()
    result_path.write_text('{"old": true}\n', encoding="utf-8")
    temporary_path = result_path.with_suffix(".json.tmp")
    monkeypatch.setattr(main_module, "CANONICAL_WORKER_RESULT_PATH", result_path)

    def fail_replace(_path: Path, _target: Path) -> Path:
        raise OSError("Atomic replace failed.")

    monkeypatch.setattr(Path, "replace", fail_replace)

    with pytest.raises(OSError, match="Atomic replace failed"):
        save_result_message(WorkerExitCode.EXECUTION_FAILED, "Handler failed.")

    assert json.loads(result_path.read_text(encoding="utf-8")) == {"old": True}
    assert not temporary_path.exists()


def test_main_returns_runtime_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_logging = MagicMock()
    runtime = MagicMock(spec=WorkerRuntime)
    runtime.run.return_value = WorkerExitCode.SUCCESS
    build_runtime = MagicMock(return_value=runtime)
    monkeypatch.setattr(main_module, "setup_logging", setup_logging)
    monkeypatch.setattr(
        main_module,
        "build_worker_runtime_from_environment",
        build_runtime,
    )

    exit_code = main_module.main()

    assert exit_code is WorkerExitCode.SUCCESS
    setup_logging.assert_called_once_with()
    build_runtime.assert_called_once_with()
    runtime.run.assert_called_once_with()


@pytest.mark.parametrize(
    "error",
    [
        WorkerConfigError("Invalid Worker configuration."),
        WorkerManifestLoadError("Invalid Worker Manifest."),
    ],
)
def test_main_maps_setup_contract_error_to_contract_violation(
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "setup_logging", MagicMock())
    monkeypatch.setattr(
        main_module,
        "build_worker_runtime_from_environment",
        MagicMock(side_effect=error),
    )

    exit_code = main_module.main()

    assert exit_code is WorkerExitCode.WORKER_CONTRACT_VIOLATION


def test_main_maps_unexpected_runtime_error_to_execution_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = MagicMock(spec=WorkerRuntime)
    runtime.run.side_effect = RuntimeError("Unexpected runtime failure.")
    monkeypatch.setattr(main_module, "setup_logging", MagicMock())
    monkeypatch.setattr(
        main_module,
        "build_worker_runtime_from_environment",
        MagicMock(return_value=runtime),
    )

    exit_code = main_module.main()

    assert exit_code is WorkerExitCode.EXECUTION_FAILED


def test_module_entry_point_exits_with_main_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(logging_config_module, "setup_logging", MagicMock())
    monkeypatch.setattr(
        config_module,
        "load_config",
        MagicMock(side_effect=WorkerConfigError("Invalid configuration.")),
    )

    with pytest.raises(SystemExit) as error:
        runpy.run_path(main_module.__file__, run_name="__main__")

    assert error.value.code is WorkerExitCode.WORKER_CONTRACT_VIOLATION


def _execution_context(**outputs: Path) -> WorkerExecutionContext:
    return WorkerExecutionContext(
        execution=Execution(
            user_id=12345,
            dag_run_id="dag-run-1",
            node_id="node-1",
            attempt_id="attempt-0",
        ),
        inputs=InputArtifacts({}),
        params=WorkerParameters({}),
        outputs=OutputArtifacts(
            {
                slot: PreparedOutputArtifact(
                    local_path=path,
                    format=ArtifactFormat.JSON,
                )
                for slot, path in outputs.items()
            }
        ),
    )
