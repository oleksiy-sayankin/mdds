# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Python Worker Runtime Common entry point.

This module is the composition root for Python Worker Runtime Common.

It wires generic runtime infrastructure:

- worker configuration;
- manifest loading;
- execution context creation;
- dynamic WorkerHandler loading.

The module intentionally does not contain worker-specific business logic.
Concrete worker behavior is delegated to the dynamically loaded WorkerHandler.
"""

import json
import logging
import stat
import multiprocessing as mp
from pathlib import Path


from mdds_worker_runtime_common.config import WorkerConfigError, load_config
from mdds_worker_runtime_common.execution.context import (
    WorkerExecutionContext,
    create_worker_execution_context,
    persist,
)

from mdds_worker_runtime_common.execution.supervisor import (
    WorkerProcessSupervisor,
    WorkerTerminationRequested,
)
from mdds_worker_runtime_common.exit_codes import WorkerExitCode
from mdds_worker_runtime_common.logging_config import setup_logging
from mdds_worker_runtime_common.manifest.loader import (
    WorkerManifestLoadError,
    load_worker_manifest,
)

logger = logging.getLogger(__name__)
CANONICAL_WORKER_RESULT_PATH = Path("/opt/mdds/result/result.json")


class WorkerOutputContractError(RuntimeError):
    """Declared Worker outputs violate the execution contract."""


class WorkerRuntime:
    """Execute one WorkerHandler for one Argo node attempt."""

    def __init__(
        self,
        worker_process_supervisor: WorkerProcessSupervisor,
        worker_execution_context: WorkerExecutionContext,
    ) -> None:
        if worker_process_supervisor is None:
            raise ValueError("worker_process_supervisor cannot be null.")
        if worker_execution_context is None:
            raise ValueError("worker_execution_context cannot be null.")

        self._worker_process_supervisor = worker_process_supervisor
        self._worker_execution_context = worker_execution_context

    def run(self) -> int:
        supervisor = self._worker_process_supervisor
        process = None

        try:
            supervisor.install_sigterm_handler()
            process = supervisor.start()

            while True:
                if supervisor.is_completed(process):
                    exit_code, message = supervisor.result(process)
                    break

                supervisor.wait(
                    process,
                )

            supervisor.raise_if_termination_requested()

            if exit_code == WorkerExitCode.SUCCESS:
                try:
                    self.validate_outputs()
                except WorkerOutputContractError as error:
                    exit_code = WorkerExitCode.WORKER_CONTRACT_VIOLATION
                    message = str(error)

            supervisor.raise_if_termination_requested()

            with supervisor.normal_completion_critical_section():
                save_result_message(exit_code, message)
            return exit_code
        except WorkerTerminationRequested:
            deadline = supervisor.begin_termination_cleanup()
            if process is not None:
                supervisor.force_terminate(process, deadline)
            exit_code = WorkerExitCode.SIGTERM_EXIT_CODE
            save_result_message(exit_code, "Terminated by SIGTERM.")
            return exit_code
        finally:
            try:
                supervisor.close()
            finally:
                supervisor.restore_sigterm_handler()

    def validate_outputs(self) -> None:
        """Validate that every declared output is a regular local file."""

        violations: list[str] = []

        for slot, artifact in self._worker_execution_context.outputs.items():
            output_path = artifact.local_path

            try:
                path_status = output_path.stat()
            except FileNotFoundError:
                violations.append(
                    f"output slot '{slot}' was not produced at '{output_path}'"
                )
            except OSError as exc:
                raise WorkerOutputContractError(
                    f"Cannot inspect output slot '{slot}' at '{output_path}': {exc}"
                ) from exc
            else:
                if not stat.S_ISREG(path_status.st_mode):
                    violations.append(
                        f"output slot '{slot}' does not reference a regular file: '{output_path}'"
                    )

        if violations:
            raise WorkerOutputContractError(
                "Worker output contract violation: " + "; ".join(violations) + "."
            )

        logger.info(
            "Worker output contract validated.",
            extra={
                "component": "worker_runtime",
                "event": "worker_runtime_outputs_validated",
            },
        )


def build_worker_runtime_from_environment() -> WorkerRuntime:
    """Build WorkerRuntime from environment-backed configuration.

    This function is intentionally kept separate from main() so tests can patch
    collaborators and verify composition without running Worker logic.
    """
    config = load_config()
    manifest = load_worker_manifest()
    context = create_worker_execution_context(manifest, config)
    persist(context)
    worker_handler = config.worker_handler
    process_context = mp.get_context("spawn")
    worker_process_supervisor = WorkerProcessSupervisor(
        handler_import_path=worker_handler, process_context=process_context
    )
    return WorkerRuntime(
        worker_process_supervisor=worker_process_supervisor,
        worker_execution_context=context,
    )


def save_result_message(
    exit_code: int,
    message: str,
) -> None:
    """Atomically persist the final Worker execution result."""
    result = {
        "exitCode": int(exit_code),
        "message": message,
    }
    result_path = CANONICAL_WORKER_RESULT_PATH
    temporary_path = result_path.with_suffix(f"{result_path.suffix}.tmp")
    result_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        temporary_path.write_text(
            json.dumps(
                result,
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(result_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    logger.info(
        "Worker execution result saved to '%s'.",
        result_path,
        extra={
            "component": "worker_runtime",
            "event": "worker_runtime_result_saved",
        },
    )


def main() -> int:
    setup_logging()

    try:
        runtime = build_worker_runtime_from_environment()
    except (
        WorkerConfigError,
        WorkerManifestLoadError,
    ):
        logger.exception("Worker Runtime contract validation failed.")
        return WorkerExitCode.WORKER_CONTRACT_VIOLATION

    try:
        return runtime.run()
    except Exception:
        logger.exception("Worker Runtime execution failed.")
        return WorkerExitCode.EXECUTION_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
