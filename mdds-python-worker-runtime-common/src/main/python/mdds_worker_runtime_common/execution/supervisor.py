# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import signal
import time
from contextlib import contextmanager
from enum import Enum, auto
from multiprocessing.connection import Connection
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess
from types import FrameType

from mdds_worker_runtime_common.execution.context import load_worker_execution_context
from mdds_worker_runtime_common.execution.handler_loader import (
    load_worker_handler,
    WorkerHandlerLoadError,
)
from mdds_worker_runtime_common.logging_config import setup_logging


class WorkerTerminationRequested(Exception):
    """Interrupt normal Runtime execution after an accepted SIGTERM."""


class WorkerRuntimePhase(Enum):
    RUNNING = auto()
    TERMINATION_REQUESTED = auto()
    NORMAL_LINEARIZED = auto()


TERMINATION_CLEANUP_TIMEOUT_SECONDS = 1.0


class WorkerProcessSupervisor:

    def __init__(
        self,
        handler_import_path: str,
        process_context: BaseContext,
    ) -> None:
        if handler_import_path is None or handler_import_path.strip() == "":
            raise ValueError("handler_import_path cannot be null or blank.")
        if process_context is None:
            raise ValueError("process_context cannot be null.")
        self._process_context = process_context
        self._phase = WorkerRuntimePhase.RUNNING
        self._previous_sigterm_handler = None
        self._termination_deadline: float | None = None
        self._handler_import_path = handler_import_path
        self._process: BaseProcess | None = None
        self._result_connection: Connection | None = None

    def __enter__(self) -> "WorkerProcessSupervisor":
        self.install_sigterm_handler()
        return self

    def __exit__(self, *_args: object) -> None:
        try:
            self.close(self._termination_deadline)
        finally:
            self.restore_sigterm_handler()

    def install_sigterm_handler(self) -> None:
        """Install the Runtime SIGTERM handler, preserving the previous handler."""
        self._previous_sigterm_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def restore_sigterm_handler(self) -> None:
        """Restore the handler that was active before Runtime execution."""
        if self._previous_sigterm_handler is not None:
            signal.signal(signal.SIGTERM, self._previous_sigterm_handler)
            self._previous_sigterm_handler = None

    def _handle_sigterm(
        self,
        _signal_number: int,
        _frame: FrameType | None,
    ) -> None:
        if self._phase is not WorkerRuntimePhase.RUNNING:
            return

        self._phase = WorkerRuntimePhase.TERMINATION_REQUESTED
        raise WorkerTerminationRequested

    def is_termination_requested(self) -> bool:
        return self._phase is WorkerRuntimePhase.TERMINATION_REQUESTED

    def raise_if_termination_requested(self) -> None:
        if self.is_termination_requested():
            raise WorkerTerminationRequested

    @contextmanager
    def normal_completion_critical_section(self):
        """Block SIGTERM while publishing and linearizing a normal result."""
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
        try:
            self.raise_if_termination_requested()
            yield
            self._phase = WorkerRuntimePhase.NORMAL_LINEARIZED
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)

    def begin_termination_cleanup(self) -> float:
        """Return the single deadline shared by all termination cleanup steps."""
        if self._termination_deadline is None:
            self._termination_deadline = (
                time.monotonic() + TERMINATION_CLEANUP_TIMEOUT_SECONDS
            )
        return self._termination_deadline

    def close(self, deadline: float | None = None) -> None:
        if deadline is None:
            deadline = self._termination_deadline

        process = self._process
        result_connection = self._result_connection

        try:
            self._close_process(process, deadline)
        finally:
            if result_connection is not None:
                result_connection.close()

            # Make close() idempotent.
            self._process = None
            self._result_connection = None

    @classmethod
    def _close_process(
        cls,
        process: BaseProcess | None,
        deadline: float | None,
    ) -> None:
        if process is None or not cls._is_started(process):
            return

        if deadline is not None:
            cls.force_terminate(process, deadline)
            if process.exitcode is not None:
                process.close()
            return

        if process.exitcode is None:
            process.kill()
        process.join()
        process.close()

    def execute_worker_handler(
        self,
        result_connection: Connection,
    ) -> None:
        exit_code = 1
        message = "WorkerHandler execution failed."

        try:
            setup_logging()
            context = load_worker_execution_context()

            try:
                handler = load_worker_handler(self._handler_import_path)
            except WorkerHandlerLoadError as error:
                exit_code = 2
                message = str(error).strip() or "WorkerHandler loading failed."
            else:
                try:
                    handler.execute(context)
                except (Exception, KeyboardInterrupt) as error:
                    message = str(error).strip() or message
                else:
                    exit_code = 0
                    message = "WorkerHandler execution completed."
        except (Exception, KeyboardInterrupt) as error:
            message = str(error).strip() or message
        finally:
            try:
                result_connection.send(
                    (
                        exit_code,
                        limit_result_message(message),
                    )
                )
            finally:
                result_connection.close()

        raise SystemExit(exit_code)

    def start(self) -> BaseProcess:
        receive_connection, send_connection = self._process_context.Pipe(duplex=False)

        process = self._process_context.Process(
            target=self.execute_worker_handler,
            args=(send_connection,),
            name="mdds-worker-handler",
        )

        self._result_connection = receive_connection
        self._process = process

        try:
            process.start()
        finally:
            send_connection.close()

        return process

    def result(
        self,
        process: BaseProcess,
    ) -> tuple[int, str]:
        if process.exitcode is None:
            raise RuntimeError("Worker process is still running.")

        try:
            if self._result_connection.poll():
                reported_exit_code, message = self._result_connection.recv()

                if reported_exit_code == process.exitcode:
                    return reported_exit_code, message

                return (
                    1,
                    "Worker process reported an inconsistent exit code.",
                )
        except (EOFError, OSError):
            pass

        if process.exitcode < 0:
            signal_number = -process.exitcode
            return (
                128 + signal_number,
                f"Worker process was terminated by signal {signal_number}.",
            )

        return (
            process.exitcode or 1,
            "Worker process completed without reporting a result.",
        )

    @staticmethod
    def wait(
        process: BaseProcess,
    ) -> None:
        process.join()

    @staticmethod
    def is_completed(process: BaseProcess) -> bool:
        return process.exitcode is not None

    @classmethod
    def force_terminate(cls, process: BaseProcess, deadline: float) -> None:
        """Send SIGKILL and wait no longer than the supplied deadline."""
        if not cls._is_started(process):
            return

        if process.exitcode is None:
            try:
                process.kill()  # SIGKILL for Linux
            except ProcessLookupError:
                # The process completed between the exit-code check and SIGKILL.
                pass

        remaining_seconds = max(0.0, deadline - time.monotonic())
        process.join(timeout=remaining_seconds)

    @staticmethod
    def _is_started(process: BaseProcess) -> bool:
        return process.pid is not None


MAX_WORKER_RESULT_MESSAGE_BYTES = 4 * 1024
TRUNCATION_SUFFIX = "... [truncated]"


def limit_result_message(message: str) -> str:
    """Limit a result message without splitting a UTF-8 character."""
    encoded_message = message.encode("utf-8", errors="replace")

    if len(encoded_message) <= MAX_WORKER_RESULT_MESSAGE_BYTES:
        return encoded_message.decode("utf-8")

    encoded_suffix = TRUNCATION_SUFFIX.encode("ascii")
    prefix_byte_limit = MAX_WORKER_RESULT_MESSAGE_BYTES - len(encoded_suffix)

    truncated_prefix = encoded_message[:prefix_byte_limit].decode(
        "utf-8", errors="ignore"
    )

    return truncated_prefix + TRUNCATION_SUFFIX
