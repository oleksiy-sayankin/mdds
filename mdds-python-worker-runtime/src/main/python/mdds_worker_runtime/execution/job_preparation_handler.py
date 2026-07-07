# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass

from mdds_worker_runtime.execution.artifacts import InputArtifactPreparer
from mdds_worker_runtime.execution.context import (
    JobExecutionContext,
    JobExecutionContextFactory,
)
from mdds_worker_runtime.execution.handler import JobHandler
from mdds_worker_runtime.execution.handler_loader import JobHandlerLoader
from mdds_worker_runtime.execution.models import JobWorkspace
from mdds_worker_runtime.execution.registry import ExecutionRegistry


@dataclass(frozen=True)
class PreparedJob:
    """Job prepared for worker-side semantic validation and execution."""

    context: JobExecutionContext
    handler: JobHandler


class JobPreparationHandler:
    """Prepares a submitted job after manifest identity is known.

    Manifest loading stays outside this component because before manifest is
    loaded the Worker cannot reliably determine jobId, userId, and jobType.

    This handler has a single responsibility: prepare runtime-local inputs,
    create JobExecutionContext, and load the concrete JobHandler.

    It must not publish lifecycle statuses, acknowledge or reject queue
    messages, clean up terminal state, or decide which WorkerJobStatus should be
    committed on failure. Preparation failures are intentionally propagated to
    JobConsumer so that the job state transition coordinator can perform a
    separate coordinator-owned terminal transition such as SUBMITTED -> ERROR.
    """

    def __init__(
        self,
        execution_registry: ExecutionRegistry,
        input_artifact_preparer: InputArtifactPreparer,
        context_factory: JobExecutionContextFactory,
        job_handler_loader: JobHandlerLoader,
    ) -> None:
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if input_artifact_preparer is None:
            raise ValueError("input_artifact_preparer cannot be null.")
        if context_factory is None:
            raise ValueError("context_factory cannot be null.")
        if job_handler_loader is None:
            raise ValueError("job_handler_loader cannot be null.")

        self._execution_registry = execution_registry
        self._input_artifact_preparer = input_artifact_preparer
        self._context_factory = context_factory
        self._job_handler_loader = job_handler_loader

    def prepare(
        self,
        *,
        workspace: JobWorkspace,
    ) -> PreparedJob:
        """Prepare local runtime context and concrete handler for a submitted job.

        This method is intended to run inside the coordinator-owned happy-path
        transition SUBMITTED -> INPUTS_PREPARED. It performs only the preparation
        work needed for that transition:

        * downloads declared input artifacts into the local job workspace;
        * creates JobExecutionContext;
        * loads the concrete JobHandler.

        Any exception is propagated unchanged. The caller must let the
        coordinator keep the worker-local state unchanged and then request a
        separate terminal transition, typically SUBMITTED -> ERROR.
        """
        if workspace is None:
            raise ValueError("workspace cannot be null.")

        prepared_job_inputs = self._input_artifact_preparer.prepare(
            workspace,
        )

        context = self._context_factory.create(
            workspace=workspace,
            prepared_job_inputs=prepared_job_inputs,
        )
        self._execution_registry.attach_context(
            job_id=workspace.job_id, context=context
        )
        handler = self._job_handler_loader.load()

        return PreparedJob(
            context=context,
            handler=handler,
        )
