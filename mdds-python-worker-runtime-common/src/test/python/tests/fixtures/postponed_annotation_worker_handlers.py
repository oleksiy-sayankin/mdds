# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from typing import TYPE_CHECKING

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.handler import WorkerHandler

if TYPE_CHECKING:

    class MissingWorkerExecutionContext:
        pass


class PostponedAnnotationsWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class PostponedWrongContextAnnotationWorkerHandler(WorkerHandler):
    def execute(self, context: str) -> None:
        pass


class PostponedWrongReturnAnnotationWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> str:
        return "wrong"


# Intentionally unresolved annotation. The module remains importable because
# postponed annotations are enabled, but handler loading must fail while
# resolving the method signature with inspect.signature(..., eval_str=True).
class PostponedUnresolvableContextAnnotationWorkerHandler(WorkerHandler):
    def execute(self, context: MissingWorkerExecutionContext) -> None:
        pass
