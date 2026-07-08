# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from typing import TYPE_CHECKING

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler

if TYPE_CHECKING:

    class MissingJobExecutionContext:
        pass


class PostponedAnnotationsJobHandler(JobHandler):
    def execute(self, context: JobExecutionContext) -> None:
        pass


class PostponedWrongContextAnnotationJobHandler(JobHandler):
    def execute(self, context: str) -> None:
        pass


class PostponedWrongReturnAnnotationJobHandler(JobHandler):
    def execute(self, context: JobExecutionContext) -> str:
        return "wrong"


# Intentionally unresolved annotation.
#
# With "from __future__ import annotations" the module remains importable,
# but JobHandlerLoader must fail when resolving this annotation with
# inspect.signature(..., eval_str=True).
class PostponedUnresolvableContextAnnotationJobHandler(JobHandler):
    def execute(self, context: MissingJobExecutionContext) -> None:
        pass
