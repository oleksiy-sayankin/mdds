# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Run the production entry point with test-owned filesystem paths.

This module is also imported as ``__mp_main__`` by multiprocessing spawn.
Consequently, the context snapshot override is applied in both the Worker
Runtime process and its supervised WorkerHandler process.
"""

import os
from functools import partial
from pathlib import Path

from mdds_worker_runtime_common import main as worker_runtime_main
from mdds_worker_runtime_common.execution import context as context_module
from mdds_worker_runtime_common.manifest.loader import load_worker_manifest

TEST_MANIFEST_PATH_ENV = "MDDS_TEST_WORKER_MANIFEST_PATH"
TEST_CONTEXT_PATH_ENV = "MDDS_TEST_WORKER_CONTEXT_PATH"
TEST_RESULT_PATH_ENV = "MDDS_TEST_WORKER_RESULT_PATH"


def _required_path(environment_variable: str) -> Path:
    value = os.environ.get(environment_variable)
    if value is None or value.strip() == "":
        raise RuntimeError(
            f"Required test environment variable '{environment_variable}' "
            "is missing."
        )
    return Path(value)


def _configure_test_paths() -> None:
    context_module.WORKER_EXECUTION_CONTEXT_SNAPSHOT_PATH = _required_path(
        TEST_CONTEXT_PATH_ENV
    )
    worker_runtime_main.CANONICAL_WORKER_RESULT_PATH = _required_path(
        TEST_RESULT_PATH_ENV
    )
    worker_runtime_main.load_worker_manifest = partial(
        load_worker_manifest,
        _required_path(TEST_MANIFEST_PATH_ENV),
    )


if __name__ in {"__main__", "__mp_main__"}:
    _configure_test_paths()


if __name__ == "__main__":
    raise SystemExit(worker_runtime_main.main())
