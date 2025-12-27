# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import os
import time
import signal
import multiprocessing as mp
import unittest
import numpy as np
from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver

min_solving_interval = 4
max_matrix_size = 10000


def find_matrix_size_and_solve_interval() -> tuple[int, float]:
    print("[parent] looking for matrix size and solve interval")
    n = 10
    while True:
        matrix = np.random.rand(n, n).astype(np.float64).tolist()
        rhs = np.random.rand(n).astype(np.float64).tolist()
        interval = find_time_of_solving(matrix, rhs) / 1000000000
        print(
            f"[parent] solved equation with {n} x {n} for {interval} seconds",
            flush=True,
        )
        if interval > min_solving_interval:
            break
        n = round(1.4 * n)
        if n > max_matrix_size:
            raise AssertionError(
                f"Could not reach {min_solving_interval}s before MAX_N={max_matrix_size}"
            )
    return n, interval


def find_time_of_solving(matrix: list[list[float]], rhs: list[float]) -> int:
    start_time_ns = time.perf_counter_ns()
    solver = NumpyExactSolver()
    solver.solve(matrix, rhs)
    end_time_ns = time.perf_counter_ns()
    duration_ns = end_time_ns - start_time_ns
    return duration_ns


def solve_job(n: int):
    print(f"[child] pid={os.getpid()} starting solve for n={n}", flush=True)

    # Generate random data for SLAE
    matrix = np.random.rand(n, n).astype(np.float64).tolist()
    rhs = np.random.rand(n).astype(np.float64).tolist()

    # Long time call for solving
    solver = NumpyExactSolver()
    x = solver.solve(matrix, rhs)
    print(f"[child] done, x0={x[0]}", flush=True)


def test_numpy_exact_solver():
    # Create multithreading context
    ctx = mp.get_context("spawn")
    n, interval = find_matrix_size_and_solve_interval()
    print(f"[parent] matrix size = {n}, solve interval = {interval}s", flush=True)
    p = ctx.Process(target=solve_job, args=(n,), daemon=True)
    print("[parent] starting solve job in a single thread")
    p.start()
    print(f"[parent] child pid={p.pid}", flush=True)
    # Wait for some time to start the job
    print(f"[parent] sleeping for {interval / 2}s", flush=True)
    time.sleep(interval / 2)
    # Check that process is still running to force stop it after that
    proc_path = f"/proc/{p.pid}"
    exists = os.path.exists(proc_path)
    print(f"[parent] /proc/{p.pid} exists? {exists}", flush=True)
    assert exists, "exists should be True"
    # Force termination of the job
    print("[parent] sending SIGTERM...", flush=True)
    os.kill(p.pid, signal.SIGTERM)
    # If we can not stop job with SIGTERM, we try with SIGKILL
    p.join(timeout=3)
    if p.is_alive():
        print("[parent] still alive -> SIGKILL", flush=True)
        os.kill(p.pid, signal.SIGKILL)
        p.join(timeout=3)

    print(f"[parent] exitcode={p.exitcode}", flush=True)
    proc_path = f"/proc/{p.pid}"
    exists = os.path.exists(proc_path)
    print(f"[parent] /proc/{p.pid} exists? {exists}", flush=True)
    assert not exists, "exists should be False or falsy"


if __name__ == "__main__":
    unittest.main()
