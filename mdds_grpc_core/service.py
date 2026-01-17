# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import threading
import logging
import multiprocessing as mp
import time

from dictionary import ThreadSafeDictionary
from generated import solver_pb2, solver_pb2_grpc
from dataclasses import dataclass, field


@dataclass
class Job:
    """
    Job with multithreading process and its status.
    """

    process: mp.Process
    grpcTaskStatus: int
    solution: list[float]
    taskMessage: str
    delivered: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


# Mapping solver name -> Python class
SOLVER_MAP = {
    "numpy_exact_solver": "NumpyExactSolver",
    "numpy_lstsq_solver": "NumpyLstsqSolver",
    "numpy_pinv_solver": "NumpyPinvSolver",
    "petsc_solver": "PetscSolver",
    "scipy_gmres_solver": "ScipyGmresSolver",
}

GrpcTaskStatus = solver_pb2.GrpcTaskStatus
DONE = int(GrpcTaskStatus.DONE)
ERROR = int(GrpcTaskStatus.ERROR)
CANCELLED = int(GrpcTaskStatus.CANCELLED)
IN_PROGRESS = int(GrpcTaskStatus.IN_PROGRESS)

TERMINAL = {DONE, ERROR, CANCELLED}

RequestStatus = solver_pb2.RequestStatus
DECLINED = RequestStatus.DECLINED
COMPLETED = RequestStatus.COMPLETED

logging.basicConfig(
    filename="SolverService.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def solve_job(matrix, rhs, method, conn):
    """
    Solves system of linear algebraic equations and sends result to connection

    :param matrix: matrix of coefficients (2D, dense or sparse) for SLAE
    :param rhs: right-hand side vector
    :param method: method to solve
    :param conn: connection to parent process
    :return: solution x of SLAE, job status and error message if any
    """
    try:
        # Load solver from module by method name
        module = __import__(
            f"slae_solver.solvers.{method}", fromlist=[SOLVER_MAP[method]]
        )
        solver_class = getattr(module, SOLVER_MAP[method])
        solver = solver_class()

        # Solve system of linear algebraic equations
        solution = solver.solve(matrix, rhs)

        # Send solution x to connection
        conn.send(
            (
                DONE,
                solution,
                "Solved system of linear algebraic equations",
            )
        )
    except Exception as e:
        conn.send((ERROR, [], f"{type(e).__name__}: {e}"))
    finally:
        conn.close()


def terminate_process(proc: mp.Process, timeout: float = 3.0) -> None:
    """Try graceful terminate first, then hard kill."""
    if proc is None:
        return
    if proc.is_alive():
        proc.terminate()  # SIGTERM
        proc.join(timeout=timeout)
    if proc.is_alive():
        proc.kill()  # SIGKILL
        proc.join(timeout=timeout)


active = ThreadSafeDictionary()  # list of active jobs

ctx = mp.get_context("spawn")  # Create multi process context


class SolverService(solver_pb2_grpc.SolverServiceServicer):
    """
    gRPC Service that takes solve request (matrix of coefficients, right hand side and solve method),
    converts Protobuf data to Python data, solves system of linear algebraic equations and send the
    result as Protobuf data back to the caller.
    """

    def SubmitTask(self, request, context):
        task_id = request.taskId
        if not task_id:
            return solver_pb2.SubmitTaskResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Task id is invalid: empty or null",
            )

        if active.get(task_id):
            return solver_pb2.SubmitTaskResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Task already submitted",
            )

        method = request.method
        if method not in SOLVER_MAP:
            return solver_pb2.SubmitTaskResponse(
                requestStatus=DECLINED,
                requestStatusDetails=f"Unknown method: {method}",
            )

        # Convert data
        matrix = [list(row.values) for row in request.matrix]
        rhs = list(request.rhs)

        parent_conn, child_conn = ctx.Pipe(duplex=False)
        # Start separate process to solve SLAE
        process = ctx.Process(target=solve_job, args=(matrix, rhs, method, child_conn))
        process.start()
        child_conn.close()

        # Save job to dictionary
        active.put(
            task_id,
            Job(
                process,
                IN_PROGRESS,
                [],
                "Task submitted and is in progress",
            ),
        )

        # Start watcher thread. In this watcher we periodically update `active` dictionary, getting
        # results (if any) from the solve process.
        watcher = threading.Thread(
            target=watch_job_result,
            args=(task_id, parent_conn, process),
            daemon=True,
        )
        watcher.start()

        # This cleaner watches if a task result was delivered and if so, removes job from dictionary
        cleaner = threading.Thread(
            target=clean_delivered_job, args=(task_id, process), daemon=True
        )
        cleaner.start()

        return solver_pb2.SubmitTaskResponse(
            requestStatus=COMPLETED,
            requestStatusDetails=f"Successfully submitted job for task: {task_id}",
        )

    def CancelTask(self, request, context):
        task_id = request.taskId
        if not task_id:
            return solver_pb2.CancelTaskResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Task id is empty",
            )
        job = active.get(task_id, None)
        if job is None:
            return solver_pb2.CancelTaskResponse(
                requestStatus=DECLINED,
                requestStatusDetails=f"Task {task_id} is not found. Total active tasks count: {active.size()}",
            )
        with job.lock:
            if job.grpcTaskStatus != IN_PROGRESS:
                return solver_pb2.CancelTaskResponse(
                    requestStatus=DECLINED,
                    requestStatusDetails=f"Task {task_id} is not in IN_PROGRESS state. Task status is {job.grpcTaskStatus}",
                )
            # mark that process is terminated intentionally and not by error
            job.grpcTaskStatus = CANCELLED
            job.taskMessage = "Cancelled by request"

        terminate_process(job.process)
        return solver_pb2.CancelTaskResponse(
            requestStatus=COMPLETED,
            requestStatusDetails=f"Task {task_id} is cancelled",
        )

    def GetTaskStatus(self, request, context):
        task_id = request.taskId
        if not task_id:
            return solver_pb2.GetTaskStatusResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Task id is empty",
            )
        job = active.get(task_id, None)
        if job is None:
            return solver_pb2.GetTaskStatusResponse(
                requestStatus=DECLINED,
                requestStatusDetails=f"Task {task_id} is not found. Total active tasks count: {active.size()}",
            )
        with job.lock:
            grpc_task_status = job.grpcTaskStatus
            solution = job.solution
            task_message = job.taskMessage
            if grpc_task_status in TERMINAL:
                job.delivered = True

        return solver_pb2.GetTaskStatusResponse(
            requestStatus=COMPLETED,
            requestStatusDetails="Found task status",
            grpcTaskStatus=grpc_task_status,
            solution=solution,
            taskMessage=task_message,
        )


def clean_delivered_job(
    task_id: str,
    process: mp.Process,
    poll_interval_seconds: float = 0.2,
    ttl_sec: float = 300.0,
):
    """
    Runs in a background thread to clean delivered jobs.
    """
    start = time.monotonic()

    while True:
        job = active.get(task_id, None)
        if job is None:
            return  # job already removed, exit daemon

        with job.lock:
            delivered = job.delivered
            status = job.grpcTaskStatus

        # If job result is delivered, and its status is terminal
        # statuses list, we can safely remove job item from the dictionary
        # and exit clean job daemon
        if delivered and status in TERMINAL and not process.is_alive():
            active.pop(task_id, None)
            return

        # skip infinite loops if client did not ask for result
        if time.monotonic() - start > ttl_sec:
            # delete jobs with terminal statuses
            if status in TERMINAL and not process.is_alive():
                active.pop(task_id, None)
            return

        time.sleep(poll_interval_seconds)


def watch_job_result(
    task_id: str, parent_conn, process: mp.Process, poll_interval_seconds: float = 0.2
):
    """
    Runs in a background thread.
    Reads worker result from Pipe and updates Job in `active`.
    """
    try:
        while True:
            job = active.get(task_id)
            if job is None:
                return  # job already removed, exit thread

            # We have data from process -> read and update Job,
            # and then exit thread
            with job.lock:
                last_status = job.grpcTaskStatus
                delivered = job.delivered

            if last_status == IN_PROGRESS and parent_conn.poll(poll_interval_seconds):
                status, solution, msg = parent_conn.recv()
                with job.lock:
                    job.grpcTaskStatus = status
                    job.solution = solution
                    job.taskMessage = msg
                break

            # If job result is delivered, and its status is terminal
            # statuses list, we can exit watch job thread
            if delivered and last_status in TERMINAL:
                break

            # Process is dead, but we have no results -> let's find out why
            # and exit watch job thread
            if not process.is_alive():
                # If task is in progress but process is dead, mark it as error
                if last_status == IN_PROGRESS:
                    with job.lock:
                        job.grpcTaskStatus = ERROR
                        job.taskMessage = f"Worker exited, exitcode={process.exitcode}"
                break
            time.sleep(poll_interval_seconds)
    except Exception as e:
        job = active.get(task_id)
        if job:
            with job.lock:
                # If task is in progress, but we raise an exception, mark it as error
                if job.grpcTaskStatus == IN_PROGRESS:
                    job.grpcTaskStatus = ERROR
                    job.taskMessage = f"Watcher error: {type(e).__name__}: {e}"
    finally:
        try:
            parent_conn.close()
        except Exception:
            pass
        try:
            process.join(timeout=0.1)
        except Exception:
            pass
