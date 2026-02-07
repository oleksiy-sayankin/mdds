# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
import multiprocessing as mp
import time

from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime, timezone
from dictionary import ThreadSafeDictionary
from generated import solver_pb2, solver_pb2_grpc
from constants import (
    IN_PROGRESS,
    DONE,
    ERROR,
    CANCELLED,
    TERMINAL,
    DECLINED,
    COMPLETED,
    JOB_TIMEOUT,
)
from job import Job


# Mapping solver name -> Python class
SOLVER_MAP = {
    "numpy_exact_solver": "NumpyExactSolver",
    "numpy_lstsq_solver": "NumpyLstsqSolver",
    "numpy_pinv_solver": "NumpyPinvSolver",
    "petsc_solver": "PetscSolver",
    "scipy_gmres_solver": "ScipyGmresSolver",
}


logger = logging.getLogger(__name__)


def ts_from_unix(sec: float) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(datetime.fromtimestamp(sec, tz=timezone.utc))
    return t


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


ctx = mp.get_context("spawn")  # Create multi process context


def get_progress(task_status: int, start_time: float) -> int:
    if task_status == IN_PROGRESS:
        p = int((time.time() - start_time) / JOB_TIMEOUT * 100)
        return max(0, min(99, p))
    if task_status == DONE:
        return 100
    if task_status in (ERROR, CANCELLED):
        return 70
    return 0


class SolverService(solver_pb2_grpc.SolverServiceServicer):
    """
    gRPC Service that takes solve request (matrix of coefficients, right hand side and solve method),
    converts Protobuf data to Python data, solves system of linear algebraic equations and send the
    result as Protobuf data back to the caller.
    """

    def __init__(self, active: ThreadSafeDictionary):
        super().__init__()
        self.active = active

    def SubmitTask(self, request, context):
        task_id = request.taskId
        if not task_id:
            logger.warning("No task id provided")
            return solver_pb2.SubmitTaskResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Task id is invalid: empty or null",
            )

        if self.active.get(task_id):
            logger.warning(f"Task {task_id} is already submitted")
            return solver_pb2.SubmitTaskResponse(
                taskId=task_id,
                requestStatus=DECLINED,
                requestStatusDetails="Task already submitted",
            )

        method = request.method
        if method not in SOLVER_MAP:
            logger.warning(f"Method {method} not supported")
            return solver_pb2.SubmitTaskResponse(
                taskId=task_id,
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
        self.active.put(
            task_id,
            Job(
                process,
                IN_PROGRESS,
                [],
                "Task submitted and is in progress",
                time.time(),
                None,
                parent_conn,
            ),
        )

        logger.info(f"Task {task_id} submitted and is in progress")
        return solver_pb2.SubmitTaskResponse(
            taskId=task_id,
            requestStatus=COMPLETED,
            requestStatusDetails=f"Successfully submitted job for task: {task_id}",
        )

    def CancelTask(self, request, context):
        task_id = request.taskId
        if not task_id:
            logger.warning("No task id provided")
            return solver_pb2.CancelTaskResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Task id is empty",
            )
        job = self.active.get(task_id, None)
        if job is None:
            logger.warning(f"Task {task_id} does not exist")
            return solver_pb2.CancelTaskResponse(
                taskId=task_id,
                requestStatus=DECLINED,
                requestStatusDetails=f"Task {task_id} is not found. Total active tasks count: {self.active.size()}",
            )
        with job.lock:
            if job.taskStatus != IN_PROGRESS:
                logger.warning(
                    f"Task {task_id} is not in progress. Task status is {solver_pb2.TaskStatus.Name(job.taskStatus)}"
                )
                return solver_pb2.CancelTaskResponse(
                    taskId=task_id,
                    requestStatus=DECLINED,
                    requestStatusDetails=f"Task {task_id} is not in IN_PROGRESS state. Task status is {solver_pb2.TaskStatus.Name(job.taskStatus)}",
                )
            # mark that process is terminated intentionally and not by error
            job.taskStatus = CANCELLED
            job.taskMessage = "Cancelled by request"
            job.endTime = time.time()

        terminate_process(job.process)
        logger.info(f"Task {task_id} is cancelled")
        return solver_pb2.CancelTaskResponse(
            taskId=task_id,
            requestStatus=COMPLETED,
            requestStatusDetails=f"Task {task_id} is cancelled",
        )

    def GetTaskStatus(self, request, context):
        task_id = request.taskId
        if not task_id:
            logger.warning("No task id provided")
            return solver_pb2.GetTaskStatusResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Task id is empty or null",
            )
        job = self.active.get(task_id, None)
        if job is None:
            logger.warning(f"Task {task_id} does not exist")
            return solver_pb2.GetTaskStatusResponse(
                taskId=task_id,
                requestStatus=DECLINED,
                requestStatusDetails=f"Task {task_id} is not found. Total active tasks count: {self.active.size()}",
            )
        with job.lock:
            task_status = job.taskStatus
            solution = job.solution
            task_message = job.taskMessage
            start_time = job.startTime
            end_time = job.endTime or time.time()
            if task_status in TERMINAL:
                job.delivered = True

        progress = get_progress(task_status, start_time)

        logger.debug(
            f"Found task {task_id} with status: {solver_pb2.TaskStatus.Name(task_status)}"
        )
        return solver_pb2.GetTaskStatusResponse(
            requestStatus=COMPLETED,
            requestStatusDetails="Found task status",
            taskId=task_id,
            startTime=ts_from_unix(start_time),
            endTime=ts_from_unix(end_time),
            progress=progress,
            taskStatus=task_status,
            solution=solution,
            taskMessage=task_message,
        )
