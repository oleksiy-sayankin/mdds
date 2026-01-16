# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import threading
import logging
import multiprocessing as mp
from dictionary import ThreadSafeDictionary
from generated import solver_pb2, solver_pb2_grpc
from dataclasses import dataclass, field


@dataclass
class Job:
    """
    Job with multithreading process and its status.
    """

    process: mp.Process
    grpcTaskStatus: solver_pb2.GrpcTaskStatus
    solution: list[float]
    taskMessage: str
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


# Mapping solver name -> Python class
SOLVER_MAP = {
    "numpy_exact_solver": "NumpyExactSolver",
    "numpy_lstsq_solver": "NumpyLstsqSolver",
    "numpy_pinv_solver": "NumpyPinvSolver",
    "petsc_solver": "PetscSolver",
    "scipy_gmres_solver": "ScipyGmresSolver",
}

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
                solver_pb2.GrpcTaskStatus.DONE,
                solution,
                "Solved system of linear algebraic equations",
            )
        )
    except Exception as e:
        conn.send((solver_pb2.GrpcTaskStatus.ERROR, [], f"{type(e).__name__}: {e}"))
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
                requestStatus=solver_pb2.RequestStatus.DECLINED,
                requestStatusDetails="Task id is invalid: empty or null",
            )

        method = request.method
        if method not in SOLVER_MAP:
            return solver_pb2.SubmitTaskResponse(
                requestStatus=solver_pb2.RequestStatus.DECLINED,
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
                solver_pb2.GrpcTaskStatus.IN_PROGRESS,
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

        return solver_pb2.SubmitTaskResponse(
            requestStatus=solver_pb2.RequestStatus.COMPLETED,
            requestStatusDetails=f"Successfully submitted job for task: {task_id}",
        )

    def CancelTask(self, request, context):
        task_id = request.taskId
        if not task_id:
            return solver_pb2.CancelTaskResponse(
                requestStatus=solver_pb2.RequestStatus.DECLINED,
                requestStatusDetails="Task id is empty",
            )
        job = active.get(task_id, None)
        if job is None:
            return solver_pb2.CancelTaskResponse(
                requestStatus=solver_pb2.RequestStatus.DECLINED,
                requestStatusDetails=f"Task {task_id} is not found. Total active tasks count: {active.size()}",
            )
        with job.lock:
            if job.grpcTaskStatus != solver_pb2.GrpcTaskStatus.IN_PROGRESS:
                return solver_pb2.CancelTaskResponse(
                    requestStatus=solver_pb2.RequestStatus.DECLINED,
                    requestStatusDetails=f"Task {task_id} is not in IN_PROGRESS state. Task status is {job.grpcTaskStatus}",
                )
            # mark that process is terminated intentionally and not by error
            job.grpcTaskStatus = solver_pb2.GrpcTaskStatus.CANCELLED

        terminate_process(job.process)
        return solver_pb2.CancelTaskResponse(
            requestStatus=solver_pb2.RequestStatus.COMPLETED,
            requestStatusDetails=f"Task {task_id} is cancelled",
        )

    def GetTaskStatus(self, request, context):
        task_id = request.taskId
        if not task_id:
            return solver_pb2.GetTaskStatusResponse(
                requestStatus=solver_pb2.RequestStatus.DECLINED,
                requestStatusDetails="Task id is empty",
            )
        job = active.get(task_id, None)
        if job is None:
            return solver_pb2.GetTaskStatusResponse(
                requestStatus=solver_pb2.RequestStatus.DECLINED,
                requestStatusDetails=f"Task {task_id} is not found. Total active tasks count: {active.size()}",
            )
        return solver_pb2.GetTaskStatusResponse(
            requestStatus=solver_pb2.RequestStatus.COMPLETED,
            requestStatusDetails="Found task status",
            grpcTaskStatus=job.grpcTaskStatus,
            solution=job.solution,
            taskMessage=job.taskMessage,
        )


def watch_job_result(task_id: str, parent_conn, process: mp.Process):
    """
    Runs in a background thread.
    Reads worker result from Pipe and updates Job in `active`.
    """
    try:
        while True:
            # We have data from process -> read and update Job
            if parent_conn.poll(0.2):
                status, solution, msg = parent_conn.recv()

                job = active.get(task_id)
                if job:
                    with job.lock:
                        job.grpcTaskStatus = status
                        job.solution = solution
                        job.taskMessage = msg
                break

            # Process is dead, but we have no results -> let's find out why
            if not process.is_alive():
                job = active.get(task_id)
                if job:
                    with job.lock:
                        # If Cancel() method marked task as CANCELLED — leave it as CANCELLED
                        if job.grpcTaskStatus == solver_pb2.GrpcTaskStatus.CANCELLED:
                            job.taskMessage = "Cancelled"
                        else:
                            job.grpcTaskStatus = solver_pb2.GrpcTaskStatus.ERROR
                            job.taskMessage = (
                                f"Worker exited, exitcode={process.exitcode}"
                            )
                break
    except Exception as e:
        job = active.get(task_id)
        if job:
            with job.lock:
                if job.grpcTaskStatus == solver_pb2.GrpcTaskStatus.CANCELLED:
                    job.taskMessage = "Cancelled"
                else:
                    job.grpcTaskStatus = solver_pb2.GrpcTaskStatus.ERROR
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
