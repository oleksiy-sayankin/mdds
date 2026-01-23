# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import threading
import time
from constants import IN_PROGRESS, ERROR, TERMINAL
from dictionary import ThreadSafeDictionary
from job import Job


def finalize_job(job: Job):
    """Here we close job connection (used to get results from Pipe) and finalize job process with `join`"""
    try:
        job.connection.close()
    except Exception:
        pass
    try:
        job.process.join(timeout=0.1)
    except Exception:
        pass


def clean_delivered_job(
    active: ThreadSafeDictionary,
    poll_interval_seconds: float = 0.2,
    ttl_sec: float = 300.0,
):
    """
    Runs in a background thread to clean delivered jobs.
    """
    while True:
        for task_id in active.keys():
            job = active.get(task_id)

            if job:
                with job.lock:
                    delivered = job.delivered
                    status = job.taskStatus
                    process = job.process
                    start_time = job.startTime

                # If job result is delivered, and its status is terminal
                # statuses list, we can safely remove job item from the dictionary
                # and exit clean job daemon
                if delivered and status in TERMINAL and not process.is_alive():
                    active.pop(task_id, None)
                    finalize_job(job)

                # skip infinite loops if client did not ask for result
                if time.time() - start_time > ttl_sec:
                    # delete jobs with terminal statuses
                    if status in TERMINAL and not process.is_alive():
                        active.pop(task_id, None)
                        finalize_job(job)

        time.sleep(poll_interval_seconds)


def watch_job_result(active: ThreadSafeDictionary, poll_interval_seconds: float = 0.2):
    """
    Runs in a background thread.
    Reads worker result from Pipe and updates Job in `active`.
    """
    while True:
        for task_id in active.keys():
            job = active.get(task_id)

            if job:
                try:
                    # We have data from process -> read and update Job,
                    # and then exit thread
                    with job.lock:
                        last_status = job.taskStatus
                        connection = job.connection
                        process = job.process

                    if last_status == IN_PROGRESS and connection.poll(0):
                        status, solution, msg = connection.recv()
                        with job.lock:
                            job.taskStatus = status
                            job.solution = solution
                            job.taskMessage = msg
                            continue

                    # Process is dead, but we have no results -> let's find out why
                    # and exit watch job thread
                    if not process.is_alive():
                        # If task is in progress but process is dead, mark it as error
                        if last_status == IN_PROGRESS:
                            with job.lock:
                                job.taskStatus = ERROR
                                job.taskMessage = (
                                    f"Worker exited, exitcode={process.exitcode}"
                                )

                except Exception as e:
                    with job.lock:
                        # If task is in progress, but we raise an exception, mark it as error
                        if job.taskStatus == IN_PROGRESS:
                            job.taskStatus = ERROR
                            job.taskMessage = f"Watcher error: {type(e).__name__}: {e}"

        time.sleep(poll_interval_seconds)


class JobRegistry:
    """
    Job registry that starts threads to watch over dictionary of active jobs
    """

    _instance = None
    active = None
    _started = False
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(JobRegistry, cls).__new__(cls)

        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.active = ThreadSafeDictionary()
        self._started = False
        self._initialized = True

    def start(self):
        # Start watcher thread. In this watcher we periodically update `active` dictionary, getting
        # results (if any) from the solve process.
        if self._started:
            return
        _watcher = threading.Thread(
            target=watch_job_result,
            args=(self.active,),
            daemon=True,
        )

        _watcher.start()

        # This cleaner watches if a task result was delivered and if so, removes job from dictionary
        _cleaner = threading.Thread(
            target=clean_delivered_job, args=(self.active,), daemon=True
        )
        _cleaner.start()

        self._started = True
