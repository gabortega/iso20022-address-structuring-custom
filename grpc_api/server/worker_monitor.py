import asyncio
import logging.config
import multiprocessing
import time
from asyncio import Future
from multiprocessing import Process, Queue
from queue import Empty

from data_structuring.config import RunServerConfig
from grpc_api.generated import (pb2_CountryMatchResult,
                                pb2_TownMatchResult,
                                pb2_PairedMatchResult,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringServicer)
from grpc_api.server.worker_process import WorkerProcess

logger = logging.getLogger(__name__)
multiprocessing.set_start_method("spawn", force=True)


class WorkerMonitor(pb2_grpc_AddressStructuringServicer):
    """
    Monitor for the servicer workers that manages the worker processes and handles health events,
    worker failures, and performs cleanup after shutdown signal is received.
    """

    def __init__(self, global_input_queue: Queue, server_config: RunServerConfig = RunServerConfig(), ):
        self._server_config: RunServerConfig = server_config
        self._pipeline_processes: list[Process] = []
        self._global_input_queue: Queue = global_input_queue
        self._global_health_queue: Queue = Queue()
        self._last_worker_health_event: dict[int, float] = {}
        self._starting: bool = True
        self._shutting_down: bool = False
        self._replace_lock: asyncio.Lock = asyncio.Lock()

        # Start background health monitor
        logger.info("Starting worker monitor task")
        self._monitor_task: Future = asyncio.ensure_future(self._monitor_workers())

    def _start_worker(self, create_health_event: bool = True) -> Process:
        """Start a new pipeline worker process."""
        worker_process = WorkerProcess(
            self._global_input_queue, self._global_health_queue, self._server_config)
        worker_process.start()
        if create_health_event:
            self._last_worker_health_event[worker_process.pid] = time.time()
        logger.info("Started worker process pid=%d", worker_process.pid)
        return worker_process

    def _kill_worker(self, process: Process) -> None:
        """
        Terminate a worker and block until it exits, then release its resources.
        """
        pid = process.pid
        process.terminate()
        process.join(timeout=self._server_config.worker_sigterm_timeout_seconds)
        if process.is_alive():
            logger.warning("Worker pid=%d did not exit after SIGTERM, sending SIGKILL", pid)
            process.kill()
            process.join(timeout=self._server_config.worker_sigkill_timeout_seconds)
        self._last_worker_health_event.pop(pid, None)
        process.close()

    def _drain_health_queue(self) -> None:
        """Drain all pending events from the health queue and update last known timestamps."""
        try:
            while True:
                event = self._global_health_queue.get_nowait()
                self._last_worker_health_event[event["worker_pid"]] = event["last_timestamp"]
        except Empty:
            pass

    async def _detect_dead_workers(self, replace: bool = False) -> tuple[int, int]:
        """
        Check all workers to see if any have died or stalled.
        If 'replace' is set to True, then replace these workers with new ones.

        A worker is considered stalled if it is alive but has not sent a health event
        within worker_stall_timeout_seconds (and has processed at least one task).

        Returns a tuple of (detected/replaced count, total worker count).
        """
        now = time.time()
        stall_timeout = self._server_config.worker_stall_timeout_seconds
        async with self._replace_lock:
            detected = 0
            for i, process in enumerate(self._pipeline_processes):
                try:
                    dead = not process.is_alive()
                    stalled = (
                            not dead
                            and stall_timeout > 0
                            and process.pid in self._last_worker_health_event
                            and (now - self._last_worker_health_event[process.pid]) > stall_timeout
                    )
                    if dead or stalled:
                        reason = "stalled" if stalled else "died"
                        logger.warning(
                            "Worker pid=%d %s (exitcode=%s, last_seen=%.1fs ago)",
                            process.pid, reason, process.exitcode,
                            now - self._last_worker_health_event.get(process.pid, now),
                        )
                        self._kill_worker(process)
                        if replace:
                            logger.info("Replacing %s worker pid=%d", reason, process.pid)
                            self._pipeline_processes[i] = self._start_worker()
                        detected += 1

                # Triggered if process was forcefully closed or an error occurred during process info retrieval
                except ValueError:
                    logger.exception("Detected worker process that was forcefully closed")
                    try:
                        self._kill_worker(process)
                    except Exception:
                        pass
                    if replace:
                        logger.info("Replacing killed worker")
                        self._pipeline_processes[i] = self._start_worker()
                    detected += 1

            if replace:
                # Replace any missing processes
                for _ in range(i + 1, self._server_config.pipeline_max_instances):
                    self._pipeline_processes.append(self._start_worker())
                    detected += 1

            return detected, len(self._pipeline_processes)

    async def _monitor_workers(self) -> None:
        """Periodically check worker health and replace dead workers."""
        try:
            # Initial startup: set timeout to ensure all workers started properly
            async with asyncio.timeout(self._server_config.monitor_full_startup_time_seconds):

                # Create the pipeline processes and pass global queue as arg
                logger.info("Initializing worker processes")
                _ = [self._pipeline_processes.append(self._start_worker(create_health_event=False))
                     for _ in range(self._server_config.pipeline_max_instances)]

                # Wait for the initial process to start
                await asyncio.sleep(self._server_config.monitor_initial_startup_time_seconds)

                # Perform initial check to ensure that process startup did not fail
                detected_dead_workers, total_workers = await self._detect_dead_workers(False)

                # Kill server if initial processes are not alive after first startup delay (e.g.: configuration issue)
                if detected_dead_workers or total_workers != self._server_config.pipeline_max_instances:
                    logger.critical(
                        "Detected %d initial worker processes that were unable to start out of %d total workers",
                        detected_dead_workers, total_workers
                    )
                    exit(1)

                # Wait for initial workers to be ready
                while len(self._last_worker_health_event) < self._server_config.pipeline_max_instances:
                    self._drain_health_queue()

                # All workers are ready, start SERVING
                self._starting = False
                logger.info("Worker monitor and all workers are ready")

        except TimeoutError:
            logger.critical("Initial worker processes were unable to become ready after %ds",
                            self._server_config.monitor_full_startup_time_seconds)
            exit(1)

        # Main loop while server is running
        while not self._shutting_down:
            try:
                await asyncio.sleep(self._server_config.monitor_check_interval_seconds)
                if self._shutting_down:
                    break
                self._drain_health_queue()
                replaced, _ = await self._detect_dead_workers(replace=True)
                if replaced:
                    logger.info("Replaced %d dead worker(s)", replaced)
            except asyncio.CancelledError:
                break
            except:
                logger.exception("Worker monitor encountered an error, will retry next cycle")

    async def handle_health_check(self) -> bool:
        """Handle the health check call for this servicer; returns True if the servicer is healthy, False otherwise."""
        if self._starting or self._shutting_down:
            return False

        dead_count, total = await self._detect_dead_workers(replace=False)
        alive_count = total - dead_count

        return alive_count > 0

    async def handle_shutdown(self) -> None:
        """Handle shutdown by terminating all worker processes and closing the global queue."""
        self._shutting_down = True
        self._monitor_task.cancel()
        self._global_input_queue.close()
        self._global_health_queue.close()
        for process in self._pipeline_processes:
            if process.is_alive():
                process.terminate()
                process.join()
