import asyncio
import logging.config
import multiprocessing
from dataclasses import dataclass
from multiprocessing import Process, Queue, Pipe
from multiprocessing.connection import Connection
from queue import Full, ShutDown
from typing import AsyncGenerator, Any, AsyncIterable

import grpc
from grpc.aio import ServicerContext

from data_structuring.components.readers.protobuf_reader import ProtoReader, ProtoAddressSample
from data_structuring.config import RunServerConfig
from data_structuring.pipeline import AddressStructuringPipeline
from grpc_api.generated import (pb2_CountryMatchResult,
                                pb2_TownMatchResult,
                                pb2_PairedMatchResult,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringServicer)
from grpc_api.server.utils import set_logging_config, unset_logging_config

logger = logging.getLogger(__name__)
multiprocessing.set_start_method("spawn", force=True)


@dataclass(frozen=False, slots=True)
class ProcessAddressTask:
    """
    A data structure to hold the input data for tracking a ProcessAddress task and returning the results.

    Attributes:
        rpc_id (str): The RPC ID string.
        address_samples (list[ProtoAddressSample]): The list of address samples to process.
        tracked_event: (asyncio.Event) The event that keeps track whether a worker has written an output.
        event_loop: (asyncio.AbstractEventLoop) The event loop to use.
        read_channel (Connection): The pipe connection channel from which to read the output.
        write_channel (Connection): The pipe connection channel to write the output onto.
    """
    rpc_id: str
    address_samples: list[ProtoAddressSample]
    tracked_event: asyncio.Event
    event_loop: asyncio.AbstractEventLoop
    read_channel: Connection
    write_channel: Connection

    def __init__(self, rpc_id: str, address_samples: list[ProtoAddressSample]) -> None:
        self.rpc_id = rpc_id
        self.address_samples = address_samples

    def __enter__(self):
        self.tracked_event = asyncio.Event()
        self.event_loop = asyncio.get_event_loop()
        self.read_channel, self.write_channel = Pipe(duplex=False)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Cleanup any open channels and pending events
        self.tracked_event.set()
        self.event_loop.remove_reader(self.read_channel)
        self.read_channel.close()
        self.write_channel.close()

    def create_and_send_worker_input(self, queue: Queue) -> None:
        logger.info("Creating and sending worker input for RPC-%s", self.rpc_id)
        queue.put_nowait(
            WorkerInput(rpc_id=self.rpc_id,
                        address_samples=self.address_samples,
                        write_channel=self.write_channel)
        )

    async def wait_for_results(self, timeout: float) -> Any:
        async with asyncio.timeout(timeout):
            self.event_loop.add_reader(self.read_channel, self.tracked_event.set)
            await self.tracked_event.wait()

        logger.info("Received results from worker for RPC-%s", self.rpc_id)
        return self.read_channel.recv()


@dataclass(frozen=True, slots=True)
class WorkerInput:
    """
    A data structure to hold the input data for a worker process.

    Attributes:
        rpc_id (str): The RPC ID string.
        address_samples (list[ProtoAddressSample]): The list of address samples to process.
        write_channel (Connection): The pipe connection channel to write the output onto.
    """
    rpc_id: str
    address_samples: list[ProtoAddressSample]
    write_channel: Connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.write_channel.close()

    def send(self, obj) -> None:
        try:
            self.write_channel.send(obj)
        except BrokenPipeError:
            logger.exception("Worker encountered an exception while sending data")


class AddressStructuringServicer(pb2_grpc_AddressStructuringServicer):
    """
    gRPC servicer that wraps the AddressStructuringPipeline and allows bidirectional streaming
    that accepts protobuf ProcessAddress requests and returns ProcessAddressResults.
    """

    def __init__(self, server_config: RunServerConfig = RunServerConfig()):
        self._server_config = server_config
        self._pipeline_processes: list[Process] = []
        self._global_queue = Queue(self._server_config.max_queue_size)
        self._shutting_down = False
        self._replace_lock = asyncio.Lock()
        # Create the pipeline processes and pass global queue as arg
        logger.info("Initializing worker processes")
        _ = [self._pipeline_processes.append(self._start_worker())
             for _ in range(self._server_config.pipeline_max_instances)]
        logger.info("All worker processes ready")
        # Start background health monitor
        logger.info("Starting worker monitor")
        self._monitor_task = asyncio.ensure_future(self._monitor_workers())

    @staticmethod
    def _pipeline_process_func(global_queue: Queue, batch_size: int) -> None:
        """Create worker pipeline task that waits for address samples to process."""
        unset_logging_config()
        pipeline = AddressStructuringPipeline(batch_size=batch_size)
        try:
            while True:
                # Use context management to handle worker input
                with global_queue.get() as worker_input:
                    set_logging_config(worker_input.rpc_id)
                    reader = ProtoReader(worker_input.address_samples)
                    results = [result for result in pipeline.run(reader)]
                    worker_input.send(results)
                    unset_logging_config()
        except Exception as e:
            logger.exception("Worker encountered an error whilst processing addresses")
            if worker_input:
                worker_input.send_and_close_channel(e)
            raise e

    async def _process_samples(self, address_samples: list[ProtoAddressSample], context: ServicerContext) -> Any:
        """
        Queue the received address samples, wait for a worker process to run the pipeline,
        and return the results via a pipe connection.

        This function also handles gRPC errors if a worker process takes too long, the global queue is full,
        or if no worker process is available.
        """
        metadata = dict(context.invocation_metadata())
        rpc_id = metadata["client-rpc-id"] if "client-rpc-id" in metadata else "<NO ID>"

        try:
            with ProcessAddressTask(rpc_id=rpc_id, address_samples=address_samples) as task_input:
                task_input.create_and_send_worker_input(self._global_queue)
                results = await task_input.wait_for_results(self._server_config.processing_timeout_seconds)

                # Did the worker crash?
                if isinstance(results, Exception):
                    logger.warning("Worker process closed pipe without sending results")
                    await context.abort(
                        grpc.StatusCode.INTERNAL,
                        "Worker process died while processing the request. "
                        "Please retry later.",
                    )
                return results

        except TimeoutError:
            logger.warning("Worker result timed out after %ds",
                           self._server_config.processing_timeout_seconds)
            await context.abort(
                grpc.StatusCode.DEADLINE_EXCEEDED,
                f"Worker did not return a result within "
                f"{self._server_config.processing_timeout_seconds}s",
            )
        except OSError:
            logger.exception("Error occurred whilst operating pipe for worker communication")
            await context.abort(
                grpc.StatusCode.INTERNAL,
                "Server encountered an error whilst communicating with worker process. "
                "Please retry later.",
            )
        except Full:
            logger.warning("Global queue is full (max_size=%d)", self._server_config.max_queue_size)
            await context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                f"Server is at capacity (queue full, max_size={self._server_config.max_queue_size}). "
                "Please retry later.",
            )
        except ShutDown:
            logger.warning("Global queue has already been closed")
            await context.abort(
                grpc.StatusCode.INTERNAL,
                "Server queue is unavailable (is the server shutting down?). "
                "Please retry later.",
            )

    async def ProcessAddress(self,
                             request_iterator: AsyncIterable[Any],
                             context: ServicerContext) -> AsyncGenerator[pb2_ProcessAddressResult, None]:
        """
        Bidirectional streaming RPC.
        Collects streamed AddressSample messages, runs the pipeline,
        and yields ProcessAddressResult messages back.
        """
        address_samples: list[ProtoAddressSample] = []
        try:
            async with asyncio.timeout(self._server_config.stream_timeout_seconds):
                async for sample_pb in request_iterator:
                    address_samples.append(ProtoAddressSample(
                        text=sample_pb.text,
                        hashId=sample_pb.hash_id,
                        suggestedCountry=(
                            sample_pb.suggested_country if sample_pb.HasField("suggested_country") else None),
                        forceSuggestedCountry=sample_pb.force_suggested_country,
                    ))
        except TimeoutError:
            logger.warning("Client stream timed out after %ds", self._server_config.stream_timeout_seconds)
            await context.abort(
                grpc.StatusCode.DEADLINE_EXCEEDED,
                f"Client stream timed out after {self._server_config.stream_timeout_seconds}s",
            )

        results = await asyncio.ensure_future(self._process_samples(address_samples, context))

        for result in results:
            matches = []
            for i in range(self._server_config.num_results):
                country_corrected, country_conf, country_matched = (
                    result.i_th_best_match_country(i, value_if_none=None))
                town_corrected, town_conf, town_matched = result.i_th_best_match_town(i, value_if_none=None)
                if country_matched is None and town_matched is None:
                    continue
                country_details = result.fuzzy_match_result.country_matches.model_dump(mode="json")[i]
                town_details = result.fuzzy_match_result.town_matches.model_dump(mode="json")[i]
                matches.append(pb2_PairedMatchResult(
                    country_match=pb2_CountryMatchResult(
                        matched=country_matched,
                        confidence_score=country_conf,
                        resolved_name=country_corrected,
                        start_index=country_details["start"],
                        end_index=country_details["end"],
                        flags=country_details["flags"] or []),
                    town_match=pb2_TownMatchResult(
                        inferred_country_code=result.fuzzy_match_result.town_matches[i].origin or "",
                        matched=town_matched,
                        confidence_score=town_conf,
                        resolved_name=town_corrected,
                        start_index=town_details["start"],
                        end_index=town_details["end"],
                        flags=town_details["flags"] or []),
                ))
            yield pb2_ProcessAddressResult(
                hash_id=result.hash_id,
                matches=matches,
            )

    def _start_worker(self) -> Process:
        """Start a new pipeline worker process."""
        process = Process(
            target=self._pipeline_process_func,
            args=(self._global_queue, self._server_config.batch_size),
        )
        process.start()
        logger.info("Started worker process pid=%d", process.pid)
        return process

    async def _detect_dead_workers(self, replace: bool = False) -> tuple[int, int]:
        """
        Check all workers to see if any have died.
        If 'replace' is set to True, then replace these dead workers with new ones.

        Returns the number of dead workers detected/replaced.
        """
        async with self._replace_lock:
            detected = 0
            for i, process in enumerate(self._pipeline_processes):
                try:
                    if not process.is_alive():
                        exit_code = process.exitcode
                        process_id = process.pid
                        logger.warning(
                            "Worker process pid=%d died (exitcode=%s)",
                            process_id, exit_code,
                        )
                        process.close()
                        if replace:
                            logger.info("Replacing worker pid=%d", process_id)
                            self._pipeline_processes[i] = self._start_worker()
                        detected += 1
                # Triggered if process was forcefully closed or an error occurred during process info retrieval
                except ValueError:
                    logger.exception("Detected worker process that was forcefully closed")
                    process.close()
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
        # Wait for the initial process to start
        await asyncio.sleep(self._server_config.monitor_startup_time_seconds)
        # Perform initial check to ensure that startup was successful
        detected_dead_workers, total_workers = await self._detect_dead_workers(False)
        # Kill server if initial processes are not alive after startup delay
        if detected_dead_workers or total_workers != self._server_config.pipeline_max_instances:
            logger.critical(
                "Detected %d initial worker processes that were unable to start out of %d total workers",
                detected_dead_workers, total_workers
            )
            exit(1)
        logger.info("Worker monitor ready")
        while not self._shutting_down:
            try:
                await asyncio.sleep(self._server_config.monitor_check_interval_seconds)
                if self._shutting_down:
                    break
                replaced, _ = await self._detect_dead_workers(replace=True)
                if replaced:
                    logger.info("Replaced %d dead worker(s)", replaced)
            except asyncio.CancelledError:
                break
            except:
                logger.exception("Worker monitor encountered an error, will retry next cycle")

    async def handle_shutdown(self) -> None:
        """Handle shutdown by terminating all worker processes and closing the global queue."""
        self._shutting_down = True
        self._monitor_task.cancel()
        self._global_queue.close()
        for process in self._pipeline_processes:
            if process.is_alive():
                process.terminate()
                process.join()
