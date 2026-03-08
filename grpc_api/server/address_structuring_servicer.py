import asyncio
import logging
import logging.config
import multiprocessing
from multiprocessing import Process, Pipe, Queue
from queue import Full, ShutDown
from typing import AsyncGenerator, Any, AsyncIterable

import grpc
from grpc.aio import ServicerContext

from data_structuring.components.readers.protobuf_reader import ProtoReader, ProtoAddressSample
from data_structuring.config import RunServerConfig, DEFAULT_LOGGING_CONFIG
from data_structuring.pipeline import AddressStructuringPipeline
from grpc_api.generated import (pb2_CountryMatchResult,
                                pb2_TownMatchResult,
                                pb2_PairedMatchResult,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringServicer)

logger = logging.getLogger(__name__)
multiprocessing.set_start_method("spawn", force=True)


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
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        pipeline = AddressStructuringPipeline(batch_size=batch_size)
        try:
            while True:
                address_samples, output_channel = global_queue.get()
                reader = ProtoReader(address_samples)
                results = [result for result in pipeline.run(reader)]
                output_channel.send(results)
        except:
            logger.exception("Worker encountered an error whilst processing addresses")

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
                if not process.is_alive():
                    exit_code = process.exitcode
                    logger.warning(
                        "Worker process pid=%d died (exitcode=%s)",
                        process.pid, exit_code,
                    )
                    process.close()
                    if replace:
                        logger.info("Replacing worker pid=%d", process.pid)
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

    async def _process_samples(self, address_samples: list[ProtoAddressSample], context: ServicerContext) -> Any:
        """
        Queue the received address samples, wait for a worker process to run the pipeline,
        and return the results via a pipe connection.

        This function also handles gRPC errors if a worker process takes too long, the global queue is full,
        or if no worker process is available.
        """
        try:
            read_out_channel, write_out_channel = Pipe(duplex=False)
            self._global_queue.put_nowait((address_samples, write_out_channel))
            if not read_out_channel.poll(timeout=self._server_config.processing_timeout_seconds):
                logger.warning("Worker result timed out after %ds",
                               self._server_config.processing_timeout_seconds)
                await context.abort(
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    f"Worker did not return a result within "
                    f"{self._server_config.processing_timeout_seconds}s",
                )
            return read_out_channel.recv()
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
        except EOFError:
            logger.exception("Worker process closed pipe without sending results")
            asyncio.create_task(self._detect_dead_workers())
            await context.abort(
                grpc.StatusCode.DEADLINE_EXCEEDED,
                "Worker process died while processing the request. "
                "New workers have been spawned, please retry.",
            )

    async def ProcessAddress(self,
                             request_iterator: AsyncIterable[Any],
                             context: ServicerContext) -> AsyncGenerator[pb2_ProcessAddressResult, None]:
        """
        Bidirectional streaming RPC.
        Collects streamed AddressSample messages, runs the pipeline,
        and yields ProcessAddressResult messages back.
        """
        try:
            address_samples: list[ProtoAddressSample] = []

            async with asyncio.timeout(self._server_config.stream_timeout_seconds):
                async for sample_pb in request_iterator:
                    address_samples.append(ProtoAddressSample(
                        text=sample_pb.text,
                        hashId=sample_pb.hash_id,
                        suggestedCountry=(
                            sample_pb.suggested_country if sample_pb.HasField("suggested_country") else None),
                        forceSuggestedCountry=sample_pb.force_suggested_country,
                    ))

            results = await self._process_samples(address_samples, context)

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

        except TimeoutError:
            logger.warning("Client stream timed out after %ds", self._server_config.stream_timeout_seconds)
            await context.abort(
                grpc.StatusCode.DEADLINE_EXCEEDED,
                f"Client stream timed out after {self._server_config.stream_timeout_seconds}s",
            )
        except Exception as e:
            logger.exception("Error whilst processing addresses")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))

    async def handle_shutdown(self) -> None:
        """Handle shutdown by terminating all worker processes and closing the global queue."""
        self._shutting_down = True
        self._monitor_task.cancel()
        self._global_queue.close()
        for process in self._pipeline_processes:
            if process.is_alive():
                process.terminate()
                process.join()
