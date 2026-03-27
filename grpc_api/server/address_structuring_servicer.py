import asyncio
import logging.config
from multiprocessing import Queue
from queue import Full, ShutDown
from typing import AsyncGenerator, Any, AsyncIterable

import grpc
from grpc.aio import ServicerContext

from data_structuring.components.readers.protobuf_reader import ProtoAddressSample
from data_structuring.config import RunServerConfig
from grpc_api.generated import (pb2_CountryMatchResult,
                                pb2_TownMatchResult,
                                pb2_PairedMatchResult,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringServicer)
from grpc_api.server.process_address_tasks import ProcessAddressTask
from grpc_api.server.worker_monitor import WorkerMonitor

logger = logging.getLogger(__name__)


class AddressStructuringServicer(pb2_grpc_AddressStructuringServicer):
    """
    gRPC servicer that wraps the AddressStructuringPipeline and allows bidirectional streaming
    that accepts protobuf ProcessAddress requests and returns ProcessAddressResults.
    """

    def __init__(self, server_config: RunServerConfig = RunServerConfig()):
        self._server_config: RunServerConfig = server_config
        self._input_queue: Queue = Queue(self._server_config.max_queue_size)
        self.monitor: WorkerMonitor = WorkerMonitor(self._input_queue, self._server_config)

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
                task_input.send_worker_input(self._input_queue)
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
