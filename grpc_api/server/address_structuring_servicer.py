import asyncio
import logging
from typing import AsyncGenerator

import grpc

from data_structuring.components.readers.protobuf_reader import ProtoReader, ProtoAddressSample
from data_structuring.config import RunServerConfig
from data_structuring.pipeline import AddressStructuringPipeline
from grpc_api.generated import (pb2_CountryMatchResult,
                                pb2_TownMatchResult,
                                pb2_PairedMatchResult,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringServicer)

logger = logging.getLogger(__name__)


class AddressStructuringServicer(pb2_grpc_AddressStructuringServicer):
    """
    gRPC servicer that wraps the AddressStructuringPipeline and allows bidirectional streaming
    that accepts protobuf ProcessAddress requests and returns ProcessAddressResults.
    """

    def __init__(self, pipeline: AddressStructuringPipeline, server_config: RunServerConfig = RunServerConfig()):
        self._pipeline = pipeline
        self._server_config = server_config

    async def ProcessAddress(self, request_iterator, context) -> AsyncGenerator[pb2_ProcessAddressResult, None]:
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

            reader = ProtoReader(address_samples)
            results = await asyncio.to_thread(self._pipeline.run, reader)
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
            logger.exception("Error processing addresses")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))
