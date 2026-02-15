import asyncio
import logging
from typing import AsyncGenerator

from data_structuring.components.readers.protobuf_reader import ProtoReader, ProtoAddressSample
from data_structuring.pipeline import AddressStructuringPipeline
from grpc_api.proto import address_structuring_pb2 as pb2, address_structuring_pb2_grpc as pb2_grpc

logger = logging.getLogger(__name__)

NUM_RESULTS = 2


class AddressStructuringServicer(pb2_grpc.AddressStructuringServicer):
    """
    gRPC servicer that wraps the AddressStructuringPipeline and allows bidirectional streaming
    that accepts protobuf ProcessAddress requests and returns AddressResults.
    """

    def __init__(self, pipeline: AddressStructuringPipeline):
        self._pipeline = pipeline

    async def ProcessAddress(self, request_iterator, context) -> AsyncGenerator[pb2.ProcessAddressResult, None]:
        """
        Bidirectional streaming RPC.
        Collects streamed AddressSample messages into batches, runs the pipeline,
        and yields AddressResult messages back as each batch completes.
        """
        address_samples: list[ProtoAddressSample] = []

        async for sample_pb in request_iterator:
            address_samples.append(ProtoAddressSample(
                text=sample_pb.text,
                hashId=sample_pb.hash_id,
                suggestedCountry=sample_pb.suggested_country if sample_pb.HasField("suggested_country") else None,
                forceSuggestedCountry=sample_pb.force_suggested_country,
            ))

        reader = ProtoReader(address_samples)
        results = await asyncio.to_thread(self._pipeline.run, reader)
        if results is not None:
            for result in results:
                matches = []
                for i in range(NUM_RESULTS):
                    country_corrected, country_conf, country_matched = (
                        result.i_th_best_match_country(i, value_if_none=None))
                    town_corrected, town_conf, town_matched = result.i_th_best_match_town(i, value_if_none=None)
                    if country_matched is None and town_matched is None:
                        continue
                    country_details = result.fuzzy_match_result.country_matches.model_dump(mode="json")[i]
                    town_details = result.fuzzy_match_result.town_matches.model_dump(mode="json")[i]
                    matches.append(pb2.PairedMatchResult(
                        country_match=pb2.CountryMatchResult(
                            matched=country_matched,
                            confidence_score=country_conf,
                            resolved_name=country_corrected,
                            start_index=country_details["start"],
                            end_index=country_details["end"],
                            flags=country_details["flags"] or []),
                        town_match=pb2.TownMatchResult(
                            inferred_country_code=result.fuzzy_match_result.town_matches[i].origin or "",
                            matched=town_matched,
                            confidence_score=town_conf,
                            resolved_name=town_corrected,
                            start_index=town_details["start"],
                            end_index=town_details["end"],
                            flags=town_details["flags"] or []),
                    ))
                yield pb2.ProcessAddressResult(
                    hash_id=result.hash_id,
                    matches=matches,
                )
