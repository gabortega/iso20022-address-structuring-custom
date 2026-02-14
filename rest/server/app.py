"""
Simple application to demonstrate how to use the tool as a REST server.
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from data_structuring.components.readers.json_reader import JsonReader
from data_structuring.config import RunServerConfig
from data_structuring.pipeline import AddressStructuringPipeline
from rest.server.api.process_address_api import ProcessAddressResponse, ProcessAddressRequest, PairedMatchResult, \
    CountryMatchResult, TownMatchResult, ProcessAddressResult

logger = logging.getLogger(__name__)

pipeline: AddressStructuringPipeline | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pipeline
    logger.info("Initializing AddressStructuringPipeline")
    server_args = RunServerConfig()  # retrieve application args
    pipeline = AddressStructuringPipeline(batch_size=server_args.batch_size)
    logger.info("Pipeline ready")
    yield
    pipeline = None


app = FastAPI(
    title="ISO 20022 Address Structuring REST Server",
    lifespan=lifespan,
)


@app.post("/process-address", response_model=ProcessAddressResponse, response_class=ORJSONResponse)
async def infer_town_country_from_address(request: ProcessAddressRequest) -> ProcessAddressResponse:
    """
    Process a list of unstructured addresses and return the inferred town and country for each address.
    """
    reader = JsonReader(request.address_samples)
    raw_results = pipeline.run(reader)

    results = []
    for result in raw_results:
        matches = []
        for i in range(request.num_results):
            country_corrected, country_conf, country_matched = result.i_th_best_match_country(i, value_if_none=None)
            town_corrected, town_conf, town_matched = result.i_th_best_match_town(i, value_if_none=None)
            if country_matched is None and town_matched is None:
                continue
            country_details = result.fuzzy_match_result.country_matches.model_dump(mode="json")[i]
            town_details = result.fuzzy_match_result.town_matches.model_dump(mode="json")[i]
            matches.append(PairedMatchResult(
                # Country results
                countryMatch=CountryMatchResult(
                    matched=country_matched,
                    confidenceScore=country_conf,
                    resolvedName=country_corrected,
                    startIndex=country_details["start"],
                    endIndex=country_details["end"],
                    flags=country_details["flags"]),
                # Town results
                townMatch=TownMatchResult(
                    inferredCountryCode=result.fuzzy_match_result.town_matches[i].origin,
                    matched=town_matched,
                    confidenceScore=town_conf,
                    resolvedName=town_corrected,
                    startIndex=town_details["start"],
                    endIndex=town_details["end"],
                    flags=town_details["flags"])
            ))
        results.append(ProcessAddressResult(
            hashId=result.hash_id,
            matches=matches,
        ))

    return ProcessAddressResponse(results=results)
