"""
Simple application to demonstrate how to use the tool as a REST server.
"""
import logging
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field
from torch.backends.mkl import verbose

from data_structuring.components.readers.list_reader import ListReader
from data_structuring.config import RunServerConfig
from data_structuring.pipeline import AddressStructuringPipeline

logger = logging.getLogger(__name__)

pipeline: AddressStructuringPipeline | None = None


class StructureRequest(BaseModel):
    addresses: list[str] = Field(description="List of unstructured addresses to process")
    num_results: int = Field(default=2, description="Number of results to return")
    verbose: bool = Field(default=False, description="Enable verbose output")


class BaseMatchResult(BaseModel):
    matched: str
    confidence_score: float | None
    resolved_name: str
    start_index: int | None
    end_index: int | None
    flags: List[str] | None


class TownMatchResult(BaseMatchResult):
    inferred_country_resolved_code: str | None


class PairedMatchResult(BaseModel):
    country_match: BaseMatchResult
    town_match: TownMatchResult


class ProcessResult(BaseModel):
    address: str
    matches: list[PairedMatchResult]


class ProcessResponse(BaseModel):
    results: list[ProcessResult]


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


@app.post("/process_address", response_model=ProcessResponse, response_class=ORJSONResponse)
def infer_town_country_from_address(request: StructureRequest):
    """Process a list of unstructured addresses and return the inferred town and country for each address."""
    reader = ListReader(request.addresses)
    raw_results = pipeline.run(reader)

    results = []
    for result in raw_results:
        matches = []
        for i in range(min(request.num_results, len(raw_results))):
            country_corrected, country_conf, country_matched = result.i_th_best_match_country(i, value_if_none=None)
            town_corrected, town_conf, town_matched = result.i_th_best_match_town(i, value_if_none=None)
            if country_matched is None and town_matched is None:
                continue
            country_details = result.fuzzy_match_result.country_matches.model_dump(mode="json")[i] if verbose else None
            town_details = result.fuzzy_match_result.town_matches.model_dump(mode="json")[i] if verbose else None
            matches.append(PairedMatchResult(
                # Country results
                country_match=BaseMatchResult(
                    matched=country_matched,
                    confidence_score=country_conf,
                    resolved_name=country_corrected,
                    start_index=country_details["start"] if verbose else None,
                    end_index=country_details["end"] if verbose else None,
                    flags=country_details["flags"] if verbose else None),
                # Town results
                town_match=TownMatchResult(
                    matched=town_matched,
                    confidence_score=town_conf,
                    resolved_name=town_corrected,
                    inferred_country_resolved_code=result.fuzzy_match_result.town_matches[i].origin,
                    start_index=town_details["start"] if verbose else None,
                    end_index=town_details["end"] if verbose else None,
                    flags=town_details["flags"] if verbose else None)
            ))
        results.append(ProcessResult(
            address=result.crf_result.details.content,
            matches=matches,
        ))

    return ProcessResponse(results=results)
