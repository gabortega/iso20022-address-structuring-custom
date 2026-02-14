from typing import List

from pydantic import Field
from pydantic.dataclasses import dataclass

from data_structuring.components.readers.json_reader import JsonAddressSample


@dataclass(frozen=True, slots=True)
class ProcessAddressRequest:
    """
    First-level of the input data structure for processing addresses.
    """
    address_samples: list[JsonAddressSample] = Field(description="List of unstructured addresses to process",
                                                     alias="addressSamples")
    num_results: int = Field(default=2,
                             description="Number of results to return",
                             alias="numResults")


@dataclass(frozen=True, slots=True)
class CountryMatchResult:
    """
    Output data structure for a country match result.
    """
    matched: str = Field(description="Matched word(s)",
                         alias="matched")
    confidence_score: float = Field(description="Confidence score for match",
                                    alias="confidenceScore")
    resolved_name: str = Field(description="Resolved name (country code)",
                               alias="resolvedName")
    # Non-default attributes
    start_index: int | None = Field(default=None,
                                    description="Start index",
                                    alias="startIndex")
    end_index: int | None = Field(default=None,
                                  description="End index",
                                  alias="endIndex")
    flags: List[str] | None = Field(default=None,
                                    description="Flags for match",
                                    alias="flags")


@dataclass(frozen=True, slots=True)
class TownMatchResult:
    """
    Output data structure for a town match result.
    """
    matched: str = Field(description="Matched word(s)",
                         alias="matched")
    confidence_score: float = Field(description="Confidence score for match",
                                    alias="confidenceScore")
    resolved_name: str = Field(description="Resolved town name",
                               alias="resolvedName")
    # Non-default attributes
    inferred_country_code: str | None = Field(default=None,
                                              description="Country code inferred from town",
                                              alias="inferredCountryCode")
    start_index: int | None = Field(default=None,
                                    description="Start index",
                                    alias="startIndex")
    end_index: int | None = Field(default=None,
                                  description="End index",
                                  alias="endIndex")
    flags: List[str] | None = Field(default=None,
                                    description="Flags for match",
                                    alias="flags")


@dataclass(frozen=True, slots=True)
class PairedMatchResult:
    """
    Output data structure for a paired match result.
    """
    country_match: CountryMatchResult = Field(description="Country match",
                                              alias="countryMatch")
    town_match: TownMatchResult = Field(description="Town match",
                                        alias="townMatch")


@dataclass(frozen=True, slots=True)
class ProcessAddressResult:
    """
    Output data structure for a a single process address result.
    """
    hash_id: str = Field(description="Unique hash identifier of the address",
                         alias="hashId")
    matches: list[PairedMatchResult] = Field(description="Matches for the processed address",
                                             alias="matches")


@dataclass(frozen=True, slots=True)
class ProcessAddressResponse:
    """
    First-level data structure of a process address response.
    """
    results: list[ProcessAddressResult] = Field(description="Processing results",
                                                alias="results")
