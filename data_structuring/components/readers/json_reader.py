from typing import Generator, Any

from dataclass_mapper import mapper, map_to
from pydantic import Field
from pydantic.dataclasses import dataclass

from data_structuring.components.readers.base_reader import BaseReader, AddressSample


@mapper(AddressSample)
@dataclass(frozen=True)
class JsonAddressSample:
    """Input JSON-like data structure for each address."""
    text: str = Field(description="Address string",
                      alias="text")
    hash_id: str = Field(description="Unique hash identifier of the address",
                         alias="hashId")
    # Non-default attributes
    suggested_country: str | None = Field(default=None,
                                          description="Suggested country code",
                                          alias="suggestedCountry")
    force_suggested_country: bool = Field(default=False,
                                          description="Toggle force suggested country",
                                          alias="forceSuggestedCountry")


class JsonReader(BaseReader):
    def __init__(self, data: list[JsonAddressSample]):
        self.data = data

    def read(self) -> Generator[AddressSample, Any, None]:
        """
        Yield lines one by one from an in-memory list.
        Returns:
            Generator[str, None, None]: A generator yielding non-null values from the list given at initialization.
        """
        yield from (map_to(address_sample, AddressSample) for address_sample in self.data)
