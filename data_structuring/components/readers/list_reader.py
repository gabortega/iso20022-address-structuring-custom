from typing import Generator

from data_structuring.components.readers.base_reader import BaseReader, AddressSample


class ListReader(BaseReader):
    def __init__(self, data: list[AddressSample]):
        self.data = data

    def read(self) -> Generator[AddressSample, None, None]:
        """
        Yield lines one by one from an in-memory list.
        Returns:
            Generator[str, None, None]: A generator yielding non-null values from the list given at initialization.
        """
        yield from self.data
