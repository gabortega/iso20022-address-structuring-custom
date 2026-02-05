from typing import Generator

from data_structuring.components.readers.base_reader import BaseReader


class ListReader(BaseReader):
    def __init__(self, data: list[str]):
        self.data = data

    def read(self) -> Generator[str, None, None]:
        """
        Yield lines one by one from an in-memory list.
        Returns:
            Generator[str, None, None]: A generator yielding non-null values from the list given at initialization.
        """
        yield from self.data
