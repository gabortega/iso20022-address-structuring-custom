import asyncio
import logging
from dataclasses import dataclass
from multiprocessing import Queue, Pipe
from multiprocessing.connection import Connection
from typing import Any

from data_structuring.components.readers.protobuf_reader import ProtoAddressSample

logger = logging.getLogger(__name__)


@dataclass(frozen=False, slots=True)
class ProcessAddressTask:
    """
    A data structure to hold the input data for tracking a ProcessAddress task and returning the results.

    Attributes:
        rpc_id (str): The RPC ID string.
        address_samples (list[ProtoAddressSample]): The list of address samples to process.
        tracked_event: (asyncio.Event) The event that keeps track whether a worker has written an output.
        event_loop: (asyncio.AbstractEventLoop) The event loop to use.
        read_channel (Connection): The pipe connection channel from which to read the output.
        write_channel (Connection): The pipe connection channel to write the output onto.
    """
    rpc_id: str
    address_samples: list[ProtoAddressSample]
    tracked_event: asyncio.Event
    event_loop: asyncio.AbstractEventLoop
    read_channel: Connection
    write_channel: Connection

    def __init__(self, rpc_id: str, address_samples: list[ProtoAddressSample]) -> None:
        self.rpc_id = rpc_id
        self.address_samples = address_samples

    def __enter__(self):
        self.tracked_event = asyncio.Event()
        self.event_loop = asyncio.get_event_loop()
        self.read_channel, self.write_channel = Pipe(duplex=False)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Cleanup any open channels and pending events
        self.tracked_event.set()
        self.event_loop.remove_reader(self.read_channel)
        self.read_channel.close()
        self.write_channel.close()

    def send_worker_input(self, queue: Queue) -> None:
        logger.info("Sending worker input for RPC-%s", self.rpc_id)
        queue.put_nowait(
            WorkerInput(rpc_id=self.rpc_id,
                        address_samples=self.address_samples,
                        write_channel=self.write_channel)
        )

    async def wait_for_results(self, timeout: float) -> Any:
        async with asyncio.timeout(timeout):
            self.event_loop.add_reader(self.read_channel, self.tracked_event.set)
            await self.tracked_event.wait()

        logger.info("Received results from worker for RPC-%s", self.rpc_id)
        return self.read_channel.recv()


@dataclass(frozen=True, slots=True)
class WorkerInput:
    """
    A data structure to hold the input data for a worker process.

    Attributes:
        rpc_id (str): The RPC ID string.
        address_samples (list[ProtoAddressSample]): The list of address samples to process.
        write_channel (Connection): The pipe connection channel to write the output onto.
    """
    rpc_id: str
    address_samples: list[ProtoAddressSample]
    write_channel: Connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.write_channel.close()

    def send(self, obj) -> None:
        try:
            self.write_channel.send(obj)
        except BrokenPipeError:
            logger.exception("Worker encountered an exception while sending data")
