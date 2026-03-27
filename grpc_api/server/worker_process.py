import logging
import os
import time
from multiprocessing import Process, Queue
from queue import Empty

from data_structuring.components.readers.protobuf_reader import ProtoReader
from data_structuring.config import RunServerConfig
from data_structuring.pipeline import AddressStructuringPipeline
from grpc_api.server.process_address_tasks import WorkerInput
from grpc_api.server.utils import unset_logging_config, set_logging_config

logger = logging.getLogger(__name__)


class WorkerProcess(Process):
    def __init__(self, input_queue: Queue, health_queue: Queue, server_config: RunServerConfig):
        super(WorkerProcess, self).__init__(
            target=self._pipeline_process_func,
            args=(input_queue, health_queue, server_config))

    @staticmethod
    def _send_health_event(health_queue: Queue) -> None:
        health_queue.put_nowait({"worker_pid": os.getpid(), "last_timestamp": time.time()})

    @staticmethod
    def _pipeline_process_func(input_queue: Queue, health_queue: Queue, server_config: RunServerConfig) -> None:
        """Create worker pipeline task that waits for address samples to process."""
        unset_logging_config()
        pipeline = AddressStructuringPipeline(batch_size=server_config.batch_size)
        # Send health event to signal worker is ready
        WorkerProcess._send_health_event(health_queue)
        try:
            while True:
                # Use context management to handle worker input
                worker_input: WorkerInput
                try:
                    with input_queue.get(block=True,
                                         timeout=server_config.worker_idle_health_interval) as worker_input:
                        set_logging_config(worker_input.rpc_id)
                        reader = ProtoReader(worker_input.address_samples)
                        results = [result for result in pipeline.run(reader)]
                        worker_input.send(results)
                        unset_logging_config()
                        WorkerProcess._send_health_event(health_queue)
                except Empty:
                    WorkerProcess._send_health_event(health_queue)
        except Exception as e:
            logger.exception("Worker encountered an error whilst processing addresses")
            if worker_input:
                worker_input.send(e)
            raise e
