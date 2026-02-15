"""
Entry point for the address structuring gRPC server.
"""
import asyncio
import logging

import grpc

from data_structuring.config import RunServerConfig
from data_structuring.pipeline import AddressStructuringPipeline
from grpc_api.proto import address_structuring_pb2_grpc as pb2_grpc
from grpc_api.server.address_structuring_servicer import AddressStructuringServicer

logger = logging.getLogger(__name__)


async def _serve():
    server_args = RunServerConfig()

    logger.info("Initializing AddressStructuringPipeline")
    pipeline = AddressStructuringPipeline(batch_size=server_args.batch_size)
    logger.info("Pipeline ready")

    server = grpc.aio.server()
    pb2_grpc.add_AddressStructuringServicer_to_server(
        AddressStructuringServicer(pipeline), server
    )
    bind_address = f"{server_args.hostname}:{server_args.port}"
    server.add_insecure_port(bind_address)

    logger.info("Starting gRPC server on %s", bind_address)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_serve())
