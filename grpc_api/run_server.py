"""
Entry point for the address structuring gRPC server.
"""
import asyncio
import logging
import signal
from concurrent import futures

import grpc

from data_structuring.config import RunServerConfig
from data_structuring.pipeline import AddressStructuringPipeline
from grpc_api.generated import pb2_grpc_add_AddressStructuringServicer_to_server
from grpc_api.server.address_structuring_servicer import AddressStructuringServicer

logger = logging.getLogger(__name__)


async def _serve():
    server_args = RunServerConfig()

    # Create AddressStructuringPipeline
    logger.info("Initializing AddressStructuringPipeline")
    pipeline = AddressStructuringPipeline(batch_size=server_args.batch_size)
    logger.info("Pipeline ready")

    # Create gRPC server and register the pipeline
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=server_args.max_workers))
    pb2_grpc_add_AddressStructuringServicer_to_server(
        AddressStructuringServicer(pipeline), server
    )

    # Bind server address and port
    bind_address = f"{server_args.hostname}:{server_args.port}"

    # Handle SSL/TLS options
    if server_args.ssl_enabled:
        if not server_args.ssl_cert_path or not server_args.ssl_key_path:
            raise ValueError("SSL is enabled but ssl_cert_path and ssl_key_path must both be provided")

        server_cert = server_args.ssl_cert_path.read_bytes()
        server_key = server_args.ssl_key_path.read_bytes()
        ca_cert = server_args.ssl_ca_cert_path.read_bytes() if server_args.ssl_ca_cert_path else None

        credentials = grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=[(server_key, server_cert)],
            root_certificates=ca_cert,
            require_client_auth=ca_cert is not None,
        )
        server.add_secure_port(bind_address, credentials)
        logger.info("SSL enabled (mutual TLS: %s)", ca_cert is not None)
    else:
        server.add_insecure_port(bind_address)

    # Add graceful shutdown timer
    async def _shutdown():
        logger.info("Shutting down gRPC server (grace=%ds)...", server_args.shutdown_grace_seconds)
        await server.stop(grace=server_args.shutdown_grace_seconds)

    event_loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown()))

    # Start gRPC server
    logger.info("Starting gRPC server on %s", bind_address)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_serve())
