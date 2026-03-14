"""
Entry point for the address structuring gRPC server.
"""
import asyncio
import logging.config
import signal

import grpc

from data_structuring.config import RunServerConfig, DEFAULT_LOGGING_CONFIG
from grpc_api.generated import pb2_grpc_add_AddressStructuringServicer_to_server
from grpc_api.interceptor.rpc_interceptor import RPCInterceptor
from grpc_api.server.address_structuring_servicer import AddressStructuringServicer

logger = logging.getLogger(__name__)


async def _serve() -> None:
    server_args = RunServerConfig()

    # Create gRPC server and register the pipelines
    logger.info("Initializing gRPC server")
    server = grpc.aio.server(
        compression=server_args.grpc_compression,
        maximum_concurrent_rpcs=(
            server_args.grpc_maximum_concurrent_rpc if server_args.grpc_maximum_concurrent_rpc > 0 else None
        ),
        interceptors=[RPCInterceptor()]
    )
    logger.info("Initializing AddressStructuringServicer")
    servicer = AddressStructuringServicer(server_config=server_args)
    pb2_grpc_add_AddressStructuringServicer_to_server(
        servicer, server
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
        await servicer.handle_shutdown()

    event_loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown()))

    # Start gRPC server
    logger.info("Starting gRPC server on %s", bind_address)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
    asyncio.run(_serve())
