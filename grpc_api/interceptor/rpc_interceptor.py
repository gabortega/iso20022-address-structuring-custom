import contextvars
import logging.config
from typing import Callable, Awaitable, AsyncIterable, Any

import grpc
from grpc._utilities import RpcMethodHandler

logger = logging.getLogger(__name__)
rpc_id_var = contextvars.ContextVar("rpc_id", default="default")


class RPCInterceptor(grpc.aio.ServerInterceptor):

    async def intercept_service(
            self,
            continuation: Callable[
                [grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]
            ],
            handler_call_details: grpc.HandlerCallDetails,
    ) -> RpcMethodHandler:
        """
        This interceptor prepends its tag to the rpc_id.
        If two of these interceptors are chained together, the resulting rpc_id
        will be something like this: Interceptor2-Interceptor1-RPC_ID.
        """

        # Get or create the rpc-id
        if rpc_id_var.get() == "default":
            _metadata = dict(handler_call_details.invocation_metadata)
            if "client-rpc-id" in _metadata:
                rpc_id_var.set(_metadata["client-rpc-id"])
            else:
                rpc_id_var.set("<NO ID>")
        else:
            rpc_id_var.set(rpc_id_var.get())

        handler = await continuation(handler_call_details)

        if handler.stream_stream:
            original_stream_stream = handler.stream_stream

            async def wrapped_stream_stream(request_iterator: AsyncIterable[Any],
                                            context: grpc.aio.ServicerContext):

                logger.info("Received RPC-%s that called %s", rpc_id_var.get(), handler_call_details.method)

                response_count = 0
                async for response in original_stream_stream(request_iterator, context):
                    response_count += 1
                    yield response

                logger.info("Processed %s addresses and sent all results for RPC-%s", response_count, rpc_id_var.get())

            return grpc.stream_stream_rpc_method_handler(
                wrapped_stream_stream,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        return handler
