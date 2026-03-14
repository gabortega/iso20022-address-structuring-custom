from unittest.mock import Mock, AsyncMock

import grpc
import pytest

from grpc_api.interceptor.rpc_interceptor import RPCInterceptor, rpc_id_var


def _make_handler_call_details(metadata: list[tuple[str, str]]) -> grpc.HandlerCallDetails:
    details = Mock()
    details.method = "/address_structuring.AddressStructuring/ProcessAddress"
    details.invocation_metadata = metadata
    return details


def _make_stream_stream_handler(responses: list):
    """Create a mock RpcMethodHandler with a stream_stream async generator."""
    handler = Mock()

    async def fake_stream_stream(request_iterator, context):
        for r in responses:
            yield r

    handler.stream_stream = fake_stream_stream
    handler.request_deserializer = Mock()
    handler.response_serializer = Mock()
    return handler


def _make_non_streaming_handler():
    handler = Mock()
    handler.stream_stream = None
    handler.request_deserializer = Mock()
    handler.response_serializer = Mock()
    return handler


class TestRPCIdExtraction:

    @pytest.mark.asyncio
    async def test_extracts_client_rpc_id_from_metadata(self):
        """When metadata contains client-rpc-id, it is set in rpc_id_var."""
        rpc_id_var.set("default")
        interceptor = RPCInterceptor()
        details = _make_handler_call_details([("client-rpc-id", "test-123")])
        inner_handler = _make_stream_stream_handler(["resp1"])

        continuation = AsyncMock(return_value=inner_handler)
        await interceptor.intercept_service(continuation, details)

        assert rpc_id_var.get() == "test-123"

    @pytest.mark.asyncio
    async def test_falls_back_to_no_id_when_missing(self):
        """When metadata has no client-rpc-id, rpc_id_var is set to '<NO ID>'."""
        rpc_id_var.set("default")
        interceptor = RPCInterceptor()
        details = _make_handler_call_details([("other-key", "value")])
        inner_handler = _make_stream_stream_handler([])

        continuation = AsyncMock(return_value=inner_handler)
        await interceptor.intercept_service(continuation, details)

        assert rpc_id_var.get() == "<NO ID>"

    @pytest.mark.asyncio
    async def test_preserves_existing_rpc_id(self):
        """When rpc_id_var is already set (not default), it is preserved."""
        rpc_id_var.set("existing-id")
        interceptor = RPCInterceptor()
        details = _make_handler_call_details([("client-rpc-id", "new-id")])
        inner_handler = _make_stream_stream_handler([])

        continuation = AsyncMock(return_value=inner_handler)
        await interceptor.intercept_service(continuation, details)

        assert rpc_id_var.get() == "existing-id"


class TestStreamWrapping:

    @pytest.mark.asyncio
    async def test_wraps_stream_stream_and_yields_all_responses(self):
        """The wrapped handler yields all responses from the original handler."""
        rpc_id_var.set("default")
        interceptor = RPCInterceptor()
        details = _make_handler_call_details([("client-rpc-id", "abc")])
        expected = ["resp1", "resp2", "resp3"]
        inner_handler = _make_stream_stream_handler(expected)

        continuation = AsyncMock(return_value=inner_handler)
        wrapped = await interceptor.intercept_service(continuation, details)

        results = []
        async for resp in wrapped.stream_stream(iter([]), Mock()):
            results.append(resp)

        assert results == expected

    @pytest.mark.asyncio
    async def test_returns_handler_unchanged_when_no_stream_stream(self):
        """When the handler has no stream_stream, it is returned as-is."""
        rpc_id_var.set("default")
        interceptor = RPCInterceptor()
        details = _make_handler_call_details([])
        inner_handler = _make_non_streaming_handler()

        continuation = AsyncMock(return_value=inner_handler)
        result = await interceptor.intercept_service(continuation, details)

        assert result is inner_handler

    @pytest.mark.asyncio
    async def test_wrapped_handler_preserves_deserializer_and_serializer(self):
        """The wrapped handler keeps the original request_deserializer and response_serializer."""
        rpc_id_var.set("default")
        interceptor = RPCInterceptor()
        details = _make_handler_call_details([])
        inner_handler = _make_stream_stream_handler([])

        continuation = AsyncMock(return_value=inner_handler)
        wrapped = await interceptor.intercept_service(continuation, details)

        assert wrapped.request_deserializer is inner_handler.request_deserializer
        assert wrapped.response_serializer is inner_handler.response_serializer
