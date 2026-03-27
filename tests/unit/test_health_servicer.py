from unittest.mock import AsyncMock, patch, Mock

import pytest
from grpc_health.v1 import health_pb2

from grpc_api.health.health_servicer import AddressStructuringHealthServicer, SERVICE_NAME


def _make_request(service: str) -> Mock:
    req = Mock(spec=health_pb2.HealthCheckRequest)
    req.service = service
    return req


def _make_health_servicer(health_check_return: bool = True) -> AddressStructuringHealthServicer:
    """Return an AddressStructuringHealthServicer with a mocked inner servicer and self.set."""
    address_structuring_servicer = Mock()
    address_structuring_servicer.monitor.handle_health_check = AsyncMock(return_value=health_check_return)
    health_svc = AddressStructuringHealthServicer(address_structuring_servicer)
    health_svc.set = AsyncMock()
    return health_svc


class TestCheck:

    @pytest.mark.asyncio
    async def test_sets_serving_for_service_name_when_healthy(self):
        """Check() sets SERVING on both keys when service == SERVICE_NAME and workers are healthy."""
        health_svc = _make_health_servicer(health_check_return=True)

        with patch("grpc_health.v1._async.HealthServicer.Check", new=AsyncMock()):
            await health_svc.Check(_make_request(SERVICE_NAME), AsyncMock())

        calls = health_svc.set.await_args_list
        assert len(calls) == 2
        assert calls[0].args == (SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)
        assert calls[1].args == ("", health_pb2.HealthCheckResponse.SERVING)

    @pytest.mark.asyncio
    async def test_sets_not_serving_for_service_name_when_unhealthy(self):
        """Check() sets NOT_SERVING on both keys when service == SERVICE_NAME and workers are down."""
        health_svc = _make_health_servicer(health_check_return=False)

        with patch("grpc_health.v1._async.HealthServicer.Check", new=AsyncMock()):
            await health_svc.Check(_make_request(SERVICE_NAME), AsyncMock())

        calls = health_svc.set.await_args_list
        assert len(calls) == 2
        assert calls[0].args == (SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING)
        assert calls[1].args == ("", health_pb2.HealthCheckResponse.NOT_SERVING)

    @pytest.mark.asyncio
    async def test_sets_serving_for_empty_service_when_healthy(self):
        """Check() sets SERVING on both keys when service == '' and workers are healthy."""
        health_svc = _make_health_servicer(health_check_return=True)

        with patch("grpc_health.v1._async.HealthServicer.Check", new=AsyncMock()):
            await health_svc.Check(_make_request(""), AsyncMock())

        calls = health_svc.set.await_args_list
        assert len(calls) == 2
        assert calls[0].args == (SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)
        assert calls[1].args == ("", health_pb2.HealthCheckResponse.SERVING)

    @pytest.mark.asyncio
    async def test_skips_status_update_for_unrelated_service(self):
        """Check() does not call handle_health_check or set() for services other than '' or SERVICE_NAME."""
        health_svc = _make_health_servicer()

        with patch("grpc_health.v1._async.HealthServicer.Check", new=AsyncMock()):
            await health_svc.Check(_make_request("some.other.Service"), AsyncMock())

        health_svc._address_structuring_servicer.monitor.handle_health_check.assert_not_awaited()
        health_svc.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_always_delegates_to_parent_check(self):
        """Check() always returns whatever the parent HealthServicer.Check returns."""
        health_svc = _make_health_servicer()

        expected = Mock(spec=health_pb2.HealthCheckResponse)
        with patch("grpc_health.v1._async.HealthServicer.Check", new=AsyncMock(return_value=expected)):
            result = await health_svc.Check(_make_request(SERVICE_NAME), AsyncMock())

        assert result is expected

    @pytest.mark.asyncio
    async def test_awaits_handle_health_check_on_inner_servicer(self):
        """Check() awaits handle_health_check() on the wrapped AddressStructuringServicer."""
        health_svc = _make_health_servicer()

        with patch("grpc_health.v1._async.HealthServicer.Check", new=AsyncMock()):
            await health_svc.Check(_make_request(SERVICE_NAME), AsyncMock())

        health_svc._address_structuring_servicer.monitor.handle_health_check.assert_awaited_once()
