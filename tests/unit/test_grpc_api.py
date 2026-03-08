from typing import Any, AsyncGenerator
from unittest.mock import Mock, AsyncMock, patch

import grpc
import pytest
import pytest_asyncio
from grpc.aio._channel import Channel

from data_structuring.components.runners import ResultPostProcessing
from data_structuring.config import RunServerConfig
from grpc_api.generated import address_structuring_pb2 as pb2, address_structuring_pb2_grpc as pb2_grpc
from grpc_api.generated.address_structuring_pb2_grpc import AddressStructuringStub
from grpc_api.server.address_structuring_servicer import AddressStructuringServicer


def _mock_pipeline_result(hash_id, country_matched, country_conf, country_resolved,
                          town_matched, town_conf, town_resolved, town_origin,
                          country_start=0, country_end=5, town_start=10, town_end=15) -> ResultPostProcessing:
    """Create a mock ResultPostProcessing with one pair of matches."""
    result = Mock()
    result.hash_id = hash_id

    def country_match(i, value_if_none=None):
        if i == 0:
            return country_resolved, country_conf, country_matched
        return value_if_none, value_if_none, value_if_none

    def town_match(i, value_if_none=None):
        if i == 0:
            return town_resolved, town_conf, town_matched
        return value_if_none, value_if_none, value_if_none

    result.i_th_best_match_country.side_effect = country_match
    result.i_th_best_match_town.side_effect = town_match

    country_detail = {"start": country_start, "end": country_end, "flags": []}
    town_detail = {"start": town_start, "end": town_end, "flags": []}

    result.fuzzy_match_result.country_matches.model_dump.return_value = [country_detail]
    result.fuzzy_match_result.town_matches.model_dump.return_value = [town_detail]

    town_match_obj = Mock()
    town_match_obj.origin = town_origin
    result.fuzzy_match_result.town_matches.__getitem__ = lambda self, i: town_match_obj

    return result


def _mock_null_pipeline_result(hash_id) -> ResultPostProcessing:
    """Create a mock ResultPostProcessing with no matches."""
    result = Mock()
    result.hash_id = hash_id
    result.i_th_best_match_country.side_effect = lambda i, value_if_none=None: (
        value_if_none, value_if_none, value_if_none
    )
    result.i_th_best_match_town.side_effect = lambda i, value_if_none=None: (
        value_if_none, value_if_none, value_if_none
    )
    return result


def _make_servicer() -> AddressStructuringServicer:
    """Create a servicer with __init__ bypassed and _process_samples mocked."""
    with patch.object(AddressStructuringServicer, "__init__", lambda self: None):
        svc = AddressStructuringServicer.__new__(AddressStructuringServicer)
        svc._server_config = RunServerConfig()
        svc._process_samples = AsyncMock(return_value=[])
        return svc


@pytest_asyncio.fixture
async def grpc_env() -> AsyncGenerator[tuple[Any, AddressStructuringServicer], Any]:
    """Start a gRPC server with a servicer and yield (channel, servicer)."""
    servicer = _make_servicer()
    server = grpc.aio.server()
    pb2_grpc.add_AddressStructuringServicer_to_server(servicer, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
        yield channel, servicer
    await server.stop(grace=None)


@pytest.fixture
def stub(grpc_env: Channel) -> AddressStructuringStub:
    channel, _ = grpc_env
    return AddressStructuringStub(channel)


@pytest.fixture
def servicer(grpc_env: Channel) -> AddressStructuringServicer:
    _, svc = grpc_env
    return svc


def _sample(text, hash_id, suggested_country=None, force_suggested_country=False) -> pb2.AddressSample:
    kwargs = dict(text=text,
                  hash_id=hash_id,
                  suggested_country=suggested_country if force_suggested_country else None,
                  force_suggested_country=force_suggested_country)
    return pb2.AddressSample(**kwargs)


class TestProcessAddress:

    @pytest.mark.asyncio
    async def test_single_address(self, stub: AddressStructuringStub, servicer: AddressStructuringServicer):
        """Single address returns one result with correct country and town match fields."""
        servicer._process_samples.return_value = [
            _mock_pipeline_result(
                hash_id="abc123",
                country_matched="SWITZERLAND", country_conf=0.95, country_resolved="CH",
                town_matched="ZURICH", town_conf=0.90, town_resolved="ZURICH",
                town_origin="CH",
            )
        ]

        responses = []
        call = stub.ProcessAddress(iter([_sample("Bahnhofstrasse 1, Zurich, Switzerland", "abc123")]))
        async for resp in call:
            responses.append(resp)

        assert len(responses) == 1
        assert responses[0].hash_id == "abc123"
        assert len(responses[0].matches) == 1

        country = responses[0].matches[0].country_match
        assert country.matched == "SWITZERLAND"
        assert country.confidence_score == pytest.approx(0.95)
        assert country.resolved_name == "CH"

        town = responses[0].matches[0].town_match
        assert town.matched == "ZURICH"
        assert town.confidence_score == pytest.approx(0.90)
        assert town.resolved_name == "ZURICH"
        assert town.inferred_country_code == "CH"

    @pytest.mark.asyncio
    async def test_multiple_addresses(self, stub: AddressStructuringStub, servicer: AddressStructuringServicer):
        """Multiple streamed addresses return results in the same order."""
        servicer._process_samples.return_value = [
            _mock_pipeline_result(
                hash_id="idx1",
                country_matched="FRANCE", country_conf=0.88, country_resolved="FR",
                town_matched="PARIS", town_conf=0.92, town_resolved="PARIS",
                town_origin="FR",
            ),
            _mock_pipeline_result(
                hash_id="idx2",
                country_matched="GERMANY", country_conf=0.85, country_resolved="DE",
                town_matched="BERLIN", town_conf=0.87, town_resolved="BERLIN",
                town_origin="DE",
            ),
        ]

        samples = [
            _sample("10 Rue de Rivoli, Paris, France", "idx1"),
            _sample("Alexanderplatz 1, Berlin, Germany", "idx2"),
        ]
        responses = []
        async for resp in stub.ProcessAddress(iter(samples)):
            responses.append(resp)

        assert len(responses) == 2
        assert responses[0].hash_id == "idx1"
        assert responses[1].hash_id == "idx2"

    @pytest.mark.asyncio
    async def test_null_matches_skipped(self, stub: AddressStructuringStub, servicer: AddressStructuringServicer):
        """When both country and town are None for a given rank, that match is skipped."""
        servicer._process_samples.return_value = [_mock_null_pipeline_result("idx1")]

        responses = []
        async for resp in stub.ProcessAddress(iter([_sample("some gibberish", "idx1")])):
            responses.append(resp)

        assert len(responses) == 1
        assert responses[0].hash_id == "idx1"
        assert len(responses[0].matches) == 0

    @pytest.mark.asyncio
    async def test_empty_stream(self, stub: AddressStructuringStub, servicer: AddressStructuringServicer):
        """Empty input stream returns no results."""

        responses = []
        async for resp in stub.ProcessAddress(iter([])):
            responses.append(resp)

        assert responses == []

    @pytest.mark.asyncio
    async def test_suggested_country_forwarded(self,
                                               stub: AddressStructuringStub,
                                               servicer: AddressStructuringServicer):
        """Verify suggestedCountry and forceSuggestedCountry reach _process_samples."""

        responses = []
        async for resp in stub.ProcessAddress(
                iter([_sample("123 Main St", "idx1", suggested_country="US", force_suggested_country=True)])
        ):
            responses.append(resp)

        servicer._process_samples.assert_awaited_once()
        samples = servicer._process_samples.call_args[0][0]
        assert len(samples) == 1
        assert samples[0].suggested_country == "US"
        assert samples[0].force_suggested_country is True

    @pytest.mark.asyncio
    async def test_pipeline_error_returns_internal(self,
                                                   stub: AddressStructuringStub,
                                                   servicer: AddressStructuringServicer):
        """Pipeline exception is caught and returned as INTERNAL gRPC status."""
        servicer._process_samples.side_effect = RuntimeError("pipeline broke")

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            async for _ in stub.ProcessAddress(iter([_sample("test", "idx1")])):
                pass

        assert exc_info.value.code() == grpc.StatusCode.INTERNAL
        assert "pipeline broke" in exc_info.value.details()

    @pytest.mark.asyncio
    async def test_response_contains_start_end_indices(self,
                                                       stub: AddressStructuringStub,
                                                       servicer: AddressStructuringServicer):
        """Match results include the correct start_index and end_index."""
        servicer._process_samples.return_value = [
            _mock_pipeline_result(
                hash_id="idx1",
                country_matched="CH", country_conf=0.9, country_resolved="CH",
                town_matched="BERN", town_conf=0.8, town_resolved="BERN",
                town_origin="CH",
                country_start=20, country_end=22,
                town_start=10, town_end=14,
            )
        ]

        responses = []
        async for resp in stub.ProcessAddress(
                iter([_sample("Marktgasse 1 Bern Switzerland", "idx1")])
        ):
            responses.append(resp)

        match = responses[0].matches[0]
        assert match.country_match.start_index == 20
        assert match.country_match.end_index == 22
        assert match.town_match.start_index == 10
        assert match.town_match.end_index == 14
