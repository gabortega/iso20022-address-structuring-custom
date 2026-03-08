import asyncio
from multiprocessing import Process, Pipe
from queue import Full, ShutDown
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest
from grpc.aio import ServicerContext
from grpc.aio._typing import MetadataType

from data_structuring.config import RunServerConfig
from grpc_api.server.address_structuring_servicer import AddressStructuringServicer


def _make_mock_process(alive: bool = True, pid: int = 1000, exitcode: int = None) -> Process:
    """Create a mock Process object."""
    process = Mock()
    process.is_alive.return_value = alive
    process.pid = pid
    process.exitcode = exitcode
    return process


@pytest.fixture
def servicer() -> AddressStructuringServicer:
    """Create a servicer with mocked internals to avoid spawning real processes."""
    with patch.object(AddressStructuringServicer, "__init__", lambda self: None):
        svc = AddressStructuringServicer.__new__(AddressStructuringServicer)
        svc._server_config = RunServerConfig(
            pipeline_max_instances=2,
            processing_timeout_seconds=5
        )
        svc._pipeline_processes = []
        svc._global_queue = Mock()
        svc._shutting_down = False
        svc._replace_lock = asyncio.Lock()
        svc._monitor_task = Mock()
        return svc


@pytest.fixture
def mock_context() -> ServicerContext:
    """Create a mock gRPC context whose abort raises to stop execution."""
    ctx = AsyncMock()

    async def abort_side_effect(code: grpc.StatusCode,
                                details: str = "",
                                trailing_metadata: MetadataType = ()):
        raise grpc.aio.AioRpcError(code, None, trailing_metadata, details=details)

    ctx.abort.side_effect = abort_side_effect
    return ctx


class TestReplaceDeadWorkers:

    @pytest.mark.asyncio
    async def test_replaces_dead_process(self, servicer: AddressStructuringServicer):
        """A dead worker is closed and replaced with a new one."""
        dead_proc = _make_mock_process(alive=False, pid=100, exitcode=-9)
        alive_proc = _make_mock_process(alive=True, pid=202)
        new_proc = _make_mock_process(alive=True, pid=200)
        servicer._pipeline_processes = [dead_proc, alive_proc]

        with patch.object(servicer, "_start_worker", return_value=new_proc) as start_worker:
            replaced = await servicer._replace_dead_workers()

        assert replaced == 1
        dead_proc.close.assert_called_once()
        alive_proc.close.assert_not_called()
        start_worker.assert_called_once()
        assert servicer._pipeline_processes[0] is new_proc
        assert servicer._pipeline_processes[1] is alive_proc

    @pytest.mark.asyncio
    async def test_replaces_missing_process(self, servicer: AddressStructuringServicer):
        """A missing worker is replaced with a new one."""
        alive_proc = _make_mock_process(alive=True, pid=202)
        new_proc = _make_mock_process(alive=True, pid=200)
        servicer._pipeline_processes = [alive_proc]

        with patch.object(servicer, "_start_worker", return_value=new_proc) as start_worker:
            replaced = await servicer._replace_dead_workers()

        assert replaced == 1
        alive_proc.close.assert_not_called()
        start_worker.assert_called_once()
        assert servicer._pipeline_processes[0] is alive_proc
        assert servicer._pipeline_processes[1] is new_proc

    @pytest.mark.asyncio
    async def test_leaves_alive_process_untouched(self, servicer: AddressStructuringServicer):
        """The alive workers are not replaced."""
        alive_proc_1 = _make_mock_process(alive=True, pid=102)
        alive_proc_2 = _make_mock_process(alive=True, pid=104)
        servicer._pipeline_processes = [alive_proc_1, alive_proc_2]

        with patch.object(servicer, "_start_worker") as start_worker:
            replaced = await servicer._replace_dead_workers()

        assert replaced == 0
        alive_proc_1.close.assert_not_called()
        alive_proc_2.close.assert_not_called()
        start_worker.assert_not_called()
        assert servicer._pipeline_processes[0] is alive_proc_1
        assert servicer._pipeline_processes[1] is alive_proc_2


class TestProcessSamples:

    @pytest.mark.asyncio
    async def test_oserror_returns_internal(self, servicer: AddressStructuringServicer, mock_context: ServicerContext):
        """Any OSError (Pipe creation, poll, recv) returns INTERNAL."""
        with patch("grpc_api.server.address_structuring_servicer.Pipe", side_effect=OSError("too many open files")):
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await servicer._process_samples([], mock_context)

        assert exc_info.value.code() == grpc.StatusCode.INTERNAL
        mock_context.abort.assert_awaited_once()
        assert "communicating with worker" in mock_context.abort.call_args[0][1].lower()

    @pytest.mark.asyncio
    async def test_queue_full_returns_resource_exhausted(self,
                                                         servicer: AddressStructuringServicer,
                                                         mock_context: ServicerContext):
        """When the queue is full, RESOURCE_EXHAUSTED is raised."""
        servicer._global_queue.put_nowait.side_effect = Full()

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await servicer._process_samples([], mock_context)

        assert exc_info.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
        mock_context.abort.assert_awaited_once()
        assert "queue full" in mock_context.abort.call_args[0][1].lower()

    @pytest.mark.asyncio
    async def test_queue_shutdown_returns_internal(self,
                                                   servicer: AddressStructuringServicer,
                                                   mock_context: ServicerContext):
        """When the queue has been shut down, INTERNAL is raised."""
        servicer._global_queue.put_nowait.side_effect = ShutDown()

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await servicer._process_samples([], mock_context)

        assert exc_info.value.code() == grpc.StatusCode.INTERNAL
        mock_context.abort.assert_awaited_once()
        assert "shutting down" in mock_context.abort.call_args[0][1].lower()

    @pytest.mark.asyncio
    async def test_timeout_returns_deadline_exceeded(self,
                                                     servicer: AddressStructuringServicer,
                                                     mock_context: ServicerContext):
        """When poll() times out, DEADLINE_EXCEEDED is raised."""
        read_end = Mock()
        read_end.poll.return_value = False  # simulate timeout

        with patch("grpc_api.server.address_structuring_servicer.Pipe", return_value=(read_end, Mock())):
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await servicer._process_samples([], mock_context)

        assert exc_info.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        mock_context.abort.assert_awaited_once()
        assert "did not return a result" in mock_context.abort.call_args[0][1].lower()

    @pytest.mark.asyncio
    async def test_successful_result_returned(self,
                                              servicer: AddressStructuringServicer,
                                              mock_context: ServicerContext):
        """When a worker sends results back through the pipe, the results are returned correctly."""
        expected_result = [{"result": "ok"}]

        read_end, write_end = Pipe(duplex=False)
        write_end.send(expected_result)

        with patch("grpc_api.server.address_structuring_servicer.Pipe", return_value=(read_end, write_end)):
            result = await servicer._process_samples([], mock_context)

        assert result == expected_result
        mock_context.abort.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_recv_eoferror_returns_deadline_exceeded(self,
                                                           servicer: AddressStructuringServicer,
                                                           mock_context: ServicerContext):
        """When recv() raises EOFError (worker crashed mid-task), DEADLINE_EXCEEDED is raised."""
        read_end = Mock()
        read_end.poll.return_value = True
        read_end.recv.side_effect = EOFError()

        with patch("grpc_api.server.address_structuring_servicer.Pipe", return_value=(read_end, Mock())):
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await servicer._process_samples([], mock_context)

        assert exc_info.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        assert "died while processing" in mock_context.abort.call_args[0][1].lower()


class TestMonitorWorkers:

    @pytest.mark.asyncio
    async def test_monitor_stops_on_shutdown(self, servicer: AddressStructuringServicer):
        """The monitor loop exits when _shutting_down is set."""
        servicer._server_config.pipeline_health_check_interval_seconds = 0.01

        with patch.object(servicer, "_replace_dead_workers", new_callable=AsyncMock, return_value=0) as mock_replace:
            # Let the monitor run briefly then shut it down
            task = asyncio.create_task(servicer._monitor_workers())
            await asyncio.sleep(0.05)

            # Set the shutting down flag
            servicer._shutting_down = True

            await asyncio.sleep(0.05)

            assert task.done()
            # It should have called replace at least once
            assert mock_replace.await_count >= 1

    @pytest.mark.asyncio
    async def test_monitor_calls_replace(self, servicer: AddressStructuringServicer):
        """The monitor invokes _replace_dead_workers each cycle."""
        servicer._server_config.pipeline_health_check_interval_seconds = 0.01
        call_count = 0

        async def counting_replace():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                servicer._shutting_down = True
            return 0

        with patch.object(servicer, "_replace_dead_workers", side_effect=counting_replace):
            await servicer._monitor_workers()

        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_monitor_survives_exception(self, servicer: AddressStructuringServicer):
        """An exception in _replace_dead_workers does not kill the monitor loop."""
        servicer._server_config.pipeline_health_check_interval_seconds = 0.01
        call_count = 0

        async def failing_then_ok_replace():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unexpected error")
            if call_count >= 3:
                servicer._shutting_down = True
            return 0

        with patch.object(servicer, "_replace_dead_workers", side_effect=failing_then_ok_replace):
            await servicer._monitor_workers()

        # Monitor survived the first failure and continued for at least 2 more cycles
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_monitor_exits_on_cancellation(self, servicer: AddressStructuringServicer):
        """CancelledError cleanly exits the monitor loop."""
        servicer._server_config.pipeline_health_check_interval_seconds = 0.01

        with patch.object(servicer, "_replace_dead_workers", new_callable=AsyncMock, return_value=0):
            task = asyncio.create_task(servicer._monitor_workers())
            await asyncio.sleep(0.03)

            # Cancel the task
            task.cancel()

            await asyncio.sleep(0.03)

            assert task.done()


class TestHandleShutdown:

    @pytest.mark.asyncio
    async def test_shutdown_terminates_alive_workers(self, servicer: AddressStructuringServicer):
        """handle_shutdown terminates all alive workers and joins them."""
        alive = _make_mock_process(alive=True, pid=1)
        dead = _make_mock_process(alive=False, pid=2)
        servicer._pipeline_processes = [alive, dead]
        servicer._monitor_task = Mock()

        await servicer.handle_shutdown()

        alive.terminate.assert_called_once()
        alive.join.assert_called_once()
        dead.terminate.assert_not_called()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_health_check(self, servicer: AddressStructuringServicer):
        """handle_shutdown cancels the background monitor task."""
        servicer._pipeline_processes = []
        servicer._monitor_task = Mock()

        await servicer.handle_shutdown()

        assert servicer._shutting_down is True
        servicer._monitor_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_closes_queue(self, servicer: AddressStructuringServicer):
        """handle_shutdown closes the global queue."""
        servicer._pipeline_processes = []
        servicer._monitor_task = Mock()

        await servicer.handle_shutdown()

        servicer._global_queue.close.assert_called_once()
