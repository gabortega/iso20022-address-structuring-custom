import asyncio
from multiprocessing import Process, Pipe
from queue import Full, ShutDown
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest
from grpc._cython.cygrpc import _Metadatum
from grpc.aio import ServicerContext
from grpc.aio._typing import MetadataType

from data_structuring.config import RunServerConfig
from grpc_api.server.address_structuring_servicer import AddressStructuringServicer

PIPELINE_MAX_INSTANCES = 2


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
            monitor_startup_time_seconds=0,
            pipeline_max_instances=PIPELINE_MAX_INSTANCES,
            processing_timeout_seconds=5,
            monitor_check_interval_seconds=0.01
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

    ctx.invocation_metadata = lambda: (_Metadatum("dummy", "val"),)
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
            detected = await servicer._detect_dead_workers(replace=True)

        assert detected[0] == 1
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
            detected = await servicer._detect_dead_workers(replace=True)

        assert detected[0] == 1
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
            detected = await servicer._detect_dead_workers(replace=True)

        assert detected[0] == 0
        alive_proc_1.close.assert_not_called()
        alive_proc_2.close.assert_not_called()
        start_worker.assert_not_called()
        assert servicer._pipeline_processes[0] is alive_proc_1
        assert servicer._pipeline_processes[1] is alive_proc_2

    @pytest.mark.asyncio
    async def test_replaces_forcefully_closed_process(self, servicer: AddressStructuringServicer):
        """A process that raises ValueError on is_alive() is detected and replaced."""
        broken_proc = _make_mock_process(alive=True, pid=100)
        broken_proc.is_alive.side_effect = ValueError("process is closed")
        alive_proc = _make_mock_process(alive=True, pid=202)
        new_proc = _make_mock_process(alive=True, pid=300)
        servicer._pipeline_processes = [broken_proc, alive_proc]

        with patch.object(servicer, "_start_worker", return_value=new_proc) as start_worker:
            detected = await servicer._detect_dead_workers(replace=True)

        assert detected[0] == 1
        broken_proc.close.assert_called_once()
        alive_proc.close.assert_not_called()
        start_worker.assert_called_once()
        assert servicer._pipeline_processes[0] is new_proc
        assert servicer._pipeline_processes[1] is alive_proc

    @pytest.mark.asyncio
    async def test_detects_forcefully_closed_without_replace(self, servicer: AddressStructuringServicer):
        """A forcefully closed process is detected but not replaced when replace=False."""
        broken_proc = _make_mock_process(alive=True, pid=100)
        broken_proc.is_alive.side_effect = ValueError("process is closed")
        servicer._pipeline_processes = [broken_proc]

        with patch.object(servicer, "_start_worker") as start_worker:
            detected = await servicer._detect_dead_workers(replace=False)

        assert detected[0] == 1
        broken_proc.close.assert_called_once()
        start_worker.assert_not_called()


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
    async def test_recv_eoferror_returns_internal(self,
                                                  servicer: AddressStructuringServicer,
                                                  mock_context: ServicerContext):
        """When recv() raises EOFError (worker crashed mid-task), INTERNAL is raised."""
        mock_event_loop = Mock()
        mock_event_loop.add_reader = lambda _, __: None

        async def event_no_wait():
            return True

        mock_event = AsyncMock()
        mock_event.wait = event_no_wait

        read_end = Mock()
        read_end.recv.return_value = Exception()

        with patch("grpc_api.server.address_structuring_servicer.Pipe", return_value=(read_end, Mock())):
            with patch("asyncio.get_event_loop", return_value=mock_event_loop):
                with patch("asyncio.Event", return_value=mock_event):
                    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                        await servicer._process_samples([], mock_context)

        assert exc_info.value.code() == grpc.StatusCode.INTERNAL
        assert "died while processing" in mock_context.abort.call_args[0][1].lower()


class TestMonitorWorkers:

    @pytest.mark.asyncio
    async def test_monitor_stops_on_shutdown(self, servicer: AddressStructuringServicer):
        """The monitor loop exits when _shutting_down is set."""

        with patch.object(servicer, "_detect_dead_workers", new_callable=AsyncMock, return_value=0) as mock_replace:
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
    async def test_monitor_calls_detect(self, servicer: AddressStructuringServicer):
        """The monitor invokes _detect_dead_workers each cycle."""
        call_count = 0

        async def counting_call(replace: Any):
            nonlocal call_count
            call_count += 1
            if call_count >= 4:
                servicer._shutting_down = True
            return 0, PIPELINE_MAX_INSTANCES

        with patch.object(servicer, "_detect_dead_workers", side_effect=counting_call):
            await servicer._monitor_workers()

        assert call_count >= 4

    @pytest.mark.asyncio
    async def test_monitor_survives_exception(self, servicer: AddressStructuringServicer):
        """An exception in _detect_dead_workers does not kill the monitor loop."""
        call_count = 0

        async def failing_then_ok_call(replace: Any):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("unexpected error")
            if call_count >= 4:
                servicer._shutting_down = True
            return 0, PIPELINE_MAX_INSTANCES

        with patch.object(servicer, "_detect_dead_workers", side_effect=failing_then_ok_call):
            await servicer._monitor_workers()

        # Monitor survived the first failure and continued for at least 2 more cycles
        assert call_count >= 4

    @pytest.mark.asyncio
    async def test_monitor_exits_on_cancellation(self, servicer: AddressStructuringServicer):
        """CancelledError cleanly exits the monitor loop."""

        with patch.object(servicer, "_detect_dead_workers", new_callable=AsyncMock, return_value=0):
            task = asyncio.create_task(servicer._monitor_workers())
            await asyncio.sleep(0.03)

            # Cancel the task
            task.cancel()

            await asyncio.sleep(0.03)

            assert task.done()

    @pytest.mark.asyncio
    async def test_monitor_exits_if_initial_workers_dead(self, servicer: AddressStructuringServicer):
        """The monitor shouldn't proceed if none of the initial workers are alive."""

        with patch.object(servicer,
                          "_detect_dead_workers",
                          new_callable=AsyncMock,
                          return_value=(PIPELINE_MAX_INSTANCES, PIPELINE_MAX_INSTANCES)) as mock_detection:
            with pytest.raises(SystemExit) as exc_info:
                await servicer._monitor_workers()

            assert exc_info.value.code == 1
            assert mock_detection.call_count == 1


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
