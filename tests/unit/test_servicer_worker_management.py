import asyncio
import time
from multiprocessing import Process, Pipe
from queue import Empty, Full, ShutDown
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest
from grpc._cython.cygrpc import _Metadatum
from grpc.aio import ServicerContext
from grpc.aio._typing import MetadataType

from data_structuring.config import RunServerConfig
from grpc_api.server.address_structuring_servicer import AddressStructuringServicer
from grpc_api.server.worker_monitor import WorkerMonitor

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
            pipeline_max_instances=PIPELINE_MAX_INSTANCES,
            processing_timeout_seconds=5,
        )
        svc._input_queue = Mock()
        return svc


@pytest.fixture
def monitor() -> WorkerMonitor:
    """Create a WorkerMonitor with mocked internals to avoid spawning real processes."""
    with patch.object(WorkerMonitor, "__init__", lambda self: None):
        mon = WorkerMonitor.__new__(WorkerMonitor)
        mon._server_config = RunServerConfig(
            monitor_initial_startup_time_seconds=0,
            monitor_full_startup_time_seconds=5.0,
            pipeline_max_instances=PIPELINE_MAX_INSTANCES,
            processing_timeout_seconds=5,
            monitor_check_interval_seconds=0.01,
            worker_stall_timeout_seconds=60.0,
        )
        mon._pipeline_processes = []
        mon._global_input_queue = Mock()
        mon._global_health_queue = Mock()
        mon._global_health_queue.get_nowait.side_effect = Empty()
        mon._last_worker_health_event = {}
        mon._starting = False
        mon._shutting_down = False
        mon._replace_lock = asyncio.Lock()
        mon._monitor_task = Mock()
        return mon


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
    async def test_replaces_dead_process(self, monitor: WorkerMonitor):
        """A dead worker is reaped (terminate → join → close) and replaced with a new one."""
        dead_proc = _make_mock_process(alive=False, pid=100, exitcode=-9)
        alive_proc = _make_mock_process(alive=True, pid=202)
        new_proc = _make_mock_process(alive=True, pid=200)
        monitor._pipeline_processes = [dead_proc, alive_proc]

        with patch.object(monitor, "_start_worker", return_value=new_proc) as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 1
        dead_proc.terminate.assert_called_once()
        dead_proc.join.assert_called()
        dead_proc.close.assert_called_once()
        alive_proc.terminate.assert_not_called()
        alive_proc.close.assert_not_called()
        start_worker.assert_called_once()
        assert monitor._pipeline_processes[0] is new_proc
        assert monitor._pipeline_processes[1] is alive_proc

    @pytest.mark.asyncio
    async def test_reap_evicts_health_event(self, monitor: WorkerMonitor):
        """Reaped worker's PID is removed from _last_worker_health_event."""
        dead_proc = _make_mock_process(alive=False, pid=100, exitcode=-9)
        monitor._pipeline_processes = [dead_proc]
        monitor._last_worker_health_event = {100: time.time()}

        with patch.object(monitor, "_start_worker", return_value=_make_mock_process(pid=200)):
            await monitor._detect_dead_workers(replace=True)

        assert 100 not in monitor._last_worker_health_event

    @pytest.mark.asyncio
    async def test_replaces_missing_process(self, monitor: WorkerMonitor):
        """A missing worker is replaced with a new one."""
        alive_proc = _make_mock_process(alive=True, pid=202)
        new_proc = _make_mock_process(alive=True, pid=200)
        monitor._pipeline_processes = [alive_proc]

        with patch.object(monitor, "_start_worker", return_value=new_proc) as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 1
        alive_proc.close.assert_not_called()
        start_worker.assert_called_once()
        assert monitor._pipeline_processes[0] is alive_proc
        assert monitor._pipeline_processes[1] is new_proc

    @pytest.mark.asyncio
    async def test_leaves_alive_process_untouched(self, monitor: WorkerMonitor):
        """The alive workers are not replaced."""
        alive_proc_1 = _make_mock_process(alive=True, pid=102)
        alive_proc_2 = _make_mock_process(alive=True, pid=104)
        monitor._pipeline_processes = [alive_proc_1, alive_proc_2]

        with patch.object(monitor, "_start_worker") as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 0
        alive_proc_1.terminate.assert_not_called()
        alive_proc_1.close.assert_not_called()
        alive_proc_2.terminate.assert_not_called()
        alive_proc_2.close.assert_not_called()
        start_worker.assert_not_called()
        assert monitor._pipeline_processes[0] is alive_proc_1
        assert monitor._pipeline_processes[1] is alive_proc_2

    @pytest.mark.asyncio
    async def test_replaces_forcefully_closed_process(self, monitor: WorkerMonitor):
        """A process that raises ValueError on is_alive() is detected and replaced."""
        broken_proc = _make_mock_process(alive=True, pid=100)
        # First call (detect) raises ValueError; second call (inside _kill_worker) returns False
        broken_proc.is_alive.side_effect = [ValueError("process is closed"), False]
        alive_proc = _make_mock_process(alive=True, pid=202)
        new_proc = _make_mock_process(alive=True, pid=300)
        monitor._pipeline_processes = [broken_proc, alive_proc]

        with patch.object(monitor, "_start_worker", return_value=new_proc) as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 1
        broken_proc.close.assert_called_once()
        alive_proc.close.assert_not_called()
        start_worker.assert_called_once()
        assert monitor._pipeline_processes[0] is new_proc
        assert monitor._pipeline_processes[1] is alive_proc

    @pytest.mark.asyncio
    async def test_detects_forcefully_closed_without_replace(self, monitor: WorkerMonitor):
        """A forcefully closed process is detected but not replaced when replace=False."""
        broken_proc = _make_mock_process(alive=True, pid=100)
        # First call (detect) raises ValueError; second call (inside _kill_worker) returns False
        broken_proc.is_alive.side_effect = [ValueError("process is closed"), False]
        monitor._pipeline_processes = [broken_proc]

        with patch.object(monitor, "_start_worker") as start_worker:
            detected = await monitor._detect_dead_workers(replace=False)

        assert detected[0] == 1
        broken_proc.close.assert_called_once()
        start_worker.assert_not_called()


class TestStallDetection:

    @pytest.mark.asyncio
    async def test_detects_and_replaces_stalled_worker(self, monitor: WorkerMonitor):
        """An alive worker with a stale health event is treated as stalled and replaced."""
        stalled_proc = _make_mock_process(alive=True, pid=100)
        alive_proc = _make_mock_process(alive=True, pid=200)
        new_proc = _make_mock_process(alive=True, pid=300)

        stale_time = time.time() - monitor._server_config.worker_stall_timeout_seconds - 1
        monitor._last_worker_health_event = {100: stale_time, 200: time.time()}
        monitor._pipeline_processes = [stalled_proc, alive_proc]

        with patch.object(monitor, "_start_worker", return_value=new_proc) as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 1
        stalled_proc.terminate.assert_called_once()
        stalled_proc.join.assert_called()
        stalled_proc.close.assert_called_once()
        alive_proc.terminate.assert_not_called()
        start_worker.assert_called_once()
        assert monitor._pipeline_processes[0] is new_proc
        assert monitor._pipeline_processes[1] is alive_proc

    @pytest.mark.asyncio
    async def test_does_not_flag_idle_worker_without_health_event(self, monitor: WorkerMonitor):
        """A worker that has never completed a task is not flagged as stalled."""
        idle_proc = _make_mock_process(alive=True, pid=100)
        monitor._last_worker_health_event = {}  # no health events → never processed a task
        monitor._pipeline_processes = [idle_proc, _make_mock_process(alive=True, pid=200)]

        with patch.object(monitor, "_start_worker") as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 0
        idle_proc.terminate.assert_not_called()
        idle_proc.close.assert_not_called()
        start_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_flag_worker_with_recent_health_event(self, monitor: WorkerMonitor):
        """A worker with a recent health event is not flagged as stalled."""
        healthy_proc = _make_mock_process(alive=True, pid=100)
        monitor._last_worker_health_event = {100: time.time()}
        monitor._pipeline_processes = [healthy_proc, _make_mock_process(alive=True, pid=200)]

        with patch.object(monitor, "_start_worker") as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 0
        healthy_proc.terminate.assert_not_called()
        start_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_stall_detection_disabled_when_timeout_is_zero(self, monitor: WorkerMonitor):
        """Setting worker_stall_timeout_seconds=0 disables stall detection entirely."""
        monitor._server_config = RunServerConfig(
            pipeline_max_instances=PIPELINE_MAX_INSTANCES,
            worker_stall_timeout_seconds=0.0,
        )
        stale_proc = _make_mock_process(alive=True, pid=100)
        monitor._last_worker_health_event = {100: 0.0}  # ancient timestamp
        monitor._pipeline_processes = [stale_proc, _make_mock_process(alive=True, pid=200)]

        with patch.object(monitor, "_start_worker") as start_worker:
            detected = await monitor._detect_dead_workers(replace=True)

        assert detected[0] == 0
        stale_proc.terminate.assert_not_called()
        start_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_sigkill_sent_if_worker_ignores_sigterm(self, monitor: WorkerMonitor):
        """If a worker stays alive after SIGTERM, SIGKILL is sent."""
        stubborn_proc = _make_mock_process(alive=False, pid=100, exitcode=-9)
        # _detect: dead=True (first call); _kill_worker's is_alive check: still alive; then dead
        stubborn_proc.is_alive.side_effect = [False, True, False]
        monitor._pipeline_processes = [stubborn_proc, _make_mock_process(alive=True, pid=200)]

        with patch.object(monitor, "_start_worker", return_value=_make_mock_process(pid=300)):
            await monitor._detect_dead_workers(replace=True)

        stubborn_proc.terminate.assert_called_once()
        stubborn_proc.kill.assert_called_once()
        stubborn_proc.close.assert_called_once()


class TestProcessSamples:

    @pytest.mark.asyncio
    async def test_oserror_returns_internal(self, servicer: AddressStructuringServicer, mock_context: ServicerContext):
        """Any OSError (Pipe creation, poll, recv) returns INTERNAL."""
        with patch("grpc_api.server.process_address_tasks.Pipe", side_effect=OSError("too many open files")):
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
        servicer._input_queue.put_nowait.side_effect = Full()

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
        servicer._input_queue.put_nowait.side_effect = ShutDown()

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

        with patch("grpc_api.server.process_address_tasks.Pipe", return_value=(read_end, write_end)):
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

        with patch("grpc_api.server.process_address_tasks.Pipe", return_value=(read_end, Mock())):
            with patch("asyncio.get_event_loop", return_value=mock_event_loop):
                with patch("asyncio.Event", return_value=mock_event):
                    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                        await servicer._process_samples([], mock_context)

        assert exc_info.value.code() == grpc.StatusCode.INTERNAL
        assert "died while processing" in mock_context.abort.call_args[0][1].lower()


class TestMonitorWorkers:

    @staticmethod
    def _make_start_worker_mock(monitor: WorkerMonitor):
        """Returns a _start_worker side_effect that seeds health events so the startup wait exits."""
        pid_counter = [100]

        def start_worker(create_health_event=True):
            proc = _make_mock_process(pid=pid_counter[0])
            pid_counter[0] += 1
            monitor._last_worker_health_event[proc.pid] = time.time()
            return proc

        return start_worker

    @pytest.mark.asyncio
    async def test_monitor_stops_on_shutdown(self, monitor: WorkerMonitor):
        """The monitor loop exits when _shutting_down is set."""
        with patch.object(monitor, "_start_worker", side_effect=self._make_start_worker_mock(monitor)):
            with patch.object(monitor, "_detect_dead_workers",
                              new_callable=AsyncMock,
                              return_value=(0, PIPELINE_MAX_INSTANCES)) as mock_replace:
                task = asyncio.create_task(monitor._monitor_workers())
                await asyncio.sleep(0.05)
                monitor._shutting_down = True
                await asyncio.sleep(0.05)

        assert task.done()
        assert mock_replace.await_count >= 1

    @pytest.mark.asyncio
    async def test_monitor_calls_detect(self, monitor: WorkerMonitor):
        """The monitor invokes _detect_dead_workers each cycle after startup."""
        call_count = 0

        async def counting_call(replace: Any):
            nonlocal call_count
            call_count += 1
            if call_count >= 5:  # first call is the startup check
                monitor._shutting_down = True
            return 0, PIPELINE_MAX_INSTANCES

        with patch.object(monitor, "_start_worker", side_effect=self._make_start_worker_mock(monitor)):
            with patch.object(monitor, "_detect_dead_workers", side_effect=counting_call):
                await monitor._monitor_workers()

        assert call_count >= 5

    @pytest.mark.asyncio
    async def test_monitor_survives_exception(self, monitor: WorkerMonitor):
        """An exception in _detect_dead_workers does not kill the monitor loop."""
        call_count = 0

        async def failing_then_ok_call(replace: Any):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                raise RuntimeError("unexpected error")
            if call_count >= 5:
                monitor._shutting_down = True
            return 0, PIPELINE_MAX_INSTANCES

        with patch.object(monitor, "_start_worker", side_effect=self._make_start_worker_mock(monitor)):
            with patch.object(monitor, "_detect_dead_workers", side_effect=failing_then_ok_call):
                await monitor._monitor_workers()

        assert call_count >= 5

    @pytest.mark.asyncio
    async def test_monitor_exits_on_cancellation(self, monitor: WorkerMonitor):
        """CancelledError cleanly exits the monitor loop."""
        with patch.object(monitor, "_start_worker", side_effect=self._make_start_worker_mock(monitor)):
            with patch.object(monitor, "_detect_dead_workers",
                              new_callable=AsyncMock,
                              return_value=(0, PIPELINE_MAX_INSTANCES)):
                task = asyncio.create_task(monitor._monitor_workers())
                await asyncio.sleep(0.03)
                task.cancel()
                await asyncio.sleep(0.03)

        assert task.done()

    @pytest.mark.asyncio
    async def test_monitor_exits_if_initial_workers_dead(self, monitor: WorkerMonitor):
        """The monitor exits with code 1 if workers are dead after the initial startup check."""
        with patch.object(monitor, "_start_worker", side_effect=self._make_start_worker_mock(monitor)):
            with patch.object(monitor,
                              "_detect_dead_workers",
                              new_callable=AsyncMock,
                              return_value=(PIPELINE_MAX_INSTANCES, PIPELINE_MAX_INSTANCES)) as mock_detection:
                with pytest.raises(SystemExit) as exc_info:
                    await monitor._monitor_workers()

        assert exc_info.value.code == 1
        assert mock_detection.call_count == 1


class TestHandleHealthCheck:

    @pytest.mark.asyncio
    async def test_returns_false_immediately_when_shutting_down(self, monitor: WorkerMonitor):
        """handle_health_check returns False without calling _detect_dead_workers when shutting down."""
        monitor._shutting_down = True

        with patch.object(monitor, "_detect_dead_workers", new_callable=AsyncMock) as mock_detect:
            result = await monitor.handle_health_check()

        assert result is False
        mock_detect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_false_while_starting(self, monitor: WorkerMonitor):
        """handle_health_check returns False while workers are still starting up."""
        monitor._starting = True

        with patch.object(monitor, "_detect_dead_workers", new_callable=AsyncMock) as mock_detect:
            result = await monitor.handle_health_check()

        assert result is False
        mock_detect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_true_when_alive_workers_exist(self, monitor: WorkerMonitor):
        """handle_health_check returns True when at least one worker is alive."""
        with patch.object(monitor, "_detect_dead_workers",
                          new_callable=AsyncMock,
                          return_value=(0, PIPELINE_MAX_INSTANCES)):
            result = await monitor.handle_health_check()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_all_workers_dead(self, monitor: WorkerMonitor):
        """handle_health_check returns False when dead_count equals total."""
        with patch.object(monitor, "_detect_dead_workers",
                          new_callable=AsyncMock,
                          return_value=(PIPELINE_MAX_INSTANCES, PIPELINE_MAX_INSTANCES)):
            result = await monitor.handle_health_check()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_workers(self, monitor: WorkerMonitor):
        """handle_health_check returns False when there are no workers at all."""
        with patch.object(monitor, "_detect_dead_workers",
                          new_callable=AsyncMock,
                          return_value=(0, 0)):
            result = await monitor.handle_health_check()

        assert result is False

    @pytest.mark.asyncio
    async def test_calls_detect_without_replace(self, monitor: WorkerMonitor):
        """handle_health_check invokes _detect_dead_workers with replace=False."""
        with patch.object(monitor, "_detect_dead_workers",
                          new_callable=AsyncMock,
                          return_value=(0, PIPELINE_MAX_INSTANCES)) as mock_detect:
            await monitor.handle_health_check()

        mock_detect.assert_awaited_once_with(replace=False)

    @pytest.mark.asyncio
    async def test_returns_true_with_partial_dead_workers(self, monitor: WorkerMonitor):
        """handle_health_check returns True when some workers are dead but at least one is alive."""
        with patch.object(monitor, "_detect_dead_workers",
                          new_callable=AsyncMock,
                          return_value=(1, PIPELINE_MAX_INSTANCES)):
            result = await monitor.handle_health_check()

        assert result is True


class TestHandleShutdown:

    @pytest.mark.asyncio
    async def test_shutdown_terminates_alive_workers(self, monitor: WorkerMonitor):
        """handle_shutdown terminates all alive workers and joins them."""
        alive = _make_mock_process(alive=True, pid=1)
        dead = _make_mock_process(alive=False, pid=2)
        monitor._pipeline_processes = [alive, dead]
        monitor._monitor_task = Mock()

        await monitor.handle_shutdown()

        alive.terminate.assert_called_once()
        alive.join.assert_called_once()
        dead.terminate.assert_not_called()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_monitor_task(self, monitor: WorkerMonitor):
        """handle_shutdown cancels the background monitor task."""
        monitor._pipeline_processes = []
        monitor._monitor_task = Mock()

        await monitor.handle_shutdown()

        assert monitor._shutting_down is True
        monitor._monitor_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_closes_queues(self, monitor: WorkerMonitor):
        """handle_shutdown closes both the input queue and the health queue."""
        monitor._pipeline_processes = []
        monitor._monitor_task = Mock()

        await monitor.handle_shutdown()

        monitor._global_input_queue.close.assert_called_once()
        monitor._global_health_queue.close.assert_called_once()
