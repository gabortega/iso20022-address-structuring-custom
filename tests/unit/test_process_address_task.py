import asyncio
from multiprocessing import Pipe
from unittest.mock import Mock

import pytest

from grpc_api.server.process_address_tasks import WorkerInput, ProcessAddressTask


class TestProcessAddressTaskContextManager:

    def test_enter_creates_event_loop_and_pipe(self):
        """__enter__ initializes tracked_event, event_loop, and pipe channels."""
        task = ProcessAddressTask(rpc_id="test-1", address_samples=[])

        with task as t:
            assert isinstance(t.tracked_event, asyncio.Event)
            assert t.event_loop is not None
            assert t.read_channel is not None
            assert t.write_channel is not None

    def test_exit_closes_channels(self):
        """__exit__ closes both read and write pipe channels."""
        task = ProcessAddressTask(rpc_id="test-1", address_samples=[])

        with task:
            read_channel = task.read_channel
            write_channel = task.write_channel

        assert read_channel.closed
        assert write_channel.closed

    def test_exit_sets_tracked_event(self):
        """__exit__ sets the tracked event to unblock any waiters."""
        task = ProcessAddressTask(rpc_id="test-1", address_samples=[])

        with task:
            event = task.tracked_event
            assert not event.is_set()

        assert event.is_set()


class TestCreateAndSendWorkerInput:

    def test_puts_worker_input_on_queue(self):
        """create_and_send_worker_input puts a WorkerInput on the queue."""
        samples = [Mock(), Mock()]
        task = ProcessAddressTask(rpc_id="rpc-42", address_samples=samples)
        queue = Mock()

        with task:
            task.send_worker_input(queue)

        queue.put_nowait.assert_called_once()
        worker_input = queue.put_nowait.call_args[0][0]
        assert isinstance(worker_input, WorkerInput)
        assert worker_input.rpc_id == "rpc-42"
        assert worker_input.address_samples is samples
        assert worker_input.write_channel is task.write_channel


class TestWaitForResults:

    @pytest.mark.asyncio
    async def test_returns_data_sent_through_pipe(self):
        """wait_for_results returns data written to the pipe by a worker."""
        task = ProcessAddressTask(rpc_id="test-1", address_samples=[])
        expected = [{"result": "ok"}]

        with task:
            # Simulate worker writing results
            task.write_channel.send(expected)
            result = await task.wait_for_results(timeout=5.0)

        assert result == expected

    @pytest.mark.asyncio
    async def test_timeout_raises(self):
        """wait_for_results raises TimeoutError when no data arrives."""
        task = ProcessAddressTask(rpc_id="test-1", address_samples=[])

        with task:
            with pytest.raises(TimeoutError):
                await task.wait_for_results(timeout=0.01)


class TestWorkerInput:

    def test_context_manager_closes_write_channel(self):
        """__exit__ closes the write channel."""
        _, write_end = Pipe(duplex=False)
        worker = WorkerInput(rpc_id="rpc-1", address_samples=[], write_channel=write_end)

        with worker:
            assert not write_end.closed

        assert write_end.closed

    def test_send_writes_to_pipe(self):
        """send() writes data through the pipe."""
        read_end, write_end = Pipe(duplex=False)
        worker = WorkerInput(rpc_id="rpc-1", address_samples=[], write_channel=write_end)

        worker.send({"data": 123})
        assert read_end.recv() == {"data": 123}
        read_end.close()
        write_end.close()

    def test_send_swallows_broken_pipe_error(self):
        """send() does not raise when the read end is already closed."""
        read_end, write_end = Pipe(duplex=False)
        read_end.close()
        worker = WorkerInput(rpc_id="rpc-1", address_samples=[], write_channel=write_end)

        # Should not raise
        worker.send("data")
        write_end.close()
