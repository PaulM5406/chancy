import asyncio
import contextlib
import signal
import time

import pytest

from chancy import Chancy, Worker, Queue, QueuedJob, job


@job()
async def async_quick_sleeper():
    await asyncio.sleep(0.5)


@job()
async def async_long_sleeper():
    await asyncio.sleep(60)


@job()
def sync_quick_sleeper():
    time.sleep(0.5)


@job()
def sync_slow_sleeper():
    time.sleep(10)


async def _wait_for_running(chancy: Chancy, ref, timeout: float = 10):
    return await chancy.wait_for_job(
        ref,
        states={QueuedJob.State.RUNNING},
        interval=0.05,
        timeout=timeout,
    )


@pytest.mark.asyncio
async def test_stop_is_idempotent(chancy: Chancy):
    """
    Calling stop() more than once is a no-op.
    """
    worker = Worker(chancy, shutdown_timeout=5, register_signal_handlers=False)
    await worker.start()
    try:
        first = await worker.stop()
        second = await worker.stop()
        assert first is True
        assert second is True
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_worker_can_restart_after_stop(chancy: Chancy):
    """
    A stopped worker instance can be started again.
    """
    await chancy.declare(
        Queue("shutdown_restart", executor=Chancy.Executor.Async)
    )
    worker = Worker(chancy, shutdown_timeout=5, register_signal_handlers=False)

    await worker.start()
    await worker.hub.wait_for("worker.queue.started", timeout=10)
    assert await worker.stop() is True

    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            async_quick_sleeper.job.with_queue("shutdown_restart")
        )
        j = await chancy.wait_for_job(ref, interval=0.05, timeout=15)
        assert j.state == QueuedJob.State.SUCCEEDED
    finally:
        assert await worker.stop() is True

    # The second stop really tore the worker down.
    assert len(worker.manager) == 0


@pytest.mark.asyncio
async def test_async_in_flight_job_finishes_during_shutdown(chancy: Chancy):
    """
    Test that a short async job in flight when stop() is called gets to
    finish instead of being cancelled.
    """
    await chancy.declare(
        Queue("shutdown_async", executor=Chancy.Executor.Async)
    )
    worker = Worker(chancy, shutdown_timeout=10, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            async_quick_sleeper.job.with_queue("shutdown_async")
        )
        await _wait_for_running(chancy, ref)

        cleanly = await worker.stop()
        assert cleanly is True

        j = await chancy.get_job(ref)
        assert j.state == QueuedJob.State.SUCCEEDED
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_async_job_force_cancelled_when_drain_times_out(chancy: Chancy):
    """
    A job that outlives shutdown_timeout gets force-cancelled, and its
    final state still makes it to the database.
    """
    await chancy.declare(
        Queue("shutdown_async_kill", executor=Chancy.Executor.Async)
    )
    worker = Worker(chancy, shutdown_timeout=2, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            async_long_sleeper.job.with_queue("shutdown_async_kill")
        )
        await _wait_for_running(chancy, ref)

        timeout_emitted = asyncio.Event()
        worker.hub.on(
            "worker.shutdown_timeout",
            lambda event: timeout_emitted.set(),
        )

        start = time.monotonic()
        cleanly = await worker.stop()
        elapsed = time.monotonic() - start

        assert cleanly is False
        assert elapsed < 10  # must not block forever
        assert timeout_emitted.is_set()

        # Not left as a stale RUNNING row claimed by a dead worker.
        j = await chancy.get_job(ref)
        assert j.state in (
            QueuedJob.State.RETRYING,
            QueuedJob.State.FAILED,
        )
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_sync_in_flight_job_finishes_during_shutdown(
    chancy: Chancy, sync_executor: str
):
    """
    Same as the async case, for each of the sync executors.
    """
    await chancy.declare(
        Queue("shutdown_sync", executor=sync_executor, concurrency=1)
    )
    worker = Worker(chancy, shutdown_timeout=10, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            sync_quick_sleeper.job.with_queue("shutdown_sync")
        )
        await _wait_for_running(chancy, ref, timeout=15)

        cleanly = await worker.stop()
        assert cleanly is True

        j = await chancy.get_job(ref)
        assert j.state == QueuedJob.State.SUCCEEDED
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_sync_job_force_cancelled_when_drain_times_out(
    chancy: Chancy, sync_executor: str
):
    """
    stop() can't sit around waiting on sync jobs it has no way to
    interrupt - it should return close to shutdown_timeout.
    """
    await chancy.declare(
        Queue("shutdown_sync_kill", executor=sync_executor, concurrency=1)
    )
    worker = Worker(chancy, shutdown_timeout=2, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            sync_slow_sleeper.job.with_queue("shutdown_sync_kill")
        )
        await _wait_for_running(chancy, ref, timeout=15)

        timeout_emitted = asyncio.Event()
        worker.hub.on(
            "worker.shutdown_timeout",
            lambda event: timeout_emitted.set(),
        )

        start = time.monotonic()
        cleanly = await worker.stop()
        elapsed = time.monotonic() - start

        assert cleanly is False
        # The job sleeps for 10s; stop() must return near the 2s timeout
        # instead of blocking until the job finishes.
        assert elapsed < 6
        assert timeout_emitted.is_set()

        j = await chancy.get_job(ref)
        assert j.state != QueuedJob.State.SUCCEEDED
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_sigterm_drains_in_flight_jobs(chancy: Chancy):
    """
    Test that SIGTERM triggers the same graceful drain as stop().
    """
    await chancy.declare(
        Queue("shutdown_signal", executor=Chancy.Executor.Async)
    )
    worker = Worker(chancy, shutdown_timeout=10, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            async_quick_sleeper.job.with_queue("shutdown_signal")
        )
        await _wait_for_running(chancy, ref)

        stopped = asyncio.Event()
        worker.hub.on("worker.stopped", lambda event: stopped.set())

        await worker.on_signal(signal.SIGTERM)

        assert stopped.is_set()
        j = await chancy.get_job(ref)
        assert j.state == QueuedJob.State.SUCCEEDED
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_cancelled_pending_future_records_update(chancy: Chancy):
    """
    A job whose pool future is cancelled before it ever started running
    should still record a final state instead of silently disappearing.
    """
    await chancy.declare(
        Queue(
            "shutdown_pending",
            executor=Chancy.Executor.Threaded,
            concurrency=1,
        )
    )
    worker = Worker(chancy, shutdown_timeout=1, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        running_ref = await chancy.push(
            sync_slow_sleeper.job.with_queue("shutdown_pending")
        )
        await _wait_for_running(chancy, running_ref, timeout=15)

        # The single pool slot is taken; push a second job straight into
        # the executor so its future stays pending.
        executor = worker._executors["shutdown_pending"]
        pending_ref = await chancy.push(
            sync_quick_sleeper.job.with_queue("shutdown_pending")
        )
        pending_job = await chancy.get_job(pending_ref)
        await executor.push(pending_job)

        cleanly = await worker.stop()
        assert cleanly is False

        # The pending job's cancellation was recorded and flushed.
        j = await chancy.get_job(pending_ref)
        assert j.state in (
            QueuedJob.State.RETRYING,
            QueuedJob.State.FAILED,
        )
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_jobs_fetched_during_shutdown_are_drained(
    chancy: Chancy, monkeypatch
):
    """
    A job whose fetch was already in flight when stop() was called is
    still executed and recorded.
    """
    await chancy.declare(
        Queue(
            "shutdown_latefetch",
            executor=Chancy.Executor.Async,
            polling_interval=1,
        )
    )
    worker = Worker(chancy, shutdown_timeout=10, register_signal_handlers=False)

    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()
    real_fetch = worker.fetch_jobs

    async def delayed_fetch(queue, conn, *, up_to=1):
        jobs = await real_fetch(queue, conn, up_to=up_to)
        if jobs:
            fetch_started.set()
            await release_fetch.wait()
        return jobs

    monkeypatch.setattr(worker, "fetch_jobs", delayed_fetch)

    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            async_quick_sleeper.job.with_queue("shutdown_latefetch")
        )
        await asyncio.wait_for(fetch_started.wait(), timeout=10)

        # The row is already marked running but the job hasn't reached
        # the executor yet. Start the shutdown, then release the fetch.
        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0.2)
        release_fetch.set()

        cleanly = await stop_task
        assert cleanly is True

        j = await chancy.get_job(ref)
        assert j.state == QueuedJob.State.SUCCEEDED
    finally:
        release_fetch.set()
        await worker.stop()


@pytest.mark.asyncio
async def test_queue_removed_emitted_on_clean_shutdown(chancy: Chancy):
    """
    Test that worker.queue.removed is still emitted on a clean shutdown.
    """
    await chancy.declare(
        Queue("shutdown_removed", executor=Chancy.Executor.Async)
    )
    worker = Worker(chancy, shutdown_timeout=10, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.hub.wait_for("worker.queue.started", timeout=10)
        ref = await chancy.push(
            async_quick_sleeper.job.with_queue("shutdown_removed")
        )
        await _wait_for_running(chancy, ref)

        removed_emitted = asyncio.Event()
        worker.hub.on(
            "worker.queue.removed",
            lambda event: removed_emitted.set(),
        )

        assert await worker.stop() is True
        assert removed_emitted.is_set()
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_flush_outgoing_requeues_on_cancellation(
    chancy: Chancy, monkeypatch
):
    """
    Updates aren't lost when the flush is cancelled mid-write.
    """
    worker = Worker(chancy, register_signal_handlers=False)
    ref = await chancy.push(async_quick_sleeper.job)
    update = await chancy.get_job(ref)
    worker.outgoing.put_nowait(update)

    entered = asyncio.Event()

    class BlockingPool:
        @contextlib.asynccontextmanager
        async def connection(self):
            entered.set()
            await asyncio.sleep(3600)
            yield

    with monkeypatch.context() as mp:
        mp.setattr(chancy, "pool", BlockingPool())

        flush_task = asyncio.create_task(worker._flush_outgoing())
        await asyncio.wait_for(entered.wait(), timeout=5)
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task

    assert worker.outgoing.qsize() == 1
    assert worker.outgoing.get_nowait().id == update.id


@pytest.mark.asyncio
async def test_stop_completes_even_if_caller_cancelled(chancy: Chancy):
    """
    Cancelling a task that's awaiting stop() doesn't abort the shutdown
    itself.
    """
    worker = Worker(chancy, shutdown_timeout=5, register_signal_handlers=False)
    await worker.start()
    try:
        caller = asyncio.create_task(worker.stop())
        # Let stop() create the underlying shutdown task before cancelling.
        while worker._stop_task is None:
            await asyncio.sleep(0)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller

        result = await asyncio.wait_for(
            asyncio.shield(worker._stop_task), timeout=10
        )
        assert result is True
        assert await worker.stop() is True
    finally:
        await worker.stop()
