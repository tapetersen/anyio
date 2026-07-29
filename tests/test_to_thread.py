from __future__ import annotations

import asyncio
import gc
import sys
import threading
import time
import weakref
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import Context, ContextVar
from functools import partial
from typing import Any, NoReturn

import pytest

import anyio.to_thread
from anyio import (
    CapacityLimiter,
    Event,
    create_task_group,
    from_thread,
    sleep,
    to_thread,
    wait_all_tasks_blocked,
)
from anyio._core._eventloop import current_async_library
from anyio.from_thread import BlockingPortalProvider
from anyio.lowlevel import checkpoint

from .conftest import asyncio_params, no_other_refs

if sys.version_info >= (3, 11):
    from typing import TypeVarTuple, Unpack
else:
    from typing_extensions import TypeVarTuple, Unpack

PosArgsT = TypeVarTuple("PosArgsT")


async def test_run_in_thread_cancelled() -> None:
    state = 0

    def thread_worker() -> None:
        nonlocal state
        state = 2

    async def worker() -> None:
        nonlocal state
        state = 1
        await to_thread.run_sync(thread_worker)
        state = 3

    async with create_task_group() as tg:
        tg.start_soon(worker)
        tg.cancel_scope.cancel()

    assert state == 1


async def test_run_in_thread_exception() -> None:
    def thread_worker() -> NoReturn:
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        await to_thread.run_sync(thread_worker)

    exc.match("^foo$")


async def test_run_in_custom_limiter() -> None:
    max_active_threads = 0

    def thread_worker() -> None:
        nonlocal max_active_threads
        active_threads.add(threading.current_thread())
        max_active_threads = max(max_active_threads, len(active_threads))
        event.wait(1)
        active_threads.remove(threading.current_thread())

    async def task_worker() -> None:
        await to_thread.run_sync(thread_worker, limiter=limiter)

    event = threading.Event()
    limiter = CapacityLimiter(3)
    active_threads: set[threading.Thread] = set()
    async with create_task_group() as tg:
        for _ in range(4):
            tg.start_soon(task_worker)

        await sleep(0.1)
        assert len(active_threads) == 3
        assert limiter.borrowed_tokens == 3
        event.set()

    assert len(active_threads) == 0
    assert max_active_threads == 3


@pytest.mark.parametrize(
    "abandon_on_cancel, expected_last_active",
    [
        pytest.param(False, "task", id="noabandon"),
        pytest.param(True, "thread", id="abandon"),
    ],
)
async def test_cancel_worker_thread(
    abandon_on_cancel: bool, expected_last_active: str
) -> None:
    """
    Test that when a task running a worker thread is cancelled, the cancellation is not
    acted on until the thread finishes.

    """
    last_active: str | None = None

    def thread_worker() -> None:
        nonlocal last_active
        from_thread.run_sync(sleep_event.set)
        time.sleep(0.2)
        last_active = "thread"
        from_thread.run_sync(finish_event.set)

    async def task_worker() -> None:
        nonlocal last_active
        try:
            await to_thread.run_sync(thread_worker, abandon_on_cancel=abandon_on_cancel)
        finally:
            last_active = "task"

    sleep_event = Event()
    finish_event = Event()
    async with create_task_group() as tg:
        tg.start_soon(task_worker)
        await sleep_event.wait()
        tg.cancel_scope.cancel()

    await finish_event.wait()
    assert last_active == expected_last_active


async def test_cancel_wait_on_thread() -> None:
    event = threading.Event()
    future: Future[bool] = Future()

    def wait_event() -> None:
        future.set_result(event.wait(5))

    async with create_task_group() as tg:
        tg.start_soon(partial(to_thread.run_sync, abandon_on_cancel=True), wait_event)
        await wait_all_tasks_blocked()
        tg.cancel_scope.cancel()

    await to_thread.run_sync(event.set)
    assert future.result(5)


async def test_deprecated_cancellable_param() -> None:
    with pytest.warns(DeprecationWarning, match="The `cancellable=`"):
        await to_thread.run_sync(bool, cancellable=True)


async def test_contextvar_propagation() -> None:
    var = ContextVar("var", default=1)
    var.set(6)
    assert await to_thread.run_sync(var.get) == 6


async def test_asynclib_detection() -> None:
    assert await to_thread.run_sync(current_async_library) is None


@pytest.mark.parametrize("anyio_backend", asyncio_params)
async def test_asyncio_cancel_native_task() -> None:
    task: asyncio.Task[None] | None = None

    async def run_in_thread() -> None:
        nonlocal task
        task = asyncio.current_task()
        await to_thread.run_sync(time.sleep, 0.2, abandon_on_cancel=True)

    async with create_task_group() as tg:
        tg.start_soon(run_in_thread)
        await wait_all_tasks_blocked()
        assert task is not None
        task.cancel()


def test_asyncio_no_root_task(asyncio_event_loop: asyncio.AbstractEventLoop) -> None:
    """
    Regression test for #264.

    Ensures that to_thread.run_sync() does not raise an error when there is no root
    task, but instead tries to find the top most parent task by traversing the cancel
    scope tree, or failing that, uses the current task to set up a shutdown callback.

    """

    async def run_in_thread() -> None:
        try:
            await to_thread.run_sync(time.sleep, 0)
        finally:
            asyncio_event_loop.call_soon(asyncio_event_loop.stop)

    task = asyncio_event_loop.create_task(run_in_thread())
    asyncio_event_loop.run_forever()
    task.result()

    # Wait for worker threads to exit
    for t in threading.enumerate():
        if t.name == "AnyIO worker thread":
            t.join(2)
            assert not t.is_alive()


def test_asyncio_future_callback_partial(
    asyncio_event_loop: asyncio.AbstractEventLoop,
) -> None:
    """
    Regression test for #272.

    Ensures that futures with partial callbacks are handled correctly when the root task
    cannot be determined.
    """

    def func(future: object) -> None:
        pass

    async def sleep_sync() -> None:
        return await to_thread.run_sync(time.sleep, 0)

    task = asyncio_event_loop.create_task(sleep_sync())
    task.add_done_callback(partial(func))
    asyncio_event_loop.run_until_complete(task)


def test_asyncio_run_sync_no_asyncio_run(
    asyncio_event_loop: asyncio.AbstractEventLoop,
) -> None:
    """Test that the thread pool shutdown callback does not raise an exception."""

    def exception_handler(loop: object, context: Any = None) -> None:
        exceptions.append(context["exception"])

    exceptions: list[BaseException] = []
    asyncio_event_loop.set_exception_handler(exception_handler)
    asyncio_event_loop.run_until_complete(to_thread.run_sync(time.sleep, 0))
    assert not exceptions


def test_asyncio_run_sync_multiple(
    asyncio_event_loop: asyncio.AbstractEventLoop,
) -> None:
    """Regression test for #304."""
    asyncio_event_loop.call_later(0.5, asyncio_event_loop.stop)
    for _ in range(3):
        asyncio_event_loop.run_until_complete(to_thread.run_sync(time.sleep, 0))

    for t in threading.enumerate():
        if t.name == "AnyIO worker thread":
            t.join(2)
            assert not t.is_alive()


def test_asyncio_no_recycle_stopping_worker(
    asyncio_event_loop: asyncio.AbstractEventLoop,
) -> None:
    """Regression test for #323."""

    async def taskfunc1() -> None:
        await anyio.to_thread.run_sync(time.sleep, 0)
        event1.set()
        await event2.wait()

    async def taskfunc2() -> None:
        await event1.wait()
        asyncio_event_loop.call_soon(event2.set)
        await anyio.to_thread.run_sync(time.sleep, 0)
        # At this point, the worker would be stopped but still in the idle workers pool,
        # so the following would hang prior to the fix
        await anyio.to_thread.run_sync(time.sleep, 0)

    event1 = asyncio.Event()
    event2 = asyncio.Event()
    task1 = asyncio_event_loop.create_task(taskfunc1())
    task2 = asyncio_event_loop.create_task(taskfunc2())
    asyncio_event_loop.run_until_complete(asyncio.gather(task1, task2))


async def test_stopiteration() -> None:
    """
    Test that raising StopIteration in a worker thread raises a RuntimeError on the
    caller.

    """

    def raise_stopiteration() -> NoReturn:
        raise StopIteration

    with pytest.raises(RuntimeError, match="coroutine raised StopIteration"):
        await to_thread.run_sync(raise_stopiteration)


class TestBlockingPortalProvider:
    @pytest.fixture
    def provider(
        self, anyio_backend_name: str, anyio_backend_options: dict[str, Any]
    ) -> BlockingPortalProvider:
        return BlockingPortalProvider(
            backend=anyio_backend_name, backend_options=anyio_backend_options
        )

    def test_single_thread(
        self, provider: BlockingPortalProvider, anyio_backend_name: str
    ) -> None:
        threads: set[threading.Thread] = set()

        async def check_thread() -> None:
            assert current_async_library() == anyio_backend_name
            threads.add(threading.current_thread())

        active_threads_before = threading.active_count()
        for _ in range(3):
            with provider as portal:
                portal.call(check_thread)

        assert len(threads) == 3
        assert threading.active_count() == active_threads_before

    def test_single_thread_overlapping(
        self, provider: BlockingPortalProvider, anyio_backend_name: str
    ) -> None:
        threads: set[threading.Thread] = set()

        async def check_thread() -> None:
            assert current_async_library() == anyio_backend_name
            threads.add(threading.current_thread())

        with provider as portal1:
            with provider as portal2:
                assert portal1 is portal2
                portal2.call(check_thread)

            portal1.call(check_thread)

        assert len(threads) == 1

    def test_multiple_threads(
        self, provider: BlockingPortalProvider, anyio_backend_name: str
    ) -> None:
        threads: set[threading.Thread] = set()
        event = Event()

        async def check_thread() -> None:
            assert current_async_library() == anyio_backend_name
            await event.wait()
            threads.add(threading.current_thread())

        def dummy() -> None:
            with provider as portal:
                portal.call(check_thread)

        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in range(3):
                pool.submit(dummy)

            with provider as portal:
                portal.call(wait_all_tasks_blocked)
                portal.call(event.set)

        assert len(threads) == 1


skipif_pypy_mark = pytest.mark.skipif(
    sys.implementation.name == "pypy",
    reason=(
        "gc.get_referrers is broken on PyPy (see "
        "https://github.com/pypy/pypy/issues/5075)"
    ),
)


@skipif_pypy_mark
async def test_run_sync_worker_cyclic_references() -> None:
    class Foo:
        pass

    def foo(_: Foo) -> None:
        pass

    cvar = ContextVar[Foo]("cvar")
    contextval = Foo()
    arg = Foo()
    cvar.set(contextval)
    await to_thread.run_sync(foo, arg)
    cvar.set(Foo())
    gc.collect()
    await checkpoint()

    assert gc.get_referrers(contextval) == no_other_refs()
    assert gc.get_referrers(foo) == no_other_refs()
    assert gc.get_referrers(arg) == no_other_refs()


@skipif_pypy_mark
def test_asyncio_run_does_not_leak_event_loop() -> None:
    """
    Regression test for #1203.

    Ensure we don't leak the root task and event loop in when caching it in a RunVar.
    """

    def thread_worker() -> None:
        pass

    async def main() -> weakref.ref[object]:
        # Exercising to_thread.run_sync() triggers find_root_task(), which caches
        # the root task (and thus the loop) in the run-vars mapping.
        await to_thread.run_sync(thread_worker)
        return weakref.ref(asyncio.get_running_loop())

    loop_ref = anyio.run(main)

    gc.collect()
    assert loop_ref() is None


class DeliveryTrackingEventLoop(asyncio.SelectorEventLoop):
    """
    Event loop that keeps track of how worker threads deliver their results.

    As ``call_soon_threadsafe()`` is called from the worker thread itself, this records
    what the worker thread did without having to patch anything.

    """

    def __init__(self) -> None:
        super().__init__()
        self.deliveries_after_close = 0
        self.fail_deliveries = False

    def call_soon_threadsafe(
        self,
        callback: Callable[[Unpack[PosArgsT]], object],
        *args: Unpack[PosArgsT],
        context: Context | None = None,
    ) -> asyncio.Handle:
        if self.is_closed():
            self.deliveries_after_close += 1
        elif self.fail_deliveries:
            raise RuntimeError("Unrelated error")

        return super().call_soon_threadsafe(callback, *args, context=context)


@pytest.fixture
def worker_thread_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> list[BaseException | None]:
    """
    Collect the exceptions raised in worker threads.

    Without this, exceptions in worker threads are only reported indirectly, as
    unhandled thread exception warnings from pytest.

    """
    exceptions: list[BaseException | None] = []
    monkeypatch.setattr(
        threading, "excepthook", lambda args: exceptions.append(args.exc_value)
    )
    return exceptions


def abandon_worker_threads(
    loop: DeliveryTrackingEventLoop,
    thread_count: int,
    thread_worker: Callable[[], object],
) -> Callable[[], None]:
    """
    Run ``thread_count`` worker threads on ``loop``, and abandon them by cancelling
    their tasks while they are still inside ``thread_worker``.

    Returns a function that waits for the abandoned threads to exit.

    """
    started = threading.Barrier(thread_count + 1)

    def run_thread_worker() -> None:
        started.wait(5)
        thread_worker()

    async def main() -> None:
        limiter = CapacityLimiter(thread_count + 1)
        async with create_task_group() as tg:
            for _ in range(thread_count):
                tg.start_soon(
                    partial(
                        to_thread.run_sync, abandon_on_cancel=True, limiter=limiter
                    ),
                    run_thread_worker,
                )

            # Wait until every worker thread is actually running its target function, as
            # a task that is cancelled before that never reports a result at all
            await to_thread.run_sync(started.wait, 5, limiter=limiter)
            tg.cancel_scope.cancel()

    loop.run_until_complete(main())

    def join_threads() -> None:
        for thread in threading.enumerate():
            if thread.name == "AnyIO worker thread":
                thread.join(5)
                assert not thread.is_alive()

    return join_threads


def test_asyncio_report_result_after_loop_closed(
    worker_thread_exceptions: list[BaseException | None],
) -> None:
    """
    Test that a worker thread, abandoned on cancellation, does not raise a RuntimeError
    when it delivers its result after the event loop has been closed.

    """
    loop = DeliveryTrackingEventLoop()
    release = threading.Event()
    join_threads = abandon_worker_threads(loop, 1, partial(release.wait, 5))
    loop.close()

    # Let the abandoned worker report its result to the now closed event loop
    release.set()
    join_threads()

    assert loop.deliveries_after_close == 1
    assert not worker_thread_exceptions


def test_asyncio_report_result_unrelated_runtime_error(
    worker_thread_exceptions: list[BaseException | None],
) -> None:
    """
    Test that a RuntimeError raised while reporting the result of a worker thread is not
    suppressed when the event loop is still open.

    """
    loop = DeliveryTrackingEventLoop()
    release = threading.Event()
    join_threads = abandon_worker_threads(loop, 1, partial(release.wait, 5))
    loop.fail_deliveries = True

    # Let the abandoned worker report its result to the still open event loop
    release.set()
    join_threads()
    loop.close()

    assert len(worker_thread_exceptions) == 1
    assert isinstance(worker_thread_exceptions[0], RuntimeError)
    assert str(worker_thread_exceptions[0]) == "Unrelated error"


def close_loop_while_reporting_results(thread_count: int) -> int:
    """
    Close an event loop at the very moment ``thread_count`` abandoned worker threads
    return from their target function.

    Returns the number of results that were delivered after the loop was closed.

    """
    loop = DeliveryTrackingEventLoop()
    release = threading.Event()
    close_at = 0.0

    def thread_worker() -> None:
        release.wait(5)
        # Spin on the clock rather than waiting for another event: a worker thread that
        # has to be woken up by the OS always arrives long after the loop was closed
        while time.perf_counter() < close_at:
            pass

    join_threads = abandon_worker_threads(loop, thread_count, thread_worker)

    # Give the worker threads a moment to start spinning, and close the loop just as
    # they return from their target function
    close_at = time.perf_counter() + 0.001
    release.set()
    while time.perf_counter() < close_at:
        pass

    loop.close()
    join_threads()
    return loop.deliveries_after_close


def test_asyncio_report_result_loop_close_race(
    worker_thread_exceptions: list[BaseException | None],
) -> None:
    """
    Stress test for the race between the event loop being closed and an abandoned worker
    thread delivering its result.

    Unlike the two tests above, this one aims at the race itself rather than at its
    outcome, but it can only hit it by chance: on an 8 core CPU it reproduces the
    pre-fix RuntimeError roughly once per 1000 abandoned worker threads, so this is a
    best effort test that gets weaker on slower machines.

    """
    deliveries_after_close = 0
    deadline = time.perf_counter() + 5
    old_switch_interval = sys.getswitchinterval()
    # The race window is only a couple of bytecodes wide, so the interpreter has to be
    # told to switch threads far more eagerly than it does by default (with the default
    # switch interval, the window is never hit at all)
    sys.setswitchinterval(1e-6)
    try:
        while time.perf_counter() < deadline and not worker_thread_exceptions:
            deliveries_after_close += close_loop_while_reporting_results(16)
    finally:
        sys.setswitchinterval(old_switch_interval)

    assert not worker_thread_exceptions
    # Fail instead of passing vacuously if the threads never got to race the loop
    assert deliveries_after_close
