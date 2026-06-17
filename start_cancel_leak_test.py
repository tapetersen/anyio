"""
Reproducer: on the asyncio backend a CancelledError leaks out of
TaskGroup.start() when the *target* group's cancel scope is cancelled before the
spawned task calls task_status.started(), while the *caller* of start() lives in
a different, non-cancelled scope.

The start()-ed task is spawned into the target group's cancel scope from birth,
so cancelling that group cancels the child before started(); the resulting
CancelledError is then delivered into the caller via the awaited future even
though the caller's own scope was never cancelled -- tearing down a task that was
not being cancelled.

This does NOT happen on trio (start() simply stays blocked there); a watchdog
forces a shutdown so the trio run terminates instead of hanging. `caller`
distinguishes a real leak from the forced shutdown by checking whether the master
scope was cancelled.

Run:  python start_cancel_leak_test.py        -> LEAK
      python start_cancel_leak_test.py trio    -> OK (no leak)
"""

from __future__ import annotations

import sys

import anyio
from anyio.abc import CancelScope, TaskGroup, TaskStatus


async def main() -> None:
    leaked: BaseException | None = None
    forced = False
    caller_done = anyio.Event()

    async def slow_starter(*, task_status: TaskStatus[None]) -> None:
        # Cancelled before it ever gets to call started()
        await anyio.sleep_forever()
        task_status.started()

    async def caller(inner_tg: TaskGroup, master: CancelScope) -> None:
        nonlocal leaked
        try:
            await inner_tg.start(slow_starter)
        except anyio.get_cancelled_exc_class() as exc:
            if master.cancel_called:
                raise  # legitimate (forced) shutdown -- honour it
            leaked = exc  # spurious cancellation -- swallow it and flag the leak
        finally:
            caller_done.set()

    async def watchdog(master: CancelScope) -> None:
        nonlocal forced
        await anyio.sleep(1.0)
        if leaked is None and not caller_done.is_set():
            forced = True
            master.cancel()  # unblock the trio case

    async def scenario(master: CancelScope) -> None:
        async with anyio.create_task_group() as inner:
            master_tg.start_soon(caller, inner, master)
            await anyio.sleep(0.1)  # let `caller` park inside start()
            inner.cancel_scope.cancel()  # cancel the TARGET group, not the caller

        # Reached promptly on asyncio; on trio this is blocked until the watchdog
        # fires, because inner's exit waits for the still-running pre-started body.
        await anyio.sleep(0.2)  # window for a prompt leak to surface

    async with anyio.create_task_group() as master_tg:
        master = master_tg.cancel_scope
        master_tg.start_soon(watchdog, master)
        master_tg.start_soon(scenario, master)

    if leaked is not None:
        print(f"LEAK: start() raised {leaked!r} into a non-cancelled caller")  # noqa: T201
    elif forced:
        print(  # noqa: T201
            "OK: no leak; start() stayed blocked until forced shutdown -- "
            "target-group cancel never reached the pre-started() body"
        )
    else:
        print("UNEXPECTED: start() returned normally without a leak")  # noqa: T201


if __name__ == "__main__":
    backend = sys.argv[1] if len(sys.argv) > 1 else "asyncio"
    print(f"backend={backend}")  # noqa: T201
    anyio.run(main, backend=backend)
