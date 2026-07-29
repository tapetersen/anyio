"""Probe how often the loop-close delivery race can be hit by brute force."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from functools import partial

from anyio import create_task_group, to_thread

ITERATIONS = int(os.environ.get("ITERATIONS", "50"))
THREADS = int(os.environ.get("THREADS", "20"))
SWITCH = float(os.environ.get("SWITCH", "1e-6"))

errors: list[BaseException | None] = []
threading.excepthook = lambda args: errors.append(args.exc_value)
old_interval = sys.getswitchinterval()
sys.setswitchinterval(SWITCH)

first_hit = None
try:
    for i in range(ITERATIONS):
        loop = asyncio.new_event_loop()
        start_barrier = threading.Barrier(THREADS + 1)
        finish_thread = threading.Event()

        def thread_worker() -> None:
            start_barrier.wait(5)
            finish_thread.wait(5)

        async def main() -> None:
            async with create_task_group() as tg:
                for _ in range(THREADS):
                    tg.start_soon(
                        partial(to_thread.run_sync, abandon_on_cancel=True),
                        thread_worker,
                    )

                await to_thread.run_sync(start_barrier.wait, 5)
                tg.cancel_scope.cancel()

        loop.run_until_complete(main())
        finish_thread.set()
        loop.close()
        for thread in threading.enumerate():
            if thread.name == "AnyIO worker thread":
                thread.join(5)
                assert not thread.is_alive(), "worker thread hung"

        if errors and first_hit is None:
            first_hit = i
finally:
    sys.setswitchinterval(old_interval)

print(f"iterations={ITERATIONS} threads={THREADS} switch={SWITCH}")
print(f"errors={len(errors)} first_hit_iteration={first_hit}")
for exc in errors[:3]:
    print(f"  {type(exc).__name__}: {exc}")
