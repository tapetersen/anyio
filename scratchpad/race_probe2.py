"""Probe v2: release the workers from the main thread right before loop.close()."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from functools import partial

from anyio import CapacityLimiter, create_task_group, to_thread

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
        # THREADS workers + the main thread: the main thread arrives last, so it is
        # released immediately and races into loop.close() while the workers wake up
        release = threading.Barrier(THREADS + 1)
        started = threading.Barrier(THREADS + 1)

        def thread_worker() -> None:
            started.wait(5)
            release.wait(5)

        async def main() -> None:
            limiter = CapacityLimiter(THREADS + 1)
            async with create_task_group() as tg:
                for _ in range(THREADS):
                    tg.start_soon(
                        partial(
                            to_thread.run_sync, abandon_on_cancel=True, limiter=limiter
                        ),
                        thread_worker,
                    )

                await to_thread.run_sync(started.wait, 5, limiter=limiter)
                tg.cancel_scope.cancel()

        loop.run_until_complete(main())
        release.wait(5)
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
