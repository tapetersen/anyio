"""Probe v4: time-based release, each worker staggered by a fixed step.

Workers sleep (cheap) until the loop is ready to be closed, then spin on the clock
until their own deadline, so their arrival at the result delivery path is spread
across the loop.close() call instead of clustering on a futex wake-up.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from functools import partial

from anyio import CapacityLimiter, create_task_group, to_thread

ITERATIONS = int(os.environ.get("ITERATIONS", "50"))
THREADS = int(os.environ.get("THREADS", "8"))
SWITCH = float(os.environ.get("SWITCH", "1e-6"))
STEP = float(os.environ.get("STEP", "2e-6"))
WARMUP = float(os.environ.get("WARMUP", "2e-3"))

errors: list[BaseException | None] = []
threading.excepthook = lambda args: errors.append(args.exc_value)
old_interval = sys.getswitchinterval()
sys.setswitchinterval(SWITCH)

first_hit = None
started = time.perf_counter()
try:
    for i in range(ITERATIONS):
        loop = asyncio.new_event_loop()
        state: dict[str, float] = {}
        release = threading.Event()

        def thread_worker(index: int) -> None:
            release.wait(5)
            # spin on the clock: no OS wake-up on the critical path, and each worker
            # arrives at the delivery path a bit later than the previous one
            target = state["deadline"] + index * STEP
            while time.perf_counter() < target:
                pass

        async def main() -> None:
            limiter = CapacityLimiter(THREADS)
            async with create_task_group() as tg:
                for index in range(THREADS):
                    tg.start_soon(
                        partial(
                            to_thread.run_sync, abandon_on_cancel=True, limiter=limiter
                        ),
                        thread_worker,
                        index,
                    )

                while limiter.borrowed_tokens < THREADS:
                    await asyncio.sleep(0)

                tg.cancel_scope.cancel()

        loop.run_until_complete(main())
        state["deadline"] = time.perf_counter() + WARMUP
        release.set()
        while time.perf_counter() < state["deadline"]:
            pass

        loop.close()
        for thread in threading.enumerate():
            if thread.name == "AnyIO worker thread":
                thread.join(5)
                assert not thread.is_alive(), "worker thread hung"

        if errors and first_hit is None:
            first_hit = i

finally:
    sys.setswitchinterval(old_interval)

elapsed = time.perf_counter() - started
trials = ITERATIONS * THREADS
print(
    f"threads={THREADS} switch={SWITCH} step={STEP} warmup={WARMUP} "
    f"iterations={ITERATIONS}"
)
print(
    f"errors={len(errors)} first_hit_iteration={first_hit} "
    f"elapsed={elapsed:.1f}s iter/s={ITERATIONS / elapsed:.1f} "
    f"trials={trials} hits_per_second={len(errors) / elapsed:.3f}"
)
for exc in errors[:3]:
    print(f"  {type(exc).__name__}: {exc}")
