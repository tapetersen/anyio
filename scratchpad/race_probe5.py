"""Probe v5: v4 plus a per-iteration base offset that sweeps the stagger ladder.

The `_closed` flip happens somewhere in the middle of loop.close(), and how far in
varies with GIL contention. Sweeping the base offset across iterations moves the
window of arrival times back and forth over the whole close() call.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from functools import partial

from anyio import CapacityLimiter, create_task_group, to_thread

ITERATIONS = int(os.environ.get("ITERATIONS", "1000"))
THREADS = int(os.environ.get("THREADS", "16"))
SWITCH = float(os.environ.get("SWITCH", "1e-6"))
STEP = float(os.environ.get("STEP", "5e-7"))
WARMUP = float(os.environ.get("WARMUP", "1e-3"))
SWEEP = int(os.environ.get("SWEEP", "16"))
SWEEP_STEP = float(os.environ.get("SWEEP_STEP", "2e-6"))

errors: list[BaseException | None] = []
threading.excepthook = lambda args: errors.append(args.exc_value)
old_interval = sys.getswitchinterval()
sys.setswitchinterval(SWITCH)

hits_by_offset: dict[int, int] = {}
first_hit = None
started = time.perf_counter()
try:
    for i in range(ITERATIONS):
        loop = asyncio.new_event_loop()
        state: dict[str, float] = {}
        release = threading.Event()
        base = (i % SWEEP) * SWEEP_STEP
        before = len(errors)

        def thread_worker(index: int) -> None:
            release.wait(5)
            target = state["deadline"] + base + index * STEP
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

        if len(errors) > before:
            hits_by_offset[i % SWEEP] = hits_by_offset.get(i % SWEEP, 0) + (
                len(errors) - before
            )
            if first_hit is None:
                first_hit = i

finally:
    sys.setswitchinterval(old_interval)

elapsed = time.perf_counter() - started
print(
    f"threads={THREADS} switch={SWITCH} step={STEP} sweep={SWEEP}x{SWEEP_STEP} "
    f"iterations={ITERATIONS}"
)
print(
    f"errors={len(errors)} first_hit_iteration={first_hit} elapsed={elapsed:.1f}s "
    f"iter/s={ITERATIONS / elapsed:.1f} hits_per_second={len(errors) / elapsed:.3f}"
)
print(f"hits_by_sweep_slot={dict(sorted(hits_by_offset.items()))}")
