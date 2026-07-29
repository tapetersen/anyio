"""Probe v3: both sides spin-wait so neither has to be woken from a futex."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from functools import partial

from anyio import CapacityLimiter, create_task_group, to_thread

ITERATIONS = int(os.environ.get("ITERATIONS", "50"))
THREADS = int(os.environ.get("THREADS", "4"))
SWITCH = float(os.environ.get("SWITCH", "1e-6"))

errors: list[BaseException | None] = []
threading.excepthook = lambda args: errors.append(args.exc_value)
old_interval = sys.getswitchinterval()
sys.setswitchinterval(SWITCH)

first_hit = None
hits_per_iteration = []
try:
    for i in range(ITERATIONS):
        loop = asyncio.new_event_loop()
        state = {"spinning": 0, "go": False}
        lock = threading.Lock()
        before = len(errors)

        def thread_worker() -> None:
            with lock:
                state["spinning"] += 1

            while not state["go"]:
                pass

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

                while state["spinning"] < THREADS:
                    await asyncio.sleep(0)

                tg.cancel_scope.cancel()

        loop.run_until_complete(main())
        # every worker is now spinning on state["go"], so none of them has to be woken
        # from the OS before it can race us to the loop
        state["go"] = True
        loop.close()
        for thread in threading.enumerate():
            if thread.name == "AnyIO worker thread":
                thread.join(5)
                assert not thread.is_alive(), "worker thread hung"

        hits_per_iteration.append(len(errors) - before)
        if errors and first_hit is None:
            first_hit = i

finally:
    sys.setswitchinterval(old_interval)

print(f"iterations={ITERATIONS} threads={THREADS} switch={SWITCH}")
print(f"errors={len(errors)} first_hit_iteration={first_hit}")
print(f"iterations_with_a_hit={sum(1 for n in hits_per_iteration if n)}")
for exc in errors[:3]:
    print(f"  {type(exc).__name__}: {exc}")
