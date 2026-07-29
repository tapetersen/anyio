# Worker thread result delivery race — probes

Throwaway tooling behind the tests in `tests/test_to_thread.py`
(`DeliveryTrackingEventLoop` and friends), for PR #1245. This whole directory is
ignored by git via its own `.gitignore`.

Run them from the repo root with the editable install, e.g.:

    .venv/bin/python scratchpad/race_probe4.py

## mutate.py

Flips the result delivery block in `WorkerThread.run()` between four variants:

    .venv/bin/python scratchpad/mutate.py src/anyio/_backends/_asyncio.py master

| variant        | delivery block                                        |
|----------------|-------------------------------------------------------|
| `fix`          | `try: call_soon_threadsafe() / except RuntimeError: re-raise if not closed` |
| `master`       | `if not self.loop.is_closed(): call_soon_threadsafe()` (pre-fix, racy) |
| `unguarded`    | bare `call_soon_threadsafe()`                         |
| `suppress-all` | `except RuntimeError: pass`                           |

Mutant matrix measured against the three tests (each cell an actual run):

| variant        | closed-loop test | unrelated error test | stress test        |
|----------------|------------------|----------------------|--------------------|
| `fix`          | pass             | pass                 | pass (5/5)         |
| `master`       | fail `0 == 1`    | pass                 | fail, real crash (8/8) |
| `unguarded`    | fail, crash      | pass                 | fail, crash        |
| `suppress-all` | pass             | fail, no exception   | pass               |

Remember to flip back to `fix` when done (`git diff src/` should be empty).

## race_probe*.py — how the stress test was arrived at

All probes need the `master` variant to produce hits. Config comes from env vars
(`ITERATIONS`, `THREADS`, `SWITCH`, `STEP`, `WARMUP`, `SWEEP`, `SWEEP_STEP`).

| probe | release mechanism                          | result on 8 cores        |
|-------|--------------------------------------------|--------------------------|
| 1     | `Event.set()` then `loop.close()`          | **0 hits** in 50 iterations |
| 2     | `Barrier` shared with the main thread      | **0 hits** in 50 iterations |
| 3     | spin on a plain flag set by the main thread| 2 hits / 6400 attempts, 36 s |
| 4     | sleep until a shared deadline, then spin on the clock | ~0.6–1.0 hits/s, ~300 iterations/s |
| 5     | v4 plus a per-iteration base offset sweep  | no better than v4 (hits land uniformly across offsets) |

Findings that shaped the test:

- A worker waiting in a futex (`Event`/`Barrier`) always resumes *after* `_closed`
  has flipped, so it never reaches the window — hence the clock spin.
- `sys.setswitchinterval()` is essential: with the default 5 ms, every config gave
  **0 hits**. 1e-6 and 1e-9 behave about the same.
- Sleeping during the cancel dance and only spinning at the end is what took
  iterations from 5.5/s to ~300/s.
- Per-attempt hit probability is ~1.5–4e-4 regardless of tuning; volume is what
  matters. Staggering arrival times does not help — jitter dominates.
- Diagnostic on `unguarded`: 5 of 20 deliveries landed after close, 15 before, which
  is how we knew the arrival times straddle the flip at all.

## Note on linting

The project linters are not meant for this directory (the probes use `print()`, which
trips ruff `T201`). Commit changes in here with `git commit --no-verify`.
