"""Switch the delivery block in _asyncio.py between four variants."""

from __future__ import annotations

import sys
from pathlib import Path

VARIANTS = {
    "fix": """                    try:
                        self.loop.call_soon_threadsafe(
                            self._report_result, future, result, exception
                        )
                    except RuntimeError:
                        if not self.loop.is_closed():
                            raise
""",
    "master": """                    if not self.loop.is_closed():
                        self.loop.call_soon_threadsafe(
                            self._report_result, future, result, exception
                        )
""",
    "unguarded": """                    self.loop.call_soon_threadsafe(
                        self._report_result, future, result, exception
                    )
""",
    "suppress-all": """                    try:
                        self.loop.call_soon_threadsafe(
                            self._report_result, future, result, exception
                        )
                    except RuntimeError:
                        pass
""",
}

path = Path(sys.argv[1])
target = sys.argv[2]
source = path.read_text()
for name, block in VARIANTS.items():
    if block in source:
        if name == target:
            print(f"already {target}")
            break

        path.write_text(source.replace(block, VARIANTS[target]))
        print(f"{name} -> {target}")
        break
else:
    raise SystemExit("no known variant found in source")
