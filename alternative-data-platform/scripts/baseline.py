"""Measure end-to-end fixture replay with a fresh database for each observation."""
import json
import platform
import sqlite3
import statistics
import tempfile
import time
from pathlib import Path

from demo import replay


def main():
    samples = []
    for _ in range(7):
        with tempfile.TemporaryDirectory() as tmp:
            start = time.perf_counter()
            result = replay(Path(tmp))
            samples.append(round(1000 * (time.perf_counter() - start), 3))
    print(json.dumps(dict(python=platform.python_version(), sqlite=sqlite3.sqlite_version,
                          system=platform.system(), release=platform.release(), machine=platform.machine(),
                          scope="fixture copy, registry setup, four pages, two idle polls, exports and health",
                          sources=len(result["health"]), pages=sum(x["cursor"] for x in result["health"]),
                          raw_input_rows=6, retained_revisions=len(result["final_history"]["records"]),
                          elapsed_ms=samples, median_ms=statistics.median(samples)), indent=2))


if __name__ == "__main__":
    main()
