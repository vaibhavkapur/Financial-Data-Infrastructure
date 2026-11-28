#!/usr/bin/env python3
"""Run the full unittest suite and emit a machine-readable report in reports/test_report.json."""

from __future__ import annotations

import json
import platform
import sys
import time
import unittest
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


class RecordingTestResult(unittest.TextTestResult):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.records: List[Dict[str, Any]] = []
        self._start_times: Dict[str, float] = {}

    def startTest(self, test: unittest.TestCase) -> None:
        self._start_times[test.id()] = time.perf_counter()
        super().startTest(test)

    def _record(self, test: unittest.TestCase, status: str, detail: str = "") -> None:
        elapsed_ms = (time.perf_counter() - self._start_times.get(test.id(), time.perf_counter())) * 1000.0
        self.records.append(
            {
                "test_id": test.id(),
                "status": status,
                "duration_ms": round(elapsed_ms, 3),
                "detail": detail,
            }
        )

    def addSuccess(self, test: unittest.TestCase) -> None:
        super().addSuccess(test)
        self._record(test, "PASS")

    def addFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self._record(test, "FAIL", self._exc_info_to_string(err, test))

    def addError(self, test: unittest.TestCase, err: Any) -> None:
        super().addError(test, err)
        self._record(test, "ERROR", self._exc_info_to_string(err, test))

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self._record(test, "SKIP", reason)


def main() -> int:
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(PROJECT_ROOT / "tests"), pattern="test_*.py")

    t0 = time.perf_counter()
    runner = unittest.TextTestRunner(
        verbosity=2,
        resultclass=RecordingTestResult,  # type: ignore[arg-type]
    )
    result: RecordingTestResult = runner.run(suite)  # type: ignore[assignment]
    total_s = time.perf_counter() - t0

    report = {
        "report_type": "test_execution_report",
        "runtime": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        },
        "summary": {
            "tests_run": result.testsRun,
            "passed": sum(1 for r in result.records if r["status"] == "PASS"),
            "failures": len(result.failures),
            "errors": len(result.errors),
            "skipped": len(result.skipped),
            "was_successful": result.wasSuccessful(),
            "total_duration_seconds": round(total_s, 4),
        },
        "cases": sorted(result.records, key=lambda r: r["test_id"]),
    }

    out_path = PROJECT_ROOT / "reports/test_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
