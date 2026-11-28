"""Structured JSONL logging, diagnostic counters, and runtime environment capture."""

from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import sys
from typing import Any, Dict, List, Optional, Union

from dqrp.contracts import utc_now_iso


def collect_runtime_environment() -> Dict[str, Any]:
    """Capture deterministic environment metadata for reproducibility reports."""
    optional_packages: Dict[str, Optional[str]] = {}
    for pkg in ("pyarrow", "dask", "distributed", "ray", "psycopg", "pytest", "numpy"):
        try:
            mod = __import__(pkg)
            optional_packages[pkg] = str(getattr(mod, "__version__", "installed"))
        except Exception:
            optional_packages[pkg] = None

    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count() or 1,
        "executable": sys.executable,
        "optional_packages": optional_packages,
    }


class StructuredLogger:
    """Append-only structured JSONL event logger for runs and diagnostics."""

    def __init__(self, log_path: Optional[Union[str, Path]] = None) -> None:
        self.log_path = Path(log_path) if log_path is not None else None
        self.events: List[Dict[str, Any]] = []
        if self.log_path is not None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event_type: str, **fields: Any) -> Dict[str, Any]:
        record: Dict[str, Any] = {
            "timestamp": utc_now_iso(),
            "event": event_type,
        }
        record.update(fields)
        self.events.append(record)
        if self.log_path is not None:
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, sort_keys=True) + "\n")
        return record
