"""Structured health, failure, and decision metrics collector."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class MetricsCollector:
    """In-memory structured operational metrics for observability and reporting."""

    counters: Counter[str] = field(default_factory=Counter)
    events: list[dict[str, Any]] = field(default_factory=list)

    def increment(self, metric: str, value: int = 1, *, tags: dict[str, Any] | None = None) -> None:
        self.counters[metric] += value
        self.events.append(
            {
                "ts": datetime.now(UTC).isoformat(),
                "metric": metric,
                "value": value,
                "tags": tags or {},
            }
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "counters": dict(self.counters),
            "event_count": len(self.events),
            "recent_events": self.events[-20:],
        }
