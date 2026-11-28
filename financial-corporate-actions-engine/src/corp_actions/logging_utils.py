"""Structured JSON logging and diagnostic counters."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List


class JsonLogFormatter(logging.Formatter):
    """Emit one JSON object per log record for structured observability."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "structured", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, sort_keys=True)


def get_structured_logger(name: str = "corp_actions") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
    return logger


@dataclass
class DiagnosticCounters:
    """Mutable counters collected during engine ingestion, queries, and replays."""

    prices_ingested: int = 0
    positions_ingested: int = 0
    fx_rates_ingested: int = 0
    actions_ingested: int = 0
    actions_applied: int = 0
    actions_cancelled: int = 0
    actions_quarantined: int = 0
    queries_executed: int = 0
    replays_executed: int = 0
    validation_errors: int = 0
    events_log: List[Dict[str, Any]] = field(default_factory=list)

    def record_event(self, event_type: str, **fields: Any) -> None:
        entry = {"event_type": event_type, **fields}
        self.events_log.append(entry)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "prices_ingested": self.prices_ingested,
            "positions_ingested": self.positions_ingested,
            "fx_rates_ingested": self.fx_rates_ingested,
            "actions_ingested": self.actions_ingested,
            "actions_applied": self.actions_applied,
            "actions_cancelled": self.actions_cancelled,
            "actions_quarantined": self.actions_quarantined,
            "queries_executed": self.queries_executed,
            "replays_executed": self.replays_executed,
            "validation_errors": self.validation_errors,
        }
