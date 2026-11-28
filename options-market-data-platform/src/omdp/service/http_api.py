"""Lightweight stdlib HTTP server for serving historical option chains, features, quality reports, and replay."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from omdp.domain.contracts import ContractValidationError, parse_utc_timestamp
from omdp.service.platform import OptionsMarketDataPlatform


def dispatch_request(
    platform: OptionsMarketDataPlatform,
    method: str,
    raw_path: str,
) -> Tuple[HTTPStatus, Dict[str, Any]]:
    """Pure HTTP route dispatcher usable both by ThreadingHTTPServer and in-process tests."""
    parsed = urlparse(raw_path)
    path = parsed.path.rstrip("/") or "/"
    query = parse_qs(parsed.query)
    verb = method.upper()

    if verb == "GET":
        if path == "/health":
            return (
                HTTPStatus.OK,
                {
                    "status": "ok",
                    "config_version": platform.config.config_version,
                    "schema_version": platform.config.schema_version,
                },
            )

        if path == "/v1/metrics":
            return HTTPStatus.OK, {"counters": platform.counters.to_dict()}

        if path == "/v1/chains":
            underlying = query.get("underlying", [None])[0]
            start_raw = query.get("start", [None])[0]
            end_raw = query.get("end", [None])[0]
            try:
                start_dt = parse_utc_timestamp(start_raw) if start_raw else None
                end_dt = parse_utc_timestamp(end_raw) if end_raw else None
            except ContractValidationError as exc:
                return HTTPStatus.BAD_REQUEST, {"error": str(exc)}
            snaps = platform.query_historical_chains(
                underlying_symbol=underlying,
                start_time=start_dt,
                end_time=end_dt,
            )
            return (
                HTTPStatus.OK,
                {
                    "count": len(snaps),
                    "snapshots": [s.to_dict() for s in snaps],
                },
            )

        parts = [p for p in path.split("/") if p]
        if len(parts) >= 3 and parts[0] == "v1" and parts[1] == "chains":
            snapshot_id = parts[2]
            if len(parts) == 3:
                snap = platform.get_chain_snapshot(snapshot_id)
                if snap is None:
                    return HTTPStatus.NOT_FOUND, {"error": f"Snapshot '{snapshot_id}' not found"}
                return HTTPStatus.OK, snap.to_dict()

            subresource = parts[3] if len(parts) == 4 else None
            if subresource == "features":
                snap = platform.get_chain_snapshot(snapshot_id)
                if snap is None:
                    return HTTPStatus.NOT_FOUND, {"error": f"Snapshot '{snapshot_id}' not found"}
                feats = platform.get_derived_features(snapshot_id)
                return (
                    HTTPStatus.OK,
                    {
                        "snapshot_id": snapshot_id,
                        "count": len(feats),
                        "features": [f.to_dict() for f in feats],
                    },
                )

            if subresource == "quality":
                report = platform.get_quality_report(snapshot_id)
                if report is None:
                    return (
                        HTTPStatus.NOT_FOUND,
                        {"error": f"Quality report for '{snapshot_id}' not found"},
                    )
                return HTTPStatus.OK, report.to_dict()

            if subresource == "replay":
                try:
                    replay = platform.replay_snapshot(snapshot_id)
                except KeyError as exc:
                    return HTTPStatus.NOT_FOUND, {"error": str(exc)}
                return HTTPStatus.OK, replay.to_dict()

        return HTTPStatus.NOT_FOUND, {"error": f"Unknown endpoint: {path}"}

    if verb == "POST":
        parts = [p for p in path.split("/") if p]
        if len(parts) == 4 and parts[0] == "v1" and parts[1] == "chains" and parts[3] == "replay":
            snapshot_id = parts[2]
            try:
                replay = platform.replay_snapshot(snapshot_id)
            except KeyError as exc:
                return HTTPStatus.NOT_FOUND, {"error": str(exc)}
            return HTTPStatus.OK, replay.to_dict()
        return HTTPStatus.NOT_FOUND, {"error": f"Unknown endpoint: {path}"}

    return HTTPStatus.METHOD_NOT_ALLOWED, {"error": f"Unsupported HTTP method: {verb}"}


def _make_handler(platform: OptionsMarketDataPlatform) -> type:
    class OMDPRequestHandler(BaseHTTPRequestHandler):
        def _send_json(self, status: HTTPStatus, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(status.value)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            # Silence default stderr spam; structured logs are emitted by platform logger
            return

        def do_GET(self) -> None:  # noqa: N802
            status, payload = dispatch_request(platform, "GET", self.path)
            self._send_json(status, payload)

        def do_POST(self) -> None:  # noqa: N802
            status, payload = dispatch_request(platform, "POST", self.path)
            self._send_json(status, payload)

    return OMDPRequestHandler


def create_http_server(
    platform: OptionsMarketDataPlatform,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> ThreadingHTTPServer:
    """Create a configured ThreadingHTTPServer bound to host:port (use port=0 for ephemeral port)."""
    handler_cls = _make_handler(platform)
    return ThreadingHTTPServer((host, port), handler_cls)
