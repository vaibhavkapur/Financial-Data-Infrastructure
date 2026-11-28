"""Zero-dependency WSGI HTTP REST API + HTML Quality Dashboard & Reviewer Workflow UI."""

from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from .adapters import raw_record_to_envelope
from .errors import NotFoundError, SecurityMasterError, ValidationError
from .models import (
    ConflictStatus,
    IdentifierNamespace,
    ReviewAction,
    format_ts,
)
from .service import SecurityMasterService
from .validation import parse_required_ts

_LISTINGS_RE = re.compile(r"^/api/v1/instruments/([^/]+)/listings$")
_CORP_ACTIONS_RE = re.compile(r"^/api/v1/instruments/([^/]+)/corporate-actions$")
_RESOLVE_RE = re.compile(r"^/api/v1/conflicts/([^/]+)/resolve$")


def _json_response(
    start_response: Callable[..., Any],
    status: int,
    payload: Dict[str, Any],
) -> List[bytes]:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    phrase = HTTPStatus(status).phrase
    start_response(
        f"{status} {phrase}",
        [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def _html_response(
    start_response: Callable[..., Any],
    status: int,
    html_text: str,
) -> List[bytes]:
    body = html_text.encode("utf-8")
    phrase = HTTPStatus(status).phrase
    start_response(
        f"{status} {phrase}",
        [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def _read_body(environ: Dict[str, Any]) -> bytes:
    try:
        length = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError:
        length = 0
    if length <= 0:
        return b""
    return environ["wsgi.input"].read(length)


def _render_dashboard_html(
    service: SecurityMasterService,
    as_of_observed: datetime,
    banner_message: Optional[str] = None,
) -> str:
    metrics = service.quality_metrics(as_of_observed)
    diag = service.diagnostics()
    open_tickets = service.list_conflicts(ConflictStatus.OPEN)
    resolved_tickets = service.list_conflicts(ConflictStatus.RESOLVED)

    banner_html = ""
    if banner_message:
        banner_html = f'<div class="banner">{html.escape(banner_message)}</div>'

    open_rows = []
    for t in open_tickets:
        cid = html.escape(t.conflict_id)
        existing_pre = html.escape(json.dumps(t.existing_claim, indent=2, sort_keys=True))
        incoming_pre = html.escape(json.dumps(t.incoming_claim, indent=2, sort_keys=True))
        open_rows.append(
            f"""
            <div class="ticket">
              <h3>{cid} &mdash; <span class="badge badge-open">{html.escape(t.conflict_type)}</span></h3>
              <p><strong>Entity Key:</strong> <code>{html.escape(t.entity_key)}</code> |
                 <strong>Created:</strong> {html.escape(format_ts(t.created_at) or '')}</p>
              <div class="claims">
                <div><h4>Existing Claim</h4><pre>{existing_pre}</pre></div>
                <div><h4>Quarantined Incoming Claim</h4><pre>{incoming_pre}</pre></div>
              </div>
              <form method="POST" action="/api/v1/conflicts/{cid}/resolve" class="review-form">
                <input type="hidden" name="redirect_dashboard" value="1" />
                <label>Reviewer ID: <input name="reviewer_id" required placeholder="analyst_1" /></label>
                <label>Action:
                  <select name="action">
                    <option value="ACCEPT_INCOMING">ACCEPT_INCOMING</option>
                    <option value="KEEP_EXISTING">KEEP_EXISTING</option>
                    <option value="CUSTOM_OVERRIDE">CUSTOM_OVERRIDE</option>
                  </select>
                </label>
                <label>Rationale: <input name="rationale" required placeholder="Verified against exchange notice" /></label>
                <label>Override JSON (optional): <input name="override_payload" placeholder='{{"currency":"USD"}}' /></label>
                <button type="submit">Submit Review Decision</button>
              </form>
            </div>
            """
        )

    open_section = (
        "\n".join(open_rows)
        if open_rows
        else "<p class='ok'>No open conflicts in quarantine queue.</p>"
    )

    resolved_rows = []
    for t in resolved_tickets:
        res = t.resolution or {}
        resolved_rows.append(
            f"<tr><td><code>{html.escape(t.conflict_id)}</code></td>"
            f"<td>{html.escape(t.conflict_type)}</td>"
            f"<td><code>{html.escape(t.entity_key)}</code></td>"
            f"<td>{html.escape(str(res.get('action', '')))}</td>"
            f"<td>{html.escape(str(res.get('reviewer_id', '')))}</td>"
            f"<td>{html.escape(str(res.get('rationale', '')))}</td></tr>"
        )
    resolved_table = (
        "<table><thead><tr><th>Conflict ID</th><th>Type</th><th>Entity</th>"
        "<th>Action</th><th>Reviewer</th><th>Rationale</th></tr></thead>"
        f"<tbody>{''.join(resolved_rows)}</tbody></table>"
        if resolved_rows
        else "<p>No resolved conflicts yet.</p>"
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Security Master Platform — Quality Dashboard &amp; Reviewer Workflow</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1a202c; background: #f7fafc; }}
    h1, h2 {{ color: #1a365d; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }}
    .card {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 14px; }}
    .card .val {{ font-size: 24px; font-weight: 700; color: #2b6cb0; }}
    .card .lbl {{ font-size: 12px; color: #4a5568; text-transform: uppercase; }}
    .ticket {{ background: #fff; border: 1px solid #feb2b2; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
    .claims {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
    pre {{ background: #edf2f7; padding: 10px; border-radius: 6px; overflow-x: auto; font-size: 12px; }}
    .badge-open {{ background: #fed7d7; color: #9b2c2c; padding: 2px 8px; border-radius: 12px; font-size: 12px; }}
    .review-form {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; align-items: center; }}
    .review-form input, .review-form select {{ padding: 6px; border: 1px solid #cbd5e0; border-radius: 4px; }}
    button {{ background: #2b6cb0; color: #fff; border: none; padding: 8px 14px; border-radius: 4px; cursor: pointer; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; }}
    th, td {{ border: 1px solid #e2e8f0; padding: 8px; text-align: left; font-size: 13px; }}
    th {{ background: #edf2f7; }}
    .banner {{ background: #c6f6d5; border: 1px solid #68d391; padding: 10px 14px; border-radius: 6px; margin-bottom: 16px; }}
    .ok {{ color: #276749; font-weight: 600; }}
  </style>
</head>
<body>
  <h1>Security Master Platform &mdash; Quality Dashboard</h1>
  <p>As-of Observed: <code>{html.escape(format_ts(as_of_observed) or '')}</code> |
     Schema: <code>{html.escape(diag['schema_version'])}</code> |
     Snapshot Hash: <code>{html.escape(diag['snapshot_hash'][:16])}</code></p>
  {banner_html}
  <div class="grid">
    <div class="card"><div class="lbl">Active Instruments</div><div class="val">{metrics.active_instruments}</div></div>
    <div class="card"><div class="lbl">Active Listings</div><div class="val">{metrics.active_listings}</div></div>
    <div class="card"><div class="lbl">Identifier Mappings</div><div class="val">{metrics.active_identifier_mappings}</div></div>
    <div class="card"><div class="lbl">Completeness</div><div class="val">{metrics.completeness_ratio * 100:.1f}%</div></div>
    <div class="card"><div class="lbl">Lineage Coverage</div><div class="val">{metrics.lineage_coverage_ratio * 100:.1f}%</div></div>
    <div class="card"><div class="lbl">Open Conflicts</div><div class="val">{metrics.open_conflicts}</div></div>
    <div class="card"><div class="lbl">Resolved Conflicts</div><div class="val">{metrics.resolved_conflicts}</div></div>
    <div class="card"><div class="lbl">Max Obs Lag (s)</div><div class="val">{metrics.max_observation_lag_seconds:.0f}</div></div>
  </div>

  <h2>Open Conflict Quarantine Queue</h2>
  {open_section}

  <h2>Resolved Conflict Audit Log</h2>
  {resolved_table}
</body>
</html>
"""


def create_wsgi_app(
    service: SecurityMasterService,
) -> Callable[[Dict[str, Any], Callable[..., Any]], Iterable[bytes]]:
    """Create a WSGI application exposing the Security Master REST API and Dashboard."""

    def app(environ: Dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")
        qs = parse_qs(environ.get("QUERY_STRING", ""))

        try:
            # Dashboard HTML
            if method == "GET" and path in ("/", "/dashboard"):
                obs_raw = qs.get("as_of_observed", [None])[0]
                as_of_obs = (
                    parse_required_ts(obs_raw, "as_of_observed")
                    if obs_raw
                    else datetime.now(timezone.utc)
                )
                msg = qs.get("msg", [None])[0]
                return _html_response(
                    start_response,
                    200,
                    _render_dashboard_html(service, as_of_obs, banner_message=msg),
                )

            # Health
            if method == "GET" and path == "/health":
                return _json_response(
                    start_response,
                    200,
                    {"status": "ok", "diagnostics": service.diagnostics()},
                )

            # Quality metrics JSON
            if method == "GET" and path == "/api/v1/quality":
                obs_raw = qs.get("as_of_observed", [None])[0]
                as_of_obs = (
                    parse_required_ts(obs_raw, "as_of_observed")
                    if obs_raw
                    else datetime.now(timezone.utc)
                )
                return _json_response(
                    start_response,
                    200,
                    {
                        "quality": service.quality_metrics(as_of_obs).to_dict(),
                        "diagnostics": service.diagnostics(),
                    },
                )

            # Event ingestion
            if method == "POST" and path == "/api/v1/events":
                raw_bytes = _read_body(environ)
                try:
                    body = json.loads(raw_bytes.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValidationError(f"Malformed JSON request body: {exc}") from exc
                if isinstance(body, dict) and "events" in body and isinstance(body["events"], list):
                    outcomes = service.ingest_raw_records(body["events"])
                    return _json_response(
                        start_response,
                        200,
                        {"outcomes": [o.to_dict() for o in outcomes]},
                    )
                if isinstance(body, dict):
                    env = raw_record_to_envelope(body)
                    outcome = service.ingest_event(env)
                    return _json_response(start_response, 200, {"outcome": outcome.to_dict()})
                raise ValidationError("Request body must be an event JSON object or {'events': [...]}.")

            # As-of symbol / identifier lookup
            if method == "GET" and path == "/api/v1/lookup":
                raw_ns = (qs.get("namespace") or [None])[0]
                value = (qs.get("value") or [None])[0]
                valid_raw = (qs.get("as_of_valid") or [None])[0]
                obs_raw = (qs.get("as_of_observed") or [None])[0]
                mic = (qs.get("mic") or [None])[0]
                if not raw_ns or not value or not valid_raw or not obs_raw:
                    raise ValidationError(
                        "Query parameters 'namespace', 'value', 'as_of_valid', and 'as_of_observed' are required."
                    )
                try:
                    ns = IdentifierNamespace(raw_ns)
                except ValueError as exc:
                    raise ValidationError(f"Unknown namespace '{raw_ns}'.") from exc
                as_of_valid = parse_required_ts(valid_raw, "as_of_valid")
                as_of_observed = parse_required_ts(obs_raw, "as_of_observed")
                res = service.lookup_as_of(
                    namespace=ns,
                    value=value,
                    as_of_valid=as_of_valid,
                    as_of_observed=as_of_observed,
                    mic=mic,
                )
                return _json_response(start_response, 200, {"result": res.to_dict()})

            # Listings for instrument
            m_list = _LISTINGS_RE.match(path)
            if method == "GET" and m_list:
                inst_id = m_list.group(1)
                valid_raw = (qs.get("as_of_valid") or [None])[0]
                obs_raw = (qs.get("as_of_observed") or [None])[0]
                if not valid_raw or not obs_raw:
                    raise ValidationError("'as_of_valid' and 'as_of_observed' are required.")
                listings = service.get_listings(
                    inst_id,
                    parse_required_ts(valid_raw, "as_of_valid"),
                    parse_required_ts(obs_raw, "as_of_observed"),
                )
                return _json_response(
                    start_response,
                    200,
                    {"instrument_id": inst_id, "listings": [item.to_dict() for item in listings]},
                )

            # Corporate actions lineage
            m_ca = _CORP_ACTIONS_RE.match(path)
            if method == "GET" and m_ca:
                inst_id = m_ca.group(1)
                valid_raw = (qs.get("as_of_valid") or [None])[0]
                obs_raw = (qs.get("as_of_observed") or [None])[0]
                if not valid_raw or not obs_raw:
                    raise ValidationError("'as_of_valid' and 'as_of_observed' are required.")
                lineage = service.trace_corporate_actions(
                    inst_id,
                    parse_required_ts(valid_raw, "as_of_valid"),
                    parse_required_ts(obs_raw, "as_of_observed"),
                )
                return _json_response(start_response, 200, {"lineage": lineage})

            # List conflicts
            if method == "GET" and path == "/api/v1/conflicts":
                raw_status = (qs.get("status") or [None])[0]
                status_filter: Optional[ConflictStatus] = None
                if raw_status:
                    try:
                        status_filter = ConflictStatus(raw_status)
                    except ValueError as exc:
                        raise ValidationError(f"Invalid conflict status '{raw_status}'.") from exc
                tickets = service.list_conflicts(status=status_filter)
                return _json_response(
                    start_response,
                    200,
                    {"conflicts": [t.to_dict() for t in tickets]},
                )

            # Resolve conflict (supports JSON or HTML dashboard form submission)
            m_res = _RESOLVE_RE.match(path)
            if method == "POST" and m_res:
                conflict_id = m_res.group(1)
                raw_bytes = _read_body(environ)
                content_type = environ.get("CONTENT_TYPE", "")
                redirect_dashboard = False
                if "application/x-www-form-urlencoded" in content_type:
                    form = parse_qs(raw_bytes.decode("utf-8"))
                    reviewer_id = (form.get("reviewer_id") or [""])[0].strip()
                    raw_action = (form.get("action") or [""])[0].strip()
                    rationale = (form.get("rationale") or [""])[0].strip()
                    obs_raw = (form.get("observed_at") or [None])[0]
                    ov_raw = (form.get("override_payload") or [""])[0].strip()
                    override_payload = json.loads(ov_raw) if ov_raw else None
                    redirect_dashboard = (form.get("redirect_dashboard") or [""])[0] == "1"
                else:
                    try:
                        body = json.loads(raw_bytes.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise ValidationError(f"Malformed JSON body: {exc}") from exc
                    reviewer_id = str(body.get("reviewer_id", "")).strip()
                    raw_action = str(body.get("action", "")).strip()
                    rationale = str(body.get("rationale", "")).strip()
                    obs_raw = body.get("observed_at")
                    override_payload = body.get("override_payload")

                if not reviewer_id or not rationale:
                    raise ValidationError("'reviewer_id' and 'rationale' are required.")
                try:
                    action = ReviewAction(raw_action)
                except ValueError as exc:
                    raise ValidationError(
                        f"Invalid review action '{raw_action}'.",
                        details={"allowed": [a.value for a in ReviewAction]},
                    ) from exc
                obs_dt = parse_required_ts(obs_raw, "observed_at") if obs_raw else None
                outcome = service.resolve_conflict(
                    conflict_id=conflict_id,
                    reviewer_id=reviewer_id,
                    action=action,
                    rationale=rationale,
                    observed_at=obs_dt,
                    override_payload=override_payload,
                )
                if redirect_dashboard:
                    start_response(
                        "303 See Other",
                        [("Location", f"/dashboard?msg=Resolved+{conflict_id}")],
                    )
                    return [b""]
                return _json_response(start_response, 200, {"outcome": outcome.to_dict()})

            # Replay journal
            if method == "POST" and path == "/api/v1/replay":
                result = service.replay_from_journal()
                return _json_response(start_response, 200, {"replay": result})

            raise NotFoundError(f"Route not found: {method} {path}")

        except SecurityMasterError as exc:
            return _json_response(start_response, exc.http_status, exc.to_dict())
        except Exception as exc:  # pragma: no cover - defensive fallback
            return _json_response(
                start_response,
                500,
                {"error": {"code": "INTERNAL_ERROR", "message": str(exc), "details": {}}},
            )

    return app


def run_server(service: SecurityMasterService, host: str, port: int) -> None:
    """Run a local HTTP server hosting the REST API and Quality Dashboard."""
    app = create_wsgi_app(service)
    with make_server(host, port, app) as httpd:
        print(f"Security Master Platform listening on http://{host}:{port} (Dashboard: /dashboard)")
        httpd.serve_forever()
