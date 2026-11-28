"""Optional FastAPI ASGI adapter wrapping SecurityMasterService.

The core platform runs with zero third-party dependencies via ``sec_master.http_api``.
When the optional ``[fastapi]`` extra is installed, ``create_fastapi_app(service)``
exposes the exact same typed endpoints with OpenAPI documentation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .adapters import raw_record_to_envelope
from .errors import SecurityMasterError
from .models import ConflictStatus, IdentifierNamespace, ReviewAction
from .service import SecurityMasterService
from .validation import parse_required_ts


def create_fastapi_app(service: SecurityMasterService) -> Any:
    """Create a FastAPI application backed by ``SecurityMasterService``."""
    try:
        from fastapi import FastAPI, HTTPException, Request
        from fastapi.responses import HTMLResponse, JSONResponse
    except ImportError as exc:
        raise RuntimeError(
            "FastAPI optional dependency is not installed. "
            "Install with `pip install -e '.[fastapi]'` or use `sec_master.http_api`."
        ) from exc

    from .http_api import _render_dashboard_html

    app = FastAPI(title="Security Master Platform", version="0.1.0")

    @app.exception_handler(SecurityMasterError)
    async def domain_error_handler(_request: Request, exc: SecurityMasterError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {"status": "ok", "diagnostics": service.diagnostics()}

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(as_of_observed: Optional[str] = None) -> str:
        obs = (
            parse_required_ts(as_of_observed, "as_of_observed")
            if as_of_observed
            else datetime.now(timezone.utc)
        )
        return _render_dashboard_html(service, obs)

    @app.post("/api/v1/events")
    async def ingest(body: Dict[str, Any]) -> Dict[str, Any]:
        if "events" in body and isinstance(body["events"], list):
            return {"outcomes": [o.to_dict() for o in service.ingest_raw_records(body["events"])]}
        env = raw_record_to_envelope(body)
        return {"outcome": service.ingest_event(env).to_dict()}

    @app.get("/api/v1/lookup")
    def lookup(
        namespace: str,
        value: str,
        as_of_valid: str,
        as_of_observed: str,
        mic: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            ns = IdentifierNamespace(namespace)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Unknown namespace: {namespace}") from exc
        res = service.lookup_as_of(
            namespace=ns,
            value=value,
            as_of_valid=parse_required_ts(as_of_valid, "as_of_valid"),
            as_of_observed=parse_required_ts(as_of_observed, "as_of_observed"),
            mic=mic,
        )
        return {"result": res.to_dict()}

    @app.get("/api/v1/conflicts")
    def list_conflicts(status: Optional[str] = None) -> Dict[str, Any]:
        st = ConflictStatus(status) if status else None
        return {"conflicts": [t.to_dict() for t in service.list_conflicts(st)]}

    @app.post("/api/v1/conflicts/{conflict_id}/resolve")
    def resolve_conflict(conflict_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        obs = (
            parse_required_ts(body["observed_at"], "observed_at")
            if body.get("observed_at")
            else None
        )
        outcome = service.resolve_conflict(
            conflict_id=conflict_id,
            reviewer_id=str(body["reviewer_id"]),
            action=ReviewAction(body["action"]),
            rationale=str(body["rationale"]),
            observed_at=obs,
            override_payload=body.get("override_payload"),
        )
        return {"outcome": outcome.to_dict()}

    @app.get("/api/v1/quality")
    def quality(as_of_observed: Optional[str] = None) -> Dict[str, Any]:
        obs = (
            parse_required_ts(as_of_observed, "as_of_observed")
            if as_of_observed
            else datetime.now(timezone.utc)
        )
        return {
            "quality": service.quality_metrics(obs).to_dict(),
            "diagnostics": service.diagnostics(),
        }

    return app
