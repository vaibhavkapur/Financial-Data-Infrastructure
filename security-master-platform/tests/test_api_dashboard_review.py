"""M4 HTTP API, Quality Dashboard, and Reviewer Correction Workflow end-to-end tests."""

from __future__ import annotations

import io
import json
import unittest
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import urlencode

from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.http_api import create_wsgi_app
from sec_master.service import SecurityMasterService

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _call_wsgi(
    app: Any,
    method: str,
    path: str,
    query: str = "",
    body: bytes = b"",
    content_type: str = "application/json",
) -> Tuple[int, Dict[str, str], bytes]:
    captured: Dict[str, Any] = {}

    def start_response(status: str, headers: list) -> None:
        captured["status"] = int(status.split(" ", 1)[0])
        captured["headers"] = {k.lower(): v for k, v in headers}

    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "CONTENT_LENGTH": str(len(body)),
        "CONTENT_TYPE": content_type,
        "wsgi.input": io.BytesIO(body),
    }
    chunks = list(app(environ, start_response))
    return captured["status"], captured["headers"], b"".join(chunks)


class TestApiDashboardAndReviewWorkflow(unittest.TestCase):
    def setUp(self) -> None:
        base = load_config(PROJECT_ROOT / "config/default.json")
        cfg = PlatformConfig(
            config_version=base.config_version,
            schema_version=base.schema_version,
            environment=base.environment,
            storage=StorageConfig(backend="memory", sqlite_path=":memory:"),
            licensing=base.licensing,
            conflict_resolution=base.conflict_resolution,
            quality=base.quality,
            reproducibility=base.reproducibility,
            http=base.http,
        )
        self.service = SecurityMasterService(cfg)
        self.app = create_wsgi_app(self.service)

    def tearDown(self) -> None:
        self.service.close()

    def test_full_api_dashboard_and_reviewer_accept_incoming_workflow(self) -> None:
        # 1. Health check
        status, _, body = _call_wsgi(self.app, "GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "ok")

        # 2. Ingest late_correction_conflicts fixture via POST /api/v1/events
        lc_fixture = json.loads(
            (PROJECT_ROOT / "data/fixtures/late_correction_conflicts.json").read_text(
                encoding="utf-8"
            )
        )
        status, _, body = _call_wsgi(
            self.app,
            "POST",
            "/api/v1/events",
            body=json.dumps({"events": lc_fixture["events"]}).encode("utf-8"),
        )
        self.assertEqual(status, 200)
        outcomes = json.loads(body)["outcomes"]
        self.assertEqual(outcomes[-1]["status"], "QUARANTINED")
        conflict_id = outcomes[-1]["conflict_id"]

        # 3. Quality Dashboard HTML renders open conflict ticket & metrics
        status, headers, html_body = _call_wsgi(
            self.app,
            "GET",
            "/dashboard",
            query=urlencode({"as_of_observed": "2024-01-12T12:00:00Z"}),
        )
        self.assertEqual(status, 200)
        self.assertIn("text/html", headers["content-type"])
        html_text = html_body.decode("utf-8")
        self.assertIn(conflict_id, html_text)
        self.assertIn("Open Conflict Quarantine Queue", html_text)

        # 4. Quality JSON reports 1 open conflict and 100% lineage coverage
        status, _, q_body = _call_wsgi(
            self.app,
            "GET",
            "/api/v1/quality",
            query=urlencode({"as_of_observed": "2024-01-12T12:00:00Z"}),
        )
        self.assertEqual(status, 200)
        q_json = json.loads(q_body)["quality"]
        self.assertEqual(q_json["open_conflicts"], 1)
        self.assertEqual(q_json["lineage_coverage_ratio"], 1.0)

        # 5. Lookup is blocked with HTTP 409 CONFLICT_BLOCKED while ticket is open
        status, _, err_body = _call_wsgi(
            self.app,
            "GET",
            "/api/v1/lookup",
            query=urlencode(
                {
                    "namespace": "MIC_TICKER",
                    "value": "XNAS:NOVA",
                    "as_of_valid": "2024-01-05T00:00:00Z",
                    "as_of_observed": "2024-01-15T00:00:00Z",
                }
            ),
        )
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(err_body)["error"]["code"], "CONFLICT_BLOCKED")

        # 6. Reviewer resolves conflict via API accepting incoming ISIN (US0000009995)
        status, _, rev_body = _call_wsgi(
            self.app,
            "POST",
            f"/api/v1/conflicts/{conflict_id}/resolve",
            body=json.dumps(
                {
                    "reviewer_id": "lead_analyst_bob",
                    "action": "ACCEPT_INCOMING",
                    "rationale": "Exchange corrigendum confirmed new ISIN US0000009995.",
                    "observed_at": "2024-01-14T09:00:00Z",
                }
            ).encode("utf-8"),
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(rev_body)["outcome"]["status"], "RESOLVED")

        # 7. Lookup after review resolves cleanly to US0000009995 WITH reviewer lineage,
        #    while historical as-of lookup BEFORE review still sees US0000004046!
        status_pre, _, body_pre = _call_wsgi(
            self.app,
            "GET",
            "/api/v1/lookup",
            query=urlencode(
                {
                    "namespace": "MIC_TICKER",
                    "value": "XNAS:NOVA",
                    "as_of_valid": "2024-01-05T00:00:00Z",
                    "as_of_observed": "2024-01-13T00:00:00Z",
                }
            ),
        )
        self.assertEqual(status_pre, 200)
        pre_ids = {
            m["namespace"]: m for m in json.loads(body_pre)["result"]["identifiers"]
        }
        self.assertEqual(pre_ids["ISIN"]["identifier_value"], "US0000004046")

        status_post, _, body_post = _call_wsgi(
            self.app,
            "GET",
            "/api/v1/lookup",
            query=urlencode(
                {
                    "namespace": "MIC_TICKER",
                    "value": "XNAS:NOVA",
                    "as_of_valid": "2024-01-05T00:00:00Z",
                    "as_of_observed": "2024-01-15T00:00:00Z",
                }
            ),
        )
        self.assertEqual(status_post, 200)
        post_ids = {
            m["namespace"]: m for m in json.loads(body_post)["result"]["identifiers"]
        }
        self.assertEqual(post_ids["ISIN"]["identifier_value"], "US0000009995")
        self.assertEqual(post_ids["ISIN"]["lineage"]["reviewer_id"], "lead_analyst_bob")
        self.assertEqual(post_ids["ISIN"]["lineage"]["conflict_id"], conflict_id)

        # 8. Pre-observation lookup via API returns HTTP 422 PRE_OBSERVATION_LOOKUP
        status_422, _, body_422 = _call_wsgi(
            self.app,
            "GET",
            "/api/v1/lookup",
            query=urlencode(
                {
                    "namespace": "MIC_TICKER",
                    "value": "XNAS:NOVA",
                    "as_of_valid": "2024-01-01T12:00:00Z",
                    "as_of_observed": "2024-01-01T12:00:00Z",
                }
            ),
        )
        self.assertEqual(status_422, 422)
        self.assertEqual(json.loads(body_422)["error"]["code"], "PRE_OBSERVATION_LOOKUP")

    def test_dashboard_form_custom_override_submission(self) -> None:
        lc_fixture = json.loads(
            (PROJECT_ROOT / "data/fixtures/late_correction_conflicts.json").read_text(
                encoding="utf-8"
            )
        )
        outs = self.service.ingest_raw_records(lc_fixture["events"])
        conflict_id = outs[-1].conflict_id

        form_bytes = urlencode(
            {
                "redirect_dashboard": "1",
                "reviewer_id": "reviewer_carol",
                "action": "CUSTOM_OVERRIDE",
                "rationale": "Verified third authoritative ISIN from national numbering agency.",
                "observed_at": "2024-01-14T15:00:00Z",
                "override_payload": json.dumps({"identifier_value": "US0000005050"}),
            }
        ).encode("utf-8")
        status, headers, _ = _call_wsgi(
            self.app,
            "POST",
            f"/api/v1/conflicts/{conflict_id}/resolve",
            body=form_bytes,
            content_type="application/x-www-form-urlencoded",
        )
        self.assertEqual(status, 303)
        self.assertIn("/dashboard", headers["location"])

        status, _, body = _call_wsgi(
            self.app,
            "GET",
            "/api/v1/lookup",
            query=urlencode(
                {
                    "namespace": "ISIN",
                    "value": "US0000005050",
                    "as_of_valid": "2024-01-05T00:00:00Z",
                    "as_of_observed": "2024-01-15T00:00:00Z",
                }
            ),
        )
        self.assertEqual(status, 200)
        res = json.loads(body)["result"]
        self.assertEqual(res["instrument"]["instrument_id"], "SEC-4524211962DF")
        self.assertEqual(res["matched_identifier"]["lineage"]["reviewer_id"], "reviewer_carol")


if __name__ == "__main__":
    unittest.main()
