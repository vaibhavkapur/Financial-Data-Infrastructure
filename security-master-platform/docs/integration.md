# Security Master Platform — Integration Boundaries

This document defines independent, contract-based adapter boundaries between `security-master-platform/` and sibling projects inside the `Trading-Data-Infrastructure` repository. Each project remains independently runnable with no cross-project code imports or proprietary dependencies.

## Sibling Projects in `Trading-Data-Infrastructure`

| Sibling Directory | Integration Role |
|---|---|
| `financial-corporate-actions-engine/` | Upstream producer of corporate-action announcements (`TICKER_CHANGE`, `MERGER`, `SPLIT`, `REVERSE_SPLIT`, `SPINOFF`) and downstream consumer of canonical `instrument_id` / `listing_id` identities |
| `distributed-tick-data-database/` | Downstream consumer resolving tick-stream `(mic, ticker, event_ts)` tuples into immutable `listing_id` / `instrument_id` keys via `GET /api/v1/lookup` |
| `options-market-data-platform/` | Downstream consumer resolving underlying equity/ETF `instrument_id` and split/merger corporate-action links for historical option chain alignment |
| `distributed-quant-research-platform/` | Downstream point-in-time backtesting consumer querying `as_of_valid` and `as_of_observed` to eliminate survivorship and look-ahead bias |
| `financial-data-lake/` & `financial-feature-store/` | Downstream consumers aligning reference instrument identities across curated partitions and feature entities |

## Adapter Interfaces Provided by Security Master Platform

1. **Inbound Corporate-Action Adapter (`sec_master.adapters.CorporateActionFeedAdapter`):**
   - Accepts canonical corporate-action JSON dictionaries (`source_event_id`, `action_type`, `from_instrument_id`, `to_instrument_id`, `ratio`, `effective_at`, `observed_at`, optional `from_listing_id`/`to_listing_id`/`metadata`) compatible with `financial-corporate-actions-engine/`.
   - Emits validated `EventEnvelope(event_type=CORPORATE_ACTION_LINK)` objects with deterministic `event_id`s and full source lineage.
2. **Inbound Vendor Reference Adapter (`sec_master.adapters.VendorReferenceAdapter`):**
   - Translates source-specific reference batches into `EventEnvelope` streams with explicit `source_system` and `schema_version = "1.0.0"`.
3. **Outbound Point-in-Time Query API (`GET /api/v1/lookup`, `GET /api/v1/instruments/{id}/listings`, `GET /api/v1/instruments/{id}/corporate-actions`):**
   - Consumed over HTTP+JSON (or directly via typed `SecurityMasterService` in Python) by tick ingestion pipelines, corporate-action adjustment jobs, and research backtests.
   - Explicit `PRE_OBSERVATION_LOOKUP` (`422`) and `CONFLICT_BLOCKED` (`409`) responses guarantee downstream consumers never silently ingest look-ahead or unreviewed conflicting mappings.

## Compatibility & Versioning Policy

- All event envelopes and REST payloads carry `schema_version: "1.0.0"`.
- Breaking contract changes require bumping major `schema_version` and updating `SUPPORTED_SCHEMA_VERSIONS` in `src/sec_master/config.py`.
- Cross-project imports are intentionally avoided until sibling packages publish versioned interfaces; integration proceeds strictly through JSON event envelopes or HTTP endpoints.
