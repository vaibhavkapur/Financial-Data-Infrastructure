# Alternative Data Platform — Implementation Plan

| Field | Value |
|---|---|
| Project | 19 of 25 |
| Classification | New capability |
| Priority | High |
| Proposed owner | CTO / trading engineering (reviewers to be assigned) |
| Suggested stack | Python, object storage, Parquet, PostgreSQL, scheduled ingestion workers |
| Initial deployment | Local reproducible prototype, then approved paper/sandbox environment |
| Status | Local synthetic-data MVP implemented. Organizational review and deployment approval remain open. |

## 1. Objective and scope

Ingest permitted non-price data such as macro releases, filings, financial news and other approved sources with provenance and revision history.

**MVP deliverables**
- Source registry and entitlement checks
- Connector framework
- Timestamped raw archive
- Normalization, entity resolution and quality checks
- Curated dataset exports

**Explicit non-goals**
- No collection of nonpublic or improperly obtained market-moving information
- no scraping against terms

## 2. Existing foundations and integration dependencies

- Greenfield capability in the prior GitHub coverage audit. Reuse common contracts and fixtures only after checking ownership and compatibility.

Start with a small independently runnable vertical slice. Do not create duplicate backtesters, order managers, ledgers or data ingestion services when a reviewed shared interface already exists.

**Dependencies / related plans:** #17; supports #4, #5, #6 and #18.

Confirm access, licenses, background IP and SAAT ownership terms before copying or adapting any personal repository. A linked prototype is not proof of production readiness. If access or licensing is unresolved, implement against clean, documented contracts instead.

## 3. Proposed architecture and component boundaries

| Component / workstream | Responsibility and boundary |
|---|---|
| Source registry and entitlement checks | Own the canonical domain contract and validate incoming data; publish versioned inputs. |
| Connector framework | Implement the core deterministic or research processing; isolate optional external dependencies. |
| Timestamped raw archive | Persist results or state with reproducible provenance; expose explicit failure modes. |
| Normalization, entity resolution and quality checks | Expose integration or operational interfaces; apply authorization and fail-safe validation. |
| Curated dataset exports | Deliver reviewable evidence, operator workflow and integration tests; do not bypass approvals. |

### Suggested repository layout

```text
alternative-data-platform/
  plan.md
  README.md
  docs/              # architecture, ADRs, assumptions, runbook
  src/               # domain contracts, core logic, adapters
  config/            # versioned settings; no secrets
  data/fixtures/     # permitted or synthetic inputs and manifests
  tests/unit/
  tests/integration/
  tests/scenarios/
  scripts/           # setup, demo, reproduce, validate
  reports/           # evaluation, benchmarks, acceptance evidence
```

Adapt the directory layout to the language and existing repository. Define domain types and schemas before choosing a distributed deployment; start with the simplest process boundaries that satisfy the workload. Use stable identifiers, explicit units/currency where relevant, UTC event and availability timestamps, versioned configuration and structured errors. Separate external provider interfaces from business logic.

## 4. Implementation milestones

Milestones are ordered by dependency, not promised calendar dates. Each milestone ends with a runnable demo and passing relevant tests.

### M1 — Foundation and design

- [x] Choose two permitted sample sources and define entitlement, schema, revision and embargo rules.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

### M2 — Core implementation

- [x] Build connectors with retries, checkpoints, content hashing and immutable raw storage.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

### M3 — Integration and validation

- [x] Normalize entity IDs and timestamps; deduplicate; preserve release revisions and missingness.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

### M4 — Hardening and delivery

- [x] Publish curated datasets and ingestion monitoring; test outages, revision handling and source removal.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

## 5. Testing and validation

Implement unit tests for domain invariants, integration tests for adapter contracts and scenario tests for failure paths. Use fixed seeds where applicable; avoid testing only happy paths.

| Scenario | Required verification |
|---|---|
| Late publication | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Revised macro series | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Duplicate article | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Ambiguous entity | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Rate limit | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Revoked entitlement | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Corrupt payload | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |

For quantitative work, use chronological splits, point-in-time inputs, realistic costs where relevant, and report failed experiments as well as successful ones. For execution systems, test idempotency, uncertain outcomes and state recovery. Never infer live performance from a simulated test.

## 6. Acceptance criteria / definition of done

- [x] Each record carries source, publication/availability time and version.
- [x] Revisions do not overwrite prior snapshots.
- [x] Connector retry is idempotent.
- [x] License restrictions propagate to exports.
- [x] Source health and gaps are visible.
- [x] A clean checkout can run the documented setup, tests and demo with permitted fixtures.
- [ ] Architecture, dependency assumptions, known limitations and runbook are committed.
- [x] The CI validation command passes locally and raw evaluation and test evidence is retained. Hosted CI awaits a repository.
- [ ] Security, data licensing, IP and any deployment approval requirements are reviewed.

See the [acceptance evidence](reports/acceptance.md) for test results, artifacts and outstanding sign-offs.

## 7. Operational controls and rollout

- Use synthetic or explicitly permitted historical data first; record provenance and entitlements.
- Store credentials outside code, separate roles, and restrict access to sensitive artifacts.
- Keep order execution disabled unless a deterministic, independently reviewed policy explicitly permits paper/sandbox operation. Any real-capital deployment is a separate approval decision requiring applicable legal/regulatory, broker, security and risk checks.
- Capture health, failure and decision metrics; document the operator response and safe fallback.
- Establish baseline measurements before setting throughput, latency, accuracy or availability targets; report environment, sample size and uncertainty rather than claiming unmeasured performance.

## 8. Handoff and evidence

Deliver source code, dependency lockfile or reproducible build instructions, architecture diagram, API/schema documentation, test report, demo script, benchmark/evaluation artifacts where applicable, and an explicit list of unsupported features. The project is complete at MVP scope only after its acceptance checklist has evidence attached.

**Next decision:** confirm the actual SAAT asset class, strategy horizon, data access, operating constraints and code ownership before selecting implementation depth or connecting real providers.
