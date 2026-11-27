# Financial Data Lake — Implementation Plan

| Field | Value |
|---|---|
| Project | 17 of 25 |
| Classification | Extension / integration of existing portfolio foundations |
| Priority | Very high |
| Proposed owner | CTO / trading engineering (reviewers to be assigned) |
| Suggested stack | Object storage, Parquet, Arrow, Polars or DuckDB, catalog database, orchestration |
| Initial deployment | Local reproducible prototype, then approved paper/sandbox environment |
| Status | Proposed; not an assertion of SAAT's existing capability |

## 1. Objective and scope

Extend tick history into a versioned, point-in-time data platform for prices, corporate actions, fundamentals and approved alternative datasets.

**MVP deliverables**
- Data catalog and entitlement registry
- Raw/normalized/curated layers
- Partitioned immutable datasets
- Quality and lineage pipeline
- As-of query and reproducible snapshots

**Explicit non-goals**
- No redistribution or use of market data outside entitlement
- no destructive history rewrite

## 2. Existing foundations and integration dependencies

- [Trading-Software — 08-market-data-platform](https://github.com/vaibhavkapur/Trading-Software/tree/main/projects/08-market-data-platform)
- [Quantum-Trading — 02-hybrid-quantum-portfolio-optimizer](https://github.com/vaibhavkapur/Quantum-Trading/tree/main/projects/02-hybrid-quantum-portfolio-optimizer)

Before coding, inspect the linked repository at a pinned commit: inventory reusable contracts, tests and documented limitations. Prefer an adapter or incremental extension over duplicating the implementation. Preserve baseline tests and benchmark evidence.

**Dependencies / related plans:** Existing TS-08; #19 supplies optional alternative datasets.

Confirm access, licenses, background IP and SAAT ownership terms before copying or adapting any personal repository. A linked prototype is not proof of production readiness. If access or licensing is unresolved, implement against clean, documented contracts instead.

## 3. Proposed architecture and component boundaries

| Component / workstream | Responsibility and boundary |
|---|---|
| Data catalog and entitlement registry | Own the canonical domain contract and validate incoming data; publish versioned inputs. |
| Raw/normalized/curated layers | Implement the core deterministic or research processing; isolate optional external dependencies. |
| Partitioned immutable datasets | Persist results or state with reproducible provenance; expose explicit failure modes. |
| Quality and lineage pipeline | Expose integration or operational interfaces; apply authorization and fail-safe validation. |
| As-of query and reproducible snapshots | Deliver reviewable evidence, operator workflow and integration tests; do not bypass approvals. |

### Suggested repository layout

```text
financial-data-lake/
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

- [x] Audit ClickHouse tick schemas and fixture loaders; specify source entitlements, event/availability timestamps and schema evolution.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

### M2 — Core implementation

- [x] Implement raw landing, normalized Parquet, checksums and idempotent ingest; add prices and reference data.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

### M3 — Integration and validation

- [x] Add corporate actions, revisions, fundamentals and quality rules; implement as-of queries and dataset manifests.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

### M4 — Hardening and delivery

- [x] Test backfill, replay and recovery; publish catalog, retention policy and sample research notebook.
- [x] Document interfaces, assumptions and design decisions introduced in this milestone.
- [x] Add automated tests and store a reproducible demonstration or validation report.

## 5. Testing and validation

Implement unit tests for domain invariants, integration tests for adapter contracts and scenario tests for failure paths. Use fixed seeds where applicable; avoid testing only happy paths.

| Scenario | Required verification |
|---|---|
| Duplicate ticks | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Late correction | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Split/dividend | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Schema change | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Corrupt partition | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Future revision | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |
| Unauthorized dataset | Reproduce the case with a fixture or injected fault; assert the documented correct outcome, rejection or recovery and retain evidence. |

For quantitative work, use chronological splits, point-in-time inputs, realistic costs where relevant, and report failed experiments as well as successful ones. For execution systems, test idempotency, uncertain outcomes and state recovery. Never infer live performance from a simulated test.

## 6. Acceptance criteria / definition of done

- [x] Each dataset has provenance, license and checksum manifest.
- [x] As-of queries exclude unavailable records.
- [x] Re-ingest does not duplicate rows.
- [x] Corrections are versioned.
- [x] Recovery reproduces a selected snapshot.
- [x] A clean checkout can run the documented setup, tests and demo with permitted fixtures.
- [x] Architecture, dependency assumptions, known limitations and runbook are committed.
- [x] Relevant CI checks pass and raw evaluation or test evidence is retained.
- [ ] Security, data licensing, IP and any deployment approval requirements are reviewed. (Prototype controls documented; organizational/legal sign-off required prior to connecting real vendor feeds or paper/live rollout.)

## 7. Operational controls and rollout

- Use synthetic or explicitly permitted historical data first; record provenance and entitlements.
- Store credentials outside code, separate roles, and restrict access to sensitive artifacts.
- Keep order execution disabled unless a deterministic, independently reviewed policy explicitly permits paper/sandbox operation. Any real-capital deployment is a separate approval decision requiring applicable legal/regulatory, broker, security and risk checks.
- Capture health, failure and decision metrics; document the operator response and safe fallback.
- Establish baseline measurements before setting throughput, latency, accuracy or availability targets; report environment, sample size and uncertainty rather than claiming unmeasured performance.

## 8. Handoff and evidence

Deliver source code, dependency lockfile or reproducible build instructions, architecture diagram, API/schema documentation, test report, demo script, benchmark/evaluation artifacts where applicable, and an explicit list of unsupported features. The project is complete at MVP scope only after its acceptance checklist has evidence attached.

**Next decision:** confirm the actual SAAT asset class, strategy horizon, data access, operating constraints and code ownership before selecting implementation depth or connecting real providers.
