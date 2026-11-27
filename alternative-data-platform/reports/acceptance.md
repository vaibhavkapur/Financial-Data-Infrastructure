# Local MVP acceptance evidence

## Reproduce the evidence

From the project directory, run `python3 scripts/validate.py`. It runs the unit, integration and scenario suites and compares a fresh two-arrival replay to the retained exports and health counters. Use `python3 scripts/demo.py` to inspect the full replay and `python3 scripts/baseline.py` to repeat the timing sample.

- [Raw validation output](validation.txt)
- [Reference exports, raw hashes, license versions and health](demo.json)
- [Baseline environment and seven individual timings](baseline.json)

## Milestone results

**M1.** Both fixtures are original synthetic content. The versioned registry defines the collection grant, expiry, permitted uses, provenance, units and entity candidates. Unit tests exercise the schema and time rules. [Registry](../config/registry.json) · [Contracts and assumptions](../docs/architecture.md)

**M2.** Integration tests read the original bytes back by hash, reject in-place changes, reopen the database, race two workers against the same page, and interrupt a transaction before its checkpoint. Scenario tests exercise bounded retries and resume after an outage. [Integration tests](../tests/integration/test_pipeline.py)

**M3.** At the first cutoff the January macro value is 101.2. At the second cutoff revision 2 is 102.4 and revision 1 remains retrievable. December retains its null and missing reason. The news article is held through its embargo. The second delivery produces one duplicate and one ambiguous-entity issue. [Reference replay](demo.json)

**M4.** Tests exercise the public CLI, license propagation, mixed-source export denial, a policy change during fetch, expiry, source removal, page gaps, corrupt bytes and restart. The operator can inspect cursor, backlog, quality issues and failure counters. [Failure and recovery scenarios](../tests/scenarios/test_operations.py) · [Operator runbook](../docs/runbook.md)

## Definition of done

- **Provenance and revision history.** Every exported row has a source, publication and availability timestamps, revision, registry version and raw-byte reference. The replay contains two immutable versions of the January macro series.
- **Idempotency.** Restart, injected failure and two-worker tests verify a single accepted copy per logical revision and one committed receipt per successful page. Each new corrupt fetch retains its own rejection receipt.
- **License propagation.** The CLI and historical-license tests verify the requested purpose, current source grant, originating grant and bundled terms. Revocation denies new historical exports.
- **Health and gaps.** The missing-page test observes a backlog of two and a blocked first sequence. After an outage on page two, recovery advances the cursor from one to two and clears the active error. Cumulative event and quality counts survive.
- **Reproducible setup.** The project uses the Python standard library. Local validation covers the documented CLI, tests and deterministic demo in an isolated project copy.
- **Documentation and CI.** The directory contains architecture, schemas, assumptions, limitations, runbook and a GitHub Actions workflow. The workflow's validation command passed locally. The supplied directory has no Git repository or remote, so a commit and hosted CI run remain unverified.
- **Review boundary.** The fixture provenance, local security controls, IP assumptions and deployment prerequisites are documented. Organizational review, ownership confirmation and deployment authorization remain open. [Review and rollout requirements](../docs/runbook.md)

## Baseline scope

Each sample creates a fresh local archive, ingests six rows across four pages, polls both sources again, exports both cuts and collects health. Four accepted revisions remain after duplicate handling and quarantine. The seven observations describe this small fixture workload on the recorded machine. Filesystem caching and other local load can materially affect these timings. Production capacity and service-level targets remain unmeasured.
