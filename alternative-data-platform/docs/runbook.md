# Operator runbook

## Ingest and monitor

Follow the persistent archive commands in the README. The CLI prints JSON to stdout and exits zero on success. A domain or storage failure returns exit code 1 and a JSON error on stderr. The archive directory and fixture paths are operator-controlled. Use a private directory and `umask 077` for both the database and redirected exports.

A scheduler may invoke the same one-shot ingest commands after provider delivery. The cursor is durable across process restarts. Inspect `health` for the last successful poll, last error, current entitlement, accepted revisions, observed high-water mark, cursor, outstanding pages, next expected sequence and cumulative quality and event counts. A source can complete its poll with quarantined rows. A zero backlog covers only pages advertised at the last poll. Compare last success with the expected source schedule to detect silence.

## Recover an interrupted feed

- **Outage or rate limit.** Check the last error and retry counts. Honor long retry delays before running again. Restore source access, then rerun ingestion. The worker resumes at the first uncommitted page.
- **Missing page or regressed manifest.** Restore the advertised contiguous sequence and rerun. Never advance the cursor by hand.
- **Malformed payload.** The rejected bytes and receipt are retained after decoding fails. Restore a corrected page at the blocked sequence and rerun. Both receipts remain available for audit.
- **Row quality issue.** Inspect `issues` joined with `receipts` on receipt ID. The receipt names the source, sequence, raw hash and registry version. The ordinal identifies the rejected row. Resolve an alias or unit in a new registry version if needed, then append a new feed page containing the corrected row. Use a new revision when correcting an already accepted record.
- **Revocation or source removal.** Disable or omit the source in a higher registry version and apply it. Current and historical export requests are denied immediately for new transactions. Retained raw material stays restricted to the operator. Track any previously delivered files with their recipients. The prototype has no remote recall mechanism.
- **Interrupted process or full disk.** Restore disk capacity and reopen the database. SQLite rolls back unfinished transactions. Check source health and rerun ingestion. Back up the private directory with workers stopped, or use SQLite's online backup interface. Restore into a separate private directory and reconcile cursors and hashes before resuming.

For a permanent curated file, write stdout to a private temporary file and rename it into place only on exit code zero. Keep the entire license-bearing JSON envelope with the dataset.

## Access, IP and release review

The local operator administers policies, reads raw data and runs ingestion and exports. A consumer receives only an authorized bundle. Enforce that separation with OS file permissions. The database, feeds and registry must be writable only by the trusted operator. A purpose flag declares the intended use. This CLI has no multi-user identity or network service. The fixture-only adapter uses local files and requires no secrets.

Both sources contain original fictional data with explicit fixture-use terms. No upstream code, provider content, personal repositories or credentials were imported. Technical checks exercise revocation, embargoes, license propagation and auditability. Independent security, data-license, IP ownership and deployment sign-offs require the responsible organization. The plan names a proposed owner and leaves reviewers unassigned.

Before enabling a real feed, the owner must establish provider access, asset class, strategy horizon, operating constraints, permitted uses, retention/deletion duties and rights to background IP. Define authenticated operator and consumer roles and review access to raw archives, backups and exports. Decide how to enforce legal holds, deletion and downstream revocation. Any source requiring physical erasure is outside this append-only prototype's scope.

## Unsupported features

Live providers, scraping, credentials, distributed workers, managed scheduling and alerts, cross-publisher near-duplicate detection, semantic entity matching, Parquet/object storage, trading integration and deployment are outside this slice. Export materializes the selected rows in memory. The local clock, ordered feed manifest, stable provider keys and source-specific alias maps are trusted inputs. A provider that edits a previously committed page must publish a new sequence. Production throughput, SLA targets and market performance have not been measured.
