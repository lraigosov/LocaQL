# Changelog

All notable user-facing changes to LocaQL are documented here, in the style of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This file is the curated, release-facing summary; `devlog.md` (gitignored, not part of the published repo) is the full internal session-by-session build log this project has kept throughout development — this file distills what actually matters to someone using LocaQL, not the process of building it.

## [Unreleased]

### Added
- Persistent single-statement GoogleSQL DDL/DML for query jobs: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE TABLE`, `CREATE [OR REPLACE] TABLE [AS SELECT]` and `DROP TABLE`, with target-table serialization, optimistic catalog version checks, statement atomicity, query parameters, BigQuery-shaped DML statistics, and a pinned `google-cloud-bigquery 3.42.3` CI smoke.
- Official Python `Client.load_table_from_file` compatibility through BigQuery's real multipart and resumable upload protocols, including `308` range progress, idempotent chunk retries, empty files, real `LoadJob` polling/statistics, and a pinned SDK smoke test in CI. Direct uploads currently require an explicit schema and incomplete resumable sessions are process-local/in-memory; see `KNOWN-DIVERGENCES.md`.
- Official Google Cloud Storage media downloads at `/download/storage/v1/b/{bucket}/o/{object}?alt=media`, including URL-escaped nested names, absolute `mediaLink` resources, `HEAD`, byte ranges (`206`/`Content-Range`) and MD5 response hashes. A pinned `google-cloud-storage 3.13.1` smoke test now covers upload, full download and ranged download in CI; the existing `objects.get?alt=media` route remains compatible.
- Lossless scalar nullability across load, catalog, query, REST, extract and Storage APIs: `NULL`, empty string, zero and false no longer collapse. Nullable Avro uses unions, nullable Parquet uses optional fields, `REQUIRED` null/absence fails explicitly, and the official Python smoke verifies `(None, "", 0, False)` plus correct `COUNT` semantics.

### Fixed
- Polling a pending mutating query no longer executes it before the job acquires its resource lock or causes it to run twice. Whole-reference backticks around same-project `project.dataset.table` are now stripped correctly, and `MERGE` affected-row counts are derived from the before/after row multiset when the embedded driver reports zero despite applying the mutation.
- Final `getQueryResults` responses no longer include `pageToken`. The official Python query `RowIterator` treated its presence as another page and could loop indefinitely even after receiving the last row; this was discovered by the new nullable E2E.

## [0.9.2] - 2026-08-06

## [0.9.1] - 2026-07-28

### Fixed
- `.github/workflows/release.yml` had unresolved git merge conflict markers committed straight into `main` (from reconciling the `v0.9.0` release-tagging PRs), making the workflow file invalid YAML — every push to `main`/`dev` and every release triggered via `auto-tag-release.yml`'s `workflow_call` failed immediately with "workflow file issue". Removed the leftover markers, restored the intended `resolve`-job design, and fixed a `docker/metadata-action` tag pattern that had been silently corrupted to `{{major}}.minor}}` instead of `{{major}}.{{minor}}` in the same conflict.
- The published `ghcr.io/lraigosov/locaql` image only ever contained the emulator — `locaql-ui` (the console) was never built into it, so `docker run` gave API-only access with no way to reach the UI. Fixed by adding a third binary, `cmd/locaql-supervisor`, as the image's entrypoint: it starts both `locaql` and `locaql-ui` as real subprocesses of the one container (no shell involved — the base image has none), forwards shutdown signals to both, and exits non-zero if either crashes rather than leaving the container looking healthy with half its processes dead. `docker run` now publishes `:9070` (console) alongside `:9050`/`:9060` and gets the full solution in one command.
## [Unreleased]

## [0.9.2] - 2026-08-06

### Fixed
- `tabledata.list` 404'd for every table when called through the official `google-cloud-bigquery` client's `list_rows()`/`Table.list_rows()` — the request lands at `GET .../datasets/{datasetId}/tables/{tableId}/data` (real BigQuery's actual REST shape), but the router only recognized a made-up `.../tabledata/{datasetId}/{tableId}/data` shape that no official client ever requests (only this project's own tests and bundled UI did). A schema fetched via `tables.get` for the very same table would succeed, making the 404 look like a data problem rather than a routing gap. Fixed by handling the real path shape explicitly; the old shape is kept working as an internal alias.
- A load job's row/byte counts were written flat under `statistics.outputRows`/`processedBytes` instead of nested under `statistics.load.outputRows`/`outputBytes`, which is what `LoadJob.output_rows`/`output_bytes` on the official client actually reads — so a client that submitted and awaited its own load job always saw `output_rows` as `None`, even though the count had been computed correctly server-side. `Table.num_rows`/`num_bytes` had the same problem one level up: `tables.get` never set `numRows`/`numBytes` at all, for any table, load-populated or not. Both are now populated for managed tables (external tables and views still omit them, matching real BigQuery, since neither materializes rows into storage the same way).
- `tabledata.list`'s pagination response always included a `pageToken` key, even on the final page, echoing the request's own start index. The official client's row iterator for this endpoint is configured to treat the mere *presence* of a `pageToken` key as "there is another page" (`next_token="pageToken"`, not `nextPageToken`, unlike most other list endpoints) — so `list_rows()` looped forever re-fetching the same last page instead of ever returning. `pageToken` (and the `nextPageToken` alias already relied on elsewhere) are now both omitted once there is nothing left to page through.

## [0.9.0] - 2026-07-27

### Added
- BigQuery Storage API (gRPC): `CreateReadSession`/`ReadRows` (Avro framing) and `CreateWriteStream`/`AppendRows`/`GetWriteStream`/`FinalizeWriteStream`/`BatchCommitWriteStreams` (protobuf rows via real runtime reflection), on a separate listener (`--storage-grpc-addr`, default `:9060`). See [README: BigQuery Storage API](README.md#bigquery-storage-api-real-grpc-read-sessions-avro-and-write-streams-protobuf).
- Sessions and multi-statement transactions: `createSession`/`connectionProperties`, session-scoped `_SESSION.<table>` temp tables, real `BEGIN`/`COMMIT`/`ROLLBACK TRANSACTION` atomicity, `INFORMATION_SCHEMA.SESSIONS_BY_USER`.
- Workspace portability over REST and the console: `/_emulator/workspace/validate|plan|diff|apply`, mirroring the `locaql workspace` CLI subcommands.
- Operational observability: structured JSON request logging, `GET /_emulator/metrics` (request/latency/job counters, live queue/backpressure gauges), extended `subsystems` detail on `GET /_emulator/health`/`readiness`.
- Guided troubleshooting: `GET /_emulator/diagnostics` (persistence write health, recent job failures, contended resource lock keys, active sessions, effective `LOCAQL_*` environment variables).
- Real GoogleSQL query engine (`goccy/googlesqlite`) behind `jobs.query`/`jobs.insert`/`projects.queries`: genuine `WHERE`, projection, `JOIN`, aggregation, `ORDER BY`, `LIMIT`.
- Views and Materialized Views as real, live-resolved resources; nested `STRUCT`/`RECORD` and `ARRAY`/`REPEATED` schemas with BigQuery's real REST wire shape; `NUMERIC`/`BIGNUMERIC` exact-precision decimal types.
- Reproducible build pipeline: `Makefile` (`make build`, `make build-all` for a 5-platform cross-compile matrix), a multi-stage, multi-arch `Dockerfile` producing a minimal, non-root container image, version/commit/build-date injected via `-ldflags` and surfaced at `GET /_emulator/version`.
- Continuous integration (`.github/workflows/ci.yml`): build/vet/test/race on Linux, end-to-end console tests, native build/vet/test on Windows/macOS, a 5-platform cross-compile matrix, a dependency license scan, and a CycloneDX SBOM (`make sbom` locally; generated fresh per CI run rather than committed, since a static copy would go stale).
- Architecture Decision Records (`docs/adr/`) for the project's independent identity/licensing, the real GoogleSQL engine choice, and the sessions/transactions design.
- Real BigQuery `queryParameters`: `NAMED` (`@name`) and `POSITIONAL` (`?`) parameter binding for `jobs.query`/`jobs.insert`, including a genuinely `NULL`-valued typed parameter, for `STRING`/`INT64`/`FLOAT64`/`BOOL`/`BYTES`/`DATE`/`DATETIME`/`TIME`/`TIMESTAMP`. See [README: Query Parameters](README.md#query-parameters-named-and-positional).
- Console: a non-affiliation legal disclaimer, a real emulator version indicator, and a "Diagnostics" tab rendering `GET /_emulator/diagnostics`.
- Automated releases (`.github/workflows/release.yml`, triggered by pushing a `vX.Y.Z` tag): a real multi-arch (`linux/amd64`+`linux/arm64`) image published to `ghcr.io/lraigosov/locaql`, plus a GitHub Release with cross-compiled binaries for every `make build-all` platform and the SBOM attached. See "How releases are cut" below. This is the first release this pipeline has ever produced.

### Fixed
- `NOTICE` didn't attribute `google/googlesql` (formerly ZetaSQL), the real Apache-2.0-licensed Google engine the query layer actually transpiles and runs — only the MIT-licensed inspiration project was mentioned. Not a license conflict (Apache-2.0 and MIT are freely combinable, and this project is already Apache-2.0), but a real attribution gap, now corrected with the full dependency chain documented in `NOTICE`.
- The `Dockerfile` hardcoded `GOOS=linux` with no `GOARCH` at all, so it always built for the host's native architecture regardless of what was requested — "multi-arch" support didn't actually work. Fixed to consume Docker's `TARGETOS`/`TARGETARCH` build args properly.

### Known limitations
See [KNOWN-DIVERGENCES.md](KNOWN-DIVERGENCES.md) for the full, severity-classified list. Highlights: Storage Write API has no BUFFERED streams/`FlushRows`; Storage Read API is Avro-only with one stream per session, no `SplitReadStream`; a session transaction's atomicity never extends to real base tables; `GET /_emulator/metrics` is plain JSON, not Prometheus text exposition; the query engine only runs correctly on Linux (including WSL) — it traps at analyzer initialization on native Windows/macOS, root-caused but not yet fixable from LocaQL's side (see Blocking #3).

## Versioning and deprecation policy

LocaQL follows [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`, where `MAJOR` bumps on a breaking
REST/gRPC contract change or a removed capability, `MINOR` adds capability without breaking existing behavior, and
`PATCH` is a fix with no contract change. Because this is a compatibility-surface emulator, "breaking" is judged
against `capabilities/registry.yaml`'s documented contract, not internal implementation details.

A capability is never removed silently: dropping or narrowing one that was previously `supported`/`partial` requires
a `MAJOR` bump, an entry in this file's release notes explaining what changed and why, and (if a replacement exists)
a pointer to it. There is no fixed support window for old versions today — this is a single-maintainer, locally-run
tool, not a hosted service with an SLA — but every release remains tagged and buildable from source indefinitely, so
pinning to an older version is always a real option for a consumer who isn't ready to move.

## How releases are cut

Fully automated except for the one approval branch protection requires:

1. Every push to `main` runs `.github/workflows/prepare-release.yml`. It checks whether `CHANGELOG.md`'s `[Unreleased]` section has anything in it; if not, it stops there (nothing to release yet). If it does, it works out the SemVer bump from which `### ` subsections are present (`### Removed` or a `BREAKING` marker → major, `### Added` → minor, anything else → patch) against the latest `vX.Y.Z` tag, dates the section into `## [X.Y.Z] - YYYY-MM-DD`, opens a fresh empty `[Unreleased]` above it, and opens (or replaces, if the bump level changed since an earlier run) a PR (`release/vX.Y.Z` → `main`) with that single commit. No manual version number, no manual trigger.
2. Review and merge the PR. That's the only manual step in the entire process — `main`'s branch protection requires it regardless of who or what is proposing the change.
3. Merging triggers `.github/workflows/auto-tag-release.yml`, which tags that exact merge commit `vX.Y.Z` and calls `.github/workflows/release.yml` directly (not by relying on the tag push itself: a tag pushed with the default `GITHUB_TOKEN` doesn't start other workflow runs, a documented GitHub anti-recursion rule, so triggering it explicitly is what actually makes this work end to end).
4. `release.yml` validates (`build`/`vet`/`test`/`-race`), then in parallel: builds and pushes a real multi-arch (`linux/amd64`+`linux/arm64`) image to `ghcr.io/lraigosov/locaql` tagged `X.Y.Z`, `X.Y`, and `latest`; and cross-compiles binaries for linux/darwin/windows × amd64/arm64 (`make build-all`), generates a CycloneDX SBOM (`make sbom`), and publishes a GitHub Release for the tag with every platform archive and the SBOM attached.

Pushing a `vX.Y.Z` tag by hand (`git tag v0.9.0 && git push origin v0.9.0`) still works too — `release.yml` keeps its original `push: tags` trigger for anyone who wants to skip the PR entirely, e.g. for a one-off rebuild. The same `make build`/`make build-all`/`make docker-build` targets also still work locally for a manual/offline build (they read the version from `git describe` when `VERSION` isn't set explicitly).
