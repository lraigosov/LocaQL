# Changelog

All notable user-facing changes to LocaQL are documented here, in the style of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This file is the curated, release-facing summary; `devlog.md` (gitignored, not part of the published repo) is the full internal session-by-session build log this project has kept throughout development — this file distills what actually matters to someone using LocaQL, not the process of building it.

## [Unreleased]

### Added
- BigQuery Storage API (gRPC): `CreateReadSession`/`ReadRows` (Avro framing) and `CreateWriteStream`/`AppendRows`/`GetWriteStream`/`FinalizeWriteStream`/`BatchCommitWriteStreams` (protobuf rows via real runtime reflection), on a separate listener (`--storage-grpc-addr`, default `:9060`). See [README: BigQuery Storage API](README.md#bigquery-storage-api-real-grpc-read-sessions-avro-and-write-streams-protobuf).
- Sessions and multi-statement transactions: `createSession`/`connectionProperties`, session-scoped `_SESSION.<table>` temp tables, real `BEGIN`/`COMMIT`/`ROLLBACK TRANSACTION` atomicity, `INFORMATION_SCHEMA.SESSIONS_BY_USER`.
- Workspace portability over REST and the console: `/_emulator/workspace/validate|plan|diff|apply`, mirroring the `locaql workspace` CLI subcommands.
- Operational observability: structured JSON request logging, `GET /_emulator/metrics` (request/latency/job counters, live queue/backpressure gauges), extended `subsystems` detail on `GET /_emulator/health`/`readiness`.
- Guided troubleshooting: `GET /_emulator/diagnostics` (persistence write health, recent job failures, contended resource lock keys, active sessions, effective `LOCAQL_*` environment variables).
- Real GoogleSQL query engine (`goccy/googlesqlite`) behind `jobs.query`/`jobs.insert`/`projects.queries`: genuine `WHERE`, projection, `JOIN`, aggregation, `ORDER BY`, `LIMIT`.
- Views and Materialized Views as real, live-resolved resources; nested `STRUCT`/`RECORD` and `ARRAY`/`REPEATED` schemas with BigQuery's real REST wire shape; `NUMERIC`/`BIGNUMERIC` exact-precision decimal types.
- Reproducible build pipeline: `Makefile` (`make build`, `make build-all` for a 5-platform cross-compile matrix), a multi-stage `Dockerfile` producing a minimal, non-root container image, version/commit/build-date injected via `-ldflags` and surfaced at `GET /_emulator/version`.
- Continuous integration (`.github/workflows/ci.yml`): build/vet/test/race on Linux, end-to-end console tests, native build/vet/test on Windows/macOS, a 5-platform cross-compile matrix, a dependency license scan, and a CycloneDX SBOM (`make sbom` locally; generated fresh per CI run rather than committed, since a static copy would go stale).
- Architecture Decision Records (`docs/adr/`) for the project's independent identity/licensing, the real GoogleSQL engine choice, and the sessions/transactions design.

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

1. Update this file's `[Unreleased]` section into a new dated version section (`## [x.y.z] - YYYY-MM-DD`), summarizing what changed for someone *using* LocaQL — not the session-by-session build process (that stays in `devlog.md`, which isn't published).
2. Tag the release (`git tag vX.Y.Z`) — `make build`/`make build-all`/`make docker-build` read the version from `git describe` automatically when `VERSION` isn't set explicitly.
3. `make build-all` produces binaries for linux/darwin/windows × amd64/arm64 under `dist/<os>_<arch>/`; `make docker-build` produces a `locaql:X.Y.Z` and `locaql:latest` image.
