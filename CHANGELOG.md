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

### Known limitations
See [KNOWN-DIVERGENCES.md](KNOWN-DIVERGENCES.md) for the full, severity-classified list. Highlights: Storage Write API has no BUFFERED streams/`FlushRows`; Storage Read API is Avro-only with one stream per session, no `SplitReadStream`; a session transaction's atomicity never extends to real base tables; `GET /_emulator/metrics` is plain JSON, not Prometheus text exposition; a real, verified upstream bug in the query engine makes constructing a `DATE` from a string literal off by one day.

## How releases are cut

1. Update this file's `[Unreleased]` section into a new dated version section (`## [x.y.z] - YYYY-MM-DD`), summarizing what changed for someone *using* LocaQL — not the session-by-session build process (that stays in `devlog.md`, which isn't published).
2. Tag the release (`git tag vX.Y.Z`) — `make build`/`make build-all`/`make docker-build` read the version from `git describe` automatically when `VERSION` isn't set explicitly.
3. `make build-all` produces binaries for linux/darwin/windows × amd64/arm64 under `dist/<os>_<arch>/`; `make docker-build` produces a `locaql:X.Y.Z` and `locaql:latest` image.
