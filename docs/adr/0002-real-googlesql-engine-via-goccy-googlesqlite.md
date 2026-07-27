# 2. Real GoogleSQL execution via `goccy/googlesqlite`, not a hand-rolled interpreter

## Status

Accepted (Sesión 73; known limitation added Sesión 85).

## Context

Before Sesión 73, `jobs.query`/`jobs.insert`/`projects.queries` executed against a regex-matched full-table dump
with a fabricated fallback for anything the regex didn't recognize — no real `WHERE`, projection, `JOIN`,
aggregation, `ORDER BY`, or `LIMIT` semantics. Two paths were available to close that gap: (a) hand-write a GoogleSQL
subset parser/executor tailored to what LocaQL needed, growing it incrementally, or (b) adopt a real, independently
maintained GoogleSQL engine and materialize LocaQL's in-memory catalog into it per query.

## Decision

Adopt `github.com/goccy/googlesqlite` — a pure-Go (no cgo) `database/sql` driver that parses and analyzes real
GoogleSQL and executes against an embedded SQLite backend. Every table a query references is materialized into a
fresh in-memory engine instance per query (see `internal/server/sql_engine.go`); `WHERE`, projection, `JOIN`,
aggregation, `ORDER BY`, and `LIMIT` are therefore genuine GoogleSQL semantics, not simulated behavior, and an
invalid query or a reference to a nonexistent table fails explicitly through the engine's own analyzer rather than
falling back to a fabricated `200`.

## Consequences

- Correctness for the covered surface is inherited from a real, independently-tested analyzer rather than
  LocaQL's own parsing logic — a much smaller surface for LocaQL-side query bugs.
- LocaQL inherits the engine's own real limitations where they exist and aren't workaroundable from the outside:
  `CREATE TEMP TABLE`/`ROLLBACK` don't function reliably enough to build session support on directly (see
  `KNOWN-DIVERGENCES.md` Blocking #2 — worked around by implementing sessions/transactions in LocaQL's own catalog
  instead), and a real, now-understood platform limitation: the engine's analyzer is itself generated from a WASM
  build of ZetaSQL via `goccy/googlesqlwasm2go` (a 316k-line transpiled Go file, not a runtime WASM VM — no
  `wazero`/`wasmer`/`wasmtime` dependency exists in this project), and that transpiled code panics on native
  Windows/macOS while working correctly on Linux (see `KNOWN-DIVERGENCES.md` Blocking #3). LocaQL's own CI
  (`.github/workflows/ci.yml`) treats native Windows/macOS test execution as informational
  (`continue-on-error: true`) until that upstream gap is fixed or an alternative engine path is adopted.
- The local master plan's §40 explores a hypothetical future migration to DuckDB as an alternative engine; that
  remains unstarted exploratory planning, not a decision — this ADR only covers the engine actually in production
  use today.
