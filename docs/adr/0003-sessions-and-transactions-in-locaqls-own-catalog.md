# 3. Session temp tables and transactions live in LocaQL's own catalog, not the engine's

## Status

Accepted (Sesión 79).

## Context

Real BigQuery sessions support session-scoped temporary tables and `BEGIN`/`COMMIT`/`ROLLBACK TRANSACTION`. The
obvious implementation would pass these straight through to the embedded query engine's own equivalent SQL
statements (`CREATE TEMP TABLE`, `ROLLBACK`), since `goccy/googlesqlite` accepts that syntax. Before committing to
that approach, a disposable investigation spike (built and deleted within Sesión 79, findings preserved in
`KNOWN-DIVERGENCES.md`) verified it directly against the real engine.

## Decision

Do not pass session temp tables or transaction control through to the engine's native support. The spike found two
real, unconditional upstream limitations: `CREATE TEMP TABLE` registration does not survive past the single
`Exec`/`Query` call that created it (even pinned to one `*sql.Conn`, even inside an already-open transaction), and
`ROLLBACK`/`ROLLBACK TRANSACTION` fails unconditionally with `Statement not supported: RollbackStatement`. Neither
is workaroundable by constructing different SQL text — both are hard failures inside the engine's own analyzer.

Session temp tables and transactions are instead implemented entirely in LocaQL's own code: a session's temp tables
live in `sessionRecord.TempTables` (`internal/server/session_service.go`), materialized into a fresh engine instance
per query exactly like any other table when referenced as `` _SESSION.<table> ``, and `BEGIN`/`COMMIT`/`ROLLBACK
TRANSACTION` snapshot and restore that same in-memory map directly — never routed through the engine's own
transaction statements.

## Consequences

- Real, tested atomicity for a session's own temporary tables, without depending on engine behavior verified to be
  broken.
- Deliberately narrower than real BigQuery: a transaction's atomicity never extends to real base tables (an
  independent, pre-existing limitation — DML/DDL inside a query job doesn't mutate LocaQL's catalog at all, session
  or not), only the `` _SESSION.<table> `` qualified reference form is recognized (not a bare unqualified name), and
  only `CREATE TEMP TABLE <name> AS <select>` is supported (not the separate two-statement create-then-insert form).
- If a future `goccy/googlesqlite` release fixes `CREATE TEMP TABLE`/`ROLLBACK`, this ADR's premise should be
  re-verified before considering a migration back to passthrough — the workaround should not become permanent by
  default once its original justification stops being true.
