# 1. Independent project, Apache-2.0, no affiliation with Google

## Status

Accepted (established across Sesiones 59-62 of the project's build log; formalized here in Sesión 86).

## Context

LocaQL is a local, BigQuery-compatible development platform. Its early planning referenced
[`goccy/bigquery-emulator`](https://github.com/goccy/bigquery-emulator) (MIT License) as an optional source of
inspiration for scope, but LocaQL does not incorporate that project's source code and is not a fork of it. The
project needed an explicit identity, license, and non-affiliation stance before it could be presented publicly:
without one, users could reasonably assume either that LocaQL is a fork of `goccy/bigquery-emulator`, or that it is
somehow endorsed by Google/BigQuery, neither of which is true.

## Decision

- LocaQL is licensed under the **Apache License, Version 2.0** (see `LICENSE`), not MIT — a deliberate choice
  distinct from the MIT-licensed project that inspired its early scope.
- `NOTICE` states the project's origin, the Apache §4(d) attribution requirement for derivative works, and explicit
  non-affiliation with Google LLC / Google Cloud / BigQuery.
- No per-file copyright headers are added: a single `LICENSE` file at the repository root is legally sufficient for
  Apache-2.0, and stamping headers into every source file was judged to add maintenance overhead without a
  corresponding legal requirement for a single-maintainer project (revisit if the contributor base grows enough that
  per-file provenance actually matters).
- `goccy/bigquery-emulator` is registered as a **read-only** `upstream` git remote (fetch-only; its push URL is set
  to a literal no-op) purely as an optional reference source — never merged automatically, never treated as roadmap
  authority. See §41.6 of the local master plan for the full upstream policy (cherry-pick with attribution, license
  and compatibility review per import, never rewriting published branches).

## Consequences

- Anyone auditing LocaQL's licensing sees one unambiguous answer (Apache-2.0) instead of having to reconcile a
  mismatched `LICENSE`/`NOTICE` pair or guess at a fork relationship that doesn't exist.
- Bringing in any code, idea, or fix referenced from `goccy/bigquery-emulator` (or any other MIT/Apache-compatible
  project) in the future requires an explicit license and compatibility check before import, not an assumption that
  "the license was fine over there."
- The `upstream` remote is a local git-config convenience (each clone that wants it must add it itself); it is not
  and cannot be part of the repository's own tracked history.
- This ADR covers non-affiliation and top-level licensing; it does not by itself cover attribution for specific
  third-party code actually embedded in LocaQL's own binaries. See ADR 0002 for a real gap found and fixed there:
  the query engine transpiles Google's own Apache-2.0-licensed `google/googlesql` source, which needed its own
  `NOTICE` entry distinct from the `goccy/bigquery-emulator` inspiration-only note above.
