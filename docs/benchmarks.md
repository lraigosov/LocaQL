# Benchmarks

This document has two parts: a reproducible performance comparison against
[`goccy/bigquery-emulator`](https://github.com/goccy/bigquery-emulator), and
a reliability finding this same benchmarking work surfaced — a real,
previously unknown crash condition, disclosed here in full rather than
quietly worked around, in keeping with this project's
[`KNOWN-DIVERGENCES.md`](../KNOWN-DIVERGENCES.md) philosophy of publishing
what's actually true rather than what's convenient.

## Methodology

[`cmd/locaql-bench`](../cmd/locaql-bench) is a small, dependency-free HTTP
client that drives identical REST workloads against **any**
BigQuery-REST-compatible server. The same binary produced every number
below for both LocaQL and goccy — not two separately hand-tuned scripts —
by pointing `--endpoint` at each server in turn. This is the same
methodology this project already applies to correctness (one divergence
catalog, evidence-based) applied to performance instead.

Workloads, run sequentially against a freshly started server for each
target:

- `sync_query_trivial` — `SELECT 1 AS one` (no catalog table touched).
- `sync_query_small_table` — `COUNT`/`SUM` with a `WHERE` over a 1,000-row table.
- `sync_query_join` — a `JOIN` between a 1,000-row and a 500-row table.
- `streaming_insert_single_row` — one `tabledata.insertAll` call per row.
- `streaming_insert_batch_100` — one `tabledata.insertAll` call per 100 rows.
- `concurrent_sync_queries` — the small-table `COUNT` query, fired from 6 concurrent workers.

Reproduce it yourself:

```bash
# terminal 1
go run ./cmd/locaql start --addr 127.0.0.1:19050

# terminal 2 (goccy, built separately: go install github.com/goccy/bigquery-emulator/cmd/bigquery-emulator@latest)
bigquery-emulator --project=bench --port=19051 --grpc-port=19061

# terminal 3
go run ./cmd/locaql-bench --endpoint http://127.0.0.1:19050 --project bench --label LocaQL --iterations 30
go run ./cmd/locaql-bench --endpoint http://127.0.0.1:19051 --project bench --label goccy   --iterations 30
```

### Environment for the results below

- CPU: Intel Core i9-10900K (6 cores allocated to the WSL2 VM)
- RAM: 31 GiB
- OS: Ubuntu 24.04 under WSL2 (Windows host)
- Go: 1.25.12
- LocaQL: commit `25dc33b` (`v0.9.2`-16 commits)
- `goccy/bigquery-emulator`: `v0.8.1`
- Both servers freshly started, single run each, `--iterations 30 --insert-iterations 300 --concurrency 6`, no other load on the machine.

Treat these as one honest data point, not a statistically rigorous benchmark suite — there is no warm-body CI runner running this repeatedly yet (see the master checklist for that as a follow-up). Numbers will vary by hardware; the *shape* of the differences (where below) is the more durable takeaway.

## Results

| Workload | LocaQL p50 | LocaQL mean | goccy p50 | goccy mean | Winner |
|---|---:|---:|---:|---:|---|
| `sync_query_trivial` | 201.4 ms | 201.6 ms | 175.0 ms | 185.5 ms | goccy |
| `sync_query_small_table` | 301.7 ms | 312.0 ms | 210.4 ms | 201.9 ms | goccy |
| `sync_query_join` | 402.1 ms | 402.3 ms | 299.4 ms | 306.3 ms | goccy |
| `streaming_insert_single_row` | 0.54 ms (1,856 rows/s) | 0.54 ms | 224.97 ms (4 rows/s) | 228.4 ms | **LocaQL, ~400x** |
| `streaming_insert_batch_100` | 1.69 ms (60,352 rows/s) | 1.66 ms | 283.5 ms (345 rows/s) | 289.6 ms | **LocaQL, ~170x** |
| `concurrent_sync_queries` (×6) | 1,304 ms | 1,304 ms | 1,433 ms | 1,352 ms | roughly tied |

Full JSON output for both runs is reproducible via `--json`; this table is the summary, not a substitute for running it yourself against your own hardware.

### Reading this honestly

**goccy is faster per query, by a real and consistent margin (~15–35%).** This project's own architecture pays for it deliberately: every query materializes its referenced tables into a fresh, isolated embedded-engine instance (`sql.Open("googlesqlite", ":memory:")` per query — see [Query Engine](../README.md#query-engine-real-googlesql-via-an-embedded-sqlite-backend)), which gives strong per-query isolation but means every single query pays a real, roughly constant ~200ms engine-startup cost on this hardware. goccy's architecture evidently amortizes this cost across queries (a persistent/reused engine instance). This is a genuine, currently-unaddressed cost of LocaQL's isolation model, not a fluke — see [Future work](#future-work) below.

**LocaQL's streaming inserts (`tabledata.insertAll`) are dramatically faster** — roughly 170–400x depending on batch size. This isn't a fluke either: LocaQL's `insertAll` is a direct, job-free catalog write (see [`streaming_inserts.go`](../internal/server/streaming_inserts.go)) that never touches the query engine at all, while every millisecond of goccy's ~225–290ms per call suggests its write path goes through the same per-call engine-startup cost its queries do. If your workload is insert-heavy (loading fixtures, seeding test data, streaming-ingest pipelines), this is a large, real, reproducible advantage.

**Concurrent query throughput is roughly comparable, and not good for either emulator** at this concurrency level — both show substantial serialization under 6 concurrent queries relative to their own single-query latency. Neither project currently has evidence of good horizontal query concurrency; this is a fair fight, not a LocaQL weakness relative to goccy specifically.

## A crash found by benchmarking, not hidden

Running this same benchmark repeatedly against one long-lived LocaQL process (roughly 250–350 cumulative queries in our testing — the exact count is not deterministic) reproduced a **real process crash**, on Linux/WSL — the platform this project documents as officially supported (unlike the already-known native Windows/macOS WASM trap, [`KNOWN-DIVERGENCES.md`](../KNOWN-DIVERGENCES.md) Blocking #2, which this is *not*).

**Root cause, as far as we could isolate it without modifying third-party code:** the embedded engine's WASM bridge (`goccy/go-googlesql` → `goccy/googlesqlite`) can panic with a memory-corruption-shaped `runtime error: slice bounds out of range` from *inside* `database/sql.Open` itself, after enough sequential/concurrent engine instances have been opened and closed over a long-running process's lifetime. We added `jobService.recoverJobPanic` ([`jobs_service.go`](../internal/server/jobs_service.go)) so a panic during query *execution* fails only that one job instead of taking the whole process down — verified with a dedicated regression test ([`panic_recovery_test.go`](../internal/server/panic_recovery_test.go)) and confirmed in this same benchmarking session: two such panics were caught and turned into ordinary failed jobs, and the server kept serving requests afterward.

**That fix is real but not complete.** Continuing the same load, the process eventually still crashed — this time from a panic inside a **Go runtime finalizer** (`runtime.runFinalizers` → `(*SimpleCatalog).free`), on a dedicated runtime-managed goroutine that no `defer`/`recover` in application code can reach, because it isn't part of any request's call stack. This is the same underlying WASM corruption, surfacing at Go's own garbage-collection cleanup step instead of at query-execution time. There is no mitigation available from LocaQL's own code for this specific failure mode — it would require a fix in `goccy/go-googlesql`'s WASM memory management itself.

We searched `goccy/googlesqlite` and `goccy/go-googlesql`'s issue trackers and found no existing report matching this signature; as far as we can tell, this is a new finding, not a rediscovery of documented behavior.

**Practical takeaway:** treat a single long-running LocaQL process the way you'd treat any process with an unbounded resource leak under this specific condition — expect it to need a restart under sustained, heavy query load, until this is fixed upstream. This does not affect correctness of any single request, and the `recoverJobPanic` improvement means most exposure now surfaces as one failed job rather than a full outage. It does mean LocaQL is not yet validated for a long-lived, high-QPS production-like deployment on any platform — a materially different statement than "runs fine on Linux/WSL," which remains true for normal development use.

## Future work

- **Reuse/pool embedded engine instances** instead of opening one per query — the single biggest lever on raw query latency, and would likely also reduce exposure to the crash above (fewer `sql.Open`/`Close` cycles over a process's lifetime). Non-trivial: the current per-query isolation is also what makes partition pruning, session temp tables and persistent DDL/DML atomicity straightforward to reason about; pooling would need to preserve those guarantees.
- **File an upstream issue** against `goccy/go-googlesql`/`goccy/googlesqlite` with the reproduction here — this is squarely a third-party bug, and upstream is best positioned to fix the WASM bridge's own memory management.
- **A soak-test CI job** that runs this same benchmark tool in a loop for an extended period, to get a real, repeatable crash-time distribution instead of the one anecdotal data point in this document.
- **Extend the comparison** to load/extract throughput and BigQuery Storage API read/write paths, which this first pass didn't cover.
