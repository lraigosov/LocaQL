package server

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
)

// sqlEnginePoolMaxIdle bounds how many warm, reusable embedded-engine
// connections are kept around; this is a cache size, not a concurrency
// limit — acquire() always creates a fresh connection on demand when the
// idle cache is empty (see its doc comment for why a hard limit here would
// risk deadlocking on recursive view resolution). A small number is enough
// to eliminate almost all sql.Open churn under realistic concurrency
// without holding many idle WASM-backed engines in memory at once.
const sqlEnginePoolMaxIdle = 8

// pooledEngine is one reusable embedded GoogleSQL connection plus enough
// bookkeeping to make reuse safe: schemas, once created, are never dropped
// (the engine does not support DROP SCHEMA — confirmed directly, see
// devlog/this file's history), but every table materialized for one query
// is dropped before the connection goes back in the pool, so a later,
// unrelated query on the same connection never inherits stale data. This
// matters well beyond general hygiene: wildcard table resolution
// (expandWildcardTableRefs, goccy/googlesqlite's own internal/wildcard_table.go)
// matches by scanning every table name the embedded engine's catalog
// currently holds, so a leftover table from a previous, unrelated query
// could otherwise silently join a later wildcard union it was never meant
// to be part of.
type pooledEngine struct {
	db      *sql.DB
	schemas map[string]bool
	tables  map[string]bool // "datasetID.tableID" currently materialized
}

// sqlEnginePool caches embedded-engine connections across queries instead
// of opening a brand new one (sql.Open("googlesqlite", ...)) every time.
//
// Why this exists: opening a fresh embedded engine per query is expensive
// (each call constructs a new WASM-side GoogleSQL catalog) and, discovered
// while building this project's own benchmark suite (see
// docs/benchmarks.md), the underlying WASM bridge (goccy/go-googlesql) can
// eventually panic with a memory-corruption-shaped error after enough
// cumulative sql.Open calls over a process's lifetime — reproduced on
// Linux/WSL, not just the already-documented native Windows/macOS trap
// (KNOWN-DIVERGENCES.md Blocking #2). Reusing connections instead of
// opening a new one per query directly reduces how often that corruption
// trigger fires, on top of the latency win from skipping catalog
// construction on every warm reuse.
type sqlEnginePool struct {
	mu   sync.Mutex
	idle []*pooledEngine
}

func newSQLEnginePool() *sqlEnginePool {
	return &sqlEnginePool{}
}

// acquire returns a warm connection from the idle cache if one is
// available, or opens a fresh one otherwise. It deliberately never blocks
// and never enforces a hard cap on connections actually in use: query
// execution can recurse (a view selecting from another view, each
// resolved via its own call to openMaterializedSQLDatabase while the
// outer call's connection is still checked out), so a hard limit sized to
// the job-level concurrency bound (jobService.runSlots) could still
// deadlock against that recursion. Capping the *idle* cache (see release)
// is enough to get the reuse benefit without that risk.
func (p *sqlEnginePool) acquire() (*pooledEngine, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		pe := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return pe, nil
	}
	p.mu.Unlock()

	db, err := sql.Open("googlesqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open real SQL engine: %w", err)
	}
	return &pooledEngine{db: db, schemas: map[string]bool{}, tables: map[string]bool{}}, nil
}

// release drops every table this connection materialized since it was last
// acquired, then returns it to the idle cache (or closes it outright if the
// cache is already full, or if cleanup itself failed or panicked — a
// connection that can't be cleaned isn't safe to hand to a later, unrelated
// query). The recover here specifically guards against reset() reaching
// back into the same fragile WASM bridge that caused this pool to exist in
// the first place: a cleanup call is exactly the kind of extra sql.Exec a
// borderline-corrupted connection might panic on, and that panic must not
// escape release() itself (its caller is typically a bare defer, with
// nothing further up the stack positioned to recover it cleanly).
func (p *sqlEnginePool) release(pe *pooledEngine) {
	if pe == nil {
		return
	}
	if !safeResetPooledEngine(pe) {
		_ = pe.db.Close()
		return
	}

	p.mu.Lock()
	if len(p.idle) >= sqlEnginePoolMaxIdle {
		p.mu.Unlock()
		_ = pe.db.Close()
		return
	}
	p.idle = append(p.idle, pe)
	p.mu.Unlock()
}

// discard closes a connection outright without attempting cleanup or reuse
// — for the rare case a caller already knows the connection is unsafe to
// hand back (e.g. a partition-filter check failed before anything was even
// materialized, so there is nothing to clean up and no reason to keep it
// warm over just closing it).
func (p *sqlEnginePool) discard(pe *pooledEngine) {
	if pe == nil {
		return
	}
	_ = pe.db.Close()
}

func safeResetPooledEngine(pe *pooledEngine) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("sqlEnginePool: recovered panic while resetting a pooled engine, discarding it instead of reusing: %v", r)
			ok = false
		}
	}()
	return pe.reset() == nil
}

// reset drops every table tracked in pe.tables and clears that set.
// Schemas are intentionally left in place (see the type's doc comment):
// the engine does not support DROP SCHEMA, and an empty, already-created
// schema is harmless to leave behind — CREATE TABLE against it next time
// works the same as if it were newly created.
func (pe *pooledEngine) reset() error {
	for key := range pe.tables {
		// IF EXISTS: a tracked ref is not always actually materialized on
		// the connection (e.g. a persistent-DDL target still fails
		// validation before its CREATE TABLE ever runs) — that must not
		// make cleanup itself fail and discard an otherwise-healthy
		// connection.
		if _, err := pe.db.Exec("DROP TABLE IF EXISTS " + qualifiedTableIdentFromKey(key)); err != nil {
			return fmt.Errorf("drop table %s while resetting pooled engine: %w", key, err)
		}
	}
	pe.tables = map[string]bool{}
	return nil
}

// ensureSchema issues CREATE SCHEMA only the first time this connection
// sees a given dataset name, since the engine has no CREATE SCHEMA IF NOT
// EXISTS and schemas are never dropped between reuses.
func (pe *pooledEngine) ensureSchema(datasetID string) error {
	if pe.schemas[datasetID] {
		return nil
	}
	if _, err := pe.db.Exec("CREATE SCHEMA " + quoteIdent(datasetID)); err != nil {
		return err
	}
	pe.schemas[datasetID] = true
	return nil
}

// markTableMaterialized records that datasetID.tableID now exists on this
// connection, so release() drops it before the connection is reused.
func (pe *pooledEngine) markTableMaterialized(datasetID, tableID string) {
	pe.tables[datasetID+"."+tableID] = true
}

func qualifiedTableIdentFromKey(key string) string {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			return quoteIdent(key[:i]) + "." + quoteIdent(key[i+1:])
		}
	}
	return quoteIdent(key)
}
