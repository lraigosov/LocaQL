package server

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// sqlEnginePoolMaxIdle bounds how many warm, reusable embedded-engine
// connections are kept around once idle (refCount 0); this is a cache size,
// not a concurrency limit — acquiring always opens a fresh connection on
// demand when none are idle (see acquirePrivate/acquireShared for why a hard
// limit on connections actually in use would risk deadlocking on recursive
// view resolution). A small number is enough to eliminate almost all
// sql.Open churn under realistic concurrency, and now also to keep a few
// materialized large tables warm across repeated queries, without holding
// many idle WASM-backed engines in memory at once.
const sqlEnginePoolMaxIdle = 8

// pooledEngine is one reusable embedded GoogleSQL connection plus enough
// bookkeeping to make reuse safe. Two ways to hold one:
//
//   - Private (acquirePrivate/releasePrivate): the caller has exclusive use
//     of the connection and mutates it freely (running DDL/DML, dropping
//     tables). Every table materialized during a private lease is dropped
//     before the connection goes back to idle, exactly as this pool has
//     always worked — required for correctness (see reset's doc comment)
//     and for a mutating statement, which must never run against a
//     connection any concurrent reader could also be using.
//   - Shared (acquireShared/releaseShared): the caller gets a connection
//     whose currently-materialized tables exactly match a requested,
//     version-tagged signature (see tableSignature) — either because another
//     concurrent reader is already using it (refCount incremented, no
//     materialization needed at all) or because it was empty/repurposed and
//     the caller must still bring it in line with that signature. A shared
//     lease is strictly read-only from the caller's side: nothing but
//     read-only SELECTs (including views, which only ever read) may use it,
//     enforced by callers checking isMutatingOrSessionControlStatement
//     before ever asking for a shared lease.
//
// signature records which exact (dataset.table@version, ...) set this
// engine's `tables` currently represents, so a later shared acquire can find
// it again; "" means the connection's materialized-table content is not
// currently valid for any signature (freshly opened, or left dirty by a
// private lease that hasn't been reset yet).
type pooledEngine struct {
	db        *sql.DB
	schemas   map[string]bool
	tables    map[string]int // "datasetID.tableID" -> materialized version
	signature string
	refCount  int
}

// sqlEnginePool caches embedded-engine connections across queries instead of
// opening a brand new one (sql.Open("googlesqlite", ...)) every time, and —
// for read-only queries whose referenced tables have not changed since a
// previous query materialized them — lets concurrent queries share one
// already-materialized connection instead of every one of them
// re-materializing the same rows from scratch.
//
// Why the first part exists: opening a fresh embedded engine per query is
// expensive (each call constructs a new WASM-side GoogleSQL catalog) and,
// discovered while building this project's own benchmark suite (see
// docs/benchmarks.md), the underlying WASM bridge (goccy/go-googlesql) can
// eventually panic with a memory-corruption-shaped error after enough
// cumulative sql.Open calls over a process's lifetime. Reusing connections
// instead of opening a new one per query directly reduces how often that
// corruption trigger fires, on top of the latency win from skipping catalog
// construction on every warm reuse.
//
// Why the second part exists: profiling a query over a 50k-row table showed
// materializing that table into a fresh engine instance — done on every
// single query that references it, private-lease or not — was 74% of total
// per-query time (see docs/benchmarks.md). Table versions (tableRecord.Version,
// bumped by every mutation path) give an exact, already-existing signal for
// "has this table changed since it was last materialized", so a read-only
// query can safely skip re-materializing (or share a connection another
// concurrent read-only query already materialized) whenever that signal
// says nothing changed.
type sqlEnginePool struct {
	mu  sync.Mutex
	all []*pooledEngine
}

func newSQLEnginePool() *sqlEnginePool {
	return &sqlEnginePool{}
}

// tableSignature builds the canonical, order-independent cache key for one
// set of (datasetID, tableID, version) triples: sorted so the same table set
// always produces the same string regardless of the order a query's FROM/JOIN
// clauses happened to list them in.
func tableSignature(refs []cacheableTableRef) string {
	if len(refs) == 0 {
		return ""
	}
	parts := make([]string, len(refs))
	for i, r := range refs {
		parts[i] = r.datasetID + "." + r.tableID + "@" + strconv.Itoa(r.version)
	}
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

// cacheableTableRef is the minimal shape tableSignature needs — kept
// separate from datasetTableRef (which has no version) so sql_engine.go can
// build one without importing anything pool-specific.
type cacheableTableRef struct {
	datasetID string
	tableID   string
	version   int
}

// tagSignature records that pe now correctly materializes sig, once the
// caller has finished materializing it, under p.mu — every other read or
// write of pe.signature already happens under p.mu (see acquirePrivate's and
// acquireShared's reuse branch, which likewise clear it before unlocking).
// This must never be a bare `pe.signature = sig` field write from outside
// this file: pe is shared with any concurrent acquireShared call scanning
// p.all for an exact match, and Go gives no atomicity guarantee for a string
// field written on one goroutine and read on another without a shared lock —
// a caller landing between an unguarded write's two halves (pointer, length)
// could observe a torn signature that matches nothing real.
func (p *sqlEnginePool) tagSignature(pe *pooledEngine, sig string) {
	p.mu.Lock()
	pe.signature = sig
	p.mu.Unlock()
}

// acquire is the single entry point openMaterializedSQLDatabase uses: sig ==
// "" means the caller has nothing cacheable (mutating/session-scoped/pruned)
// and always gets a private, exclusively-owned connection; any other sig
// goes through acquireShared. shared reports which path was taken, so the
// caller knows whether to releaseShared or releasePrivate later.
func (p *sqlEnginePool) acquire(sig string) (pe *pooledEngine, hit bool, shared bool, err error) {
	if sig == "" {
		pe, err = p.acquirePrivate()
		return pe, false, false, err
	}
	pe, hit, err = p.acquireShared(sig)
	return pe, hit, true, err
}

// acquirePrivate returns a connection for the caller's exclusive use: fully
// reset (every previously materialized table dropped, signature cleared) if
// it was left dirty by an earlier private lease, or freshly opened if none
// were idle. Never returns a connection another goroutine might concurrently
// be reading — callers running anything other than a pure read-only query
// must use this, not acquireShared.
func (p *sqlEnginePool) acquirePrivate() (*pooledEngine, error) {
	p.mu.Lock()
	for _, pe := range p.all {
		if pe.refCount == 0 {
			pe.refCount = 1
			// Clear signature while still holding p.mu, not after: a
			// concurrent acquireShared's exact-signature-match scan runs
			// under this same lock and does not check refCount (by design,
			// so it can join an engine another reader is still using) — if
			// this engine's old signature were left in place until
			// safeResetPooledEngine ran below, a concurrent acquireShared
			// call landing in that window would wrongly "hit" an engine
			// that is about to have every one of its tables dropped out
			// from under it.
			pe.signature = ""
			p.mu.Unlock()
			if err := safeResetPooledEngine(pe); err != nil {
				p.discard(pe)
				return p.openFresh()
			}
			return pe, nil
		}
	}
	p.mu.Unlock()
	return p.openFresh()
}

// releasePrivate drops every table the caller's private lease materialized,
// clears its signature, and returns it to idle (or closes it outright if
// cleanup itself failed or panicked, or the idle cap is already full for
// this connection to usefully stay around). See safeResetPooledEngine for
// why a cleanup panic must not escape here.
func (p *sqlEnginePool) releasePrivate(pe *pooledEngine) {
	if pe == nil {
		return
	}
	if err := safeResetPooledEngine(pe); err != nil {
		p.mu.Lock()
		pe.refCount = 0
		p.mu.Unlock()
		p.discard(pe)
		return
	}
	p.mu.Lock()
	pe.refCount = 0
	p.evictOverCapLocked()
	p.mu.Unlock()
}

// acquireShared returns a connection whose materialized tables exactly
// match sig, and whether that was already the case (hit) or the caller must
// still reconcile the returned connection's content to sig itself (see
// reconcileSharedEngine in sql_engine.go): joining an existing lease (in use
// or idle) if one already carries that exact signature; otherwise an idle
// connection — preferring an already-blank one, but repurposing (dropping
// whatever it held) any idle one if nothing blank was available — or a
// freshly opened one if nothing was idle at all. sig must never be "" —
// callers with nothing cacheable (session-scoped tables, a pruned read, or a
// mutating statement) must use acquirePrivate instead.
func (p *sqlEnginePool) acquireShared(sig string) (pe *pooledEngine, hit bool, err error) {
	p.mu.Lock()
	for _, e := range p.all {
		if e.signature == sig {
			e.refCount++
			p.mu.Unlock()
			return e, true, nil
		}
	}
	var reuse *pooledEngine
	for _, e := range p.all {
		if e.refCount != 0 {
			continue
		}
		if e.signature == "" {
			reuse = e
			break
		}
		if reuse == nil {
			reuse = e
		}
	}
	var reuseWasDirty bool
	if reuse != nil {
		reuse.refCount = 1
		reuseWasDirty = reuse.signature != ""
		// Same reasoning as acquirePrivate: clear the old signature before
		// unlocking, not after safeResetPooledEngine runs below, so no
		// concurrent acquireShared call can match it in between.
		reuse.signature = ""
	}
	p.mu.Unlock()

	if reuse == nil {
		pe, err = p.openFresh()
		return pe, false, err
	}
	if reuseWasDirty {
		if resetErr := safeResetPooledEngine(reuse); resetErr != nil {
			p.discard(reuse)
			pe, err = p.openFresh()
			return pe, false, err
		}
	}
	return reuse, false, nil
}

// releaseShared decrements the lease count a prior acquireShared call
// established. While other concurrent readers still hold it (refCount > 0
// after the decrement), its signature and materialized content are left
// untouched. Once the last reader releases it, it stays idle *with its
// signature intact* — a later request for the same signature is then a free
// hit with no materialization at all — subject to the usual idle cap, under
// which the least useful (blank, then arbitrary) idle connections are
// closed first.
func (p *sqlEnginePool) releaseShared(pe *pooledEngine) {
	if pe == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if pe.refCount > 0 {
		pe.refCount--
	}
	if pe.refCount == 0 {
		p.evictOverCapLocked()
	}
}

// evictOverCapLocked closes idle (refCount == 0) connections, preferring
// blank ones first, until at most sqlEnginePoolMaxIdle idle connections
// remain. Must be called with p.mu held.
func (p *sqlEnginePool) evictOverCapLocked() {
	idle := make([]*pooledEngine, 0, len(p.all))
	for _, e := range p.all {
		if e.refCount == 0 {
			idle = append(idle, e)
		}
	}
	if len(idle) <= sqlEnginePoolMaxIdle {
		return
	}
	sort.SliceStable(idle, func(i, j int) bool {
		// Blank (no signature) connections are the cheapest to lose —
		// evict those before ones that would otherwise serve a future hit.
		return idle[i].signature == "" && idle[j].signature != ""
	})
	toEvict := idle[:len(idle)-sqlEnginePoolMaxIdle]
	evictSet := make(map[*pooledEngine]bool, len(toEvict))
	for _, e := range toEvict {
		evictSet[e] = true
	}
	kept := make([]*pooledEngine, 0, len(p.all))
	for _, e := range p.all {
		if evictSet[e] {
			_ = e.db.Close()
			continue
		}
		kept = append(kept, e)
	}
	p.all = kept
}

func (p *sqlEnginePool) openFresh() (*pooledEngine, error) {
	db, err := sql.Open("googlesqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open real SQL engine: %w", err)
	}
	// The embedded engine's own catalog (goccy/go-googlesql's
	// internal.Catalog) is shared by every driver-level connection opened
	// against this *sql.DB (that sharing is exactly what makes reuse across
	// queries safe at all — see this file's doc comment), but that shared
	// catalog is not itself safe for concurrent access: two goroutines
	// legitimately holding the same shared pooledEngine (acquireShared) and
	// calling Query/Exec at the same moment can race inside it, observed
	// directly as a spurious "no such table" once concurrent read-only
	// queries against a warm shared engine actually overlap in time (see
	// BenchmarkConcurrentSyncQueriesLargeTable). Capping this connection to
	// exactly one underlying driver connection makes database/sql itself
	// serialize every Query/Exec against it, so concurrent holders queue for
	// their turn instead of ever calling into the driver at the same time.
	// This does not defeat the point of sharing: the expensive part
	// (materializing rows into the engine) still happens once, not once per
	// concurrent reader — only the final SQL execution is serialized.
	db.SetMaxOpenConns(1)
	pe := &pooledEngine{db: db, schemas: map[string]bool{}, tables: map[string]int{}, refCount: 1}
	p.mu.Lock()
	p.all = append(p.all, pe)
	p.mu.Unlock()
	return pe, nil
}

// discard removes pe from the pool and closes it outright — for a
// connection already known unsafe to keep (cleanup failed) or never
// registered in p.all yet (openFresh's own error path).
func (p *sqlEnginePool) discard(pe *pooledEngine) {
	if pe == nil {
		return
	}
	p.mu.Lock()
	kept := make([]*pooledEngine, 0, len(p.all))
	for _, e := range p.all {
		if e != pe {
			kept = append(kept, e)
		}
	}
	p.all = kept
	p.mu.Unlock()
	_ = pe.db.Close()
}

func safeResetPooledEngine(pe *pooledEngine) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("sqlEnginePool: recovered panic while resetting a pooled engine, discarding it instead of reusing: %v", r)
			err = fmt.Errorf("panic during reset: %v", r)
		}
	}()
	return pe.reset()
}

// reset drops every table tracked in pe.tables and clears that set plus its
// signature. Schemas are intentionally left in place: the engine does not
// support DROP SCHEMA, and an empty, already-created schema is harmless to
// leave behind — CREATE TABLE against it next time works the same as if it
// were newly created.
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
	pe.tables = map[string]int{}
	pe.signature = ""
	return nil
}

// ensureSchema issues CREATE SCHEMA only the first time this connection sees
// a given dataset name, since the engine has no CREATE SCHEMA IF NOT EXISTS
// and schemas are never dropped between reuses.
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

// markTableMaterialized records that datasetID.tableID is now materialized
// on this connection at the given catalog version.
func (pe *pooledEngine) markTableMaterialized(datasetID, tableID string, version int) {
	pe.tables[datasetID+"."+tableID] = version
}

// materializedVersion reports the version datasetID.tableID is currently
// materialized at on this connection, if any.
func (pe *pooledEngine) materializedVersion(datasetID, tableID string) (int, bool) {
	v, ok := pe.tables[datasetID+"."+tableID]
	return v, ok
}

// dropMaterialized drops one table this connection previously materialized,
// e.g. to reconcile a repurposed shared connection down to exactly the
// tables a new signature wants.
func (pe *pooledEngine) dropMaterialized(datasetID, tableID string) error {
	key := datasetID + "." + tableID
	if _, ok := pe.tables[key]; !ok {
		return nil
	}
	if _, err := pe.db.Exec("DROP TABLE IF EXISTS " + qualifiedTableIdentFromKey(key)); err != nil {
		return err
	}
	delete(pe.tables, key)
	return nil
}

func qualifiedTableIdentFromKey(key string) string {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			return quoteIdent(key[:i]) + "." + quoteIdent(key[i+1:])
		}
	}
	return quoteIdent(key)
}
