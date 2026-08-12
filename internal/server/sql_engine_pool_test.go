package server

import "testing"

func TestTableSignatureIsOrderIndependentAndVersionSensitive(t *testing.T) {
	a := []cacheableTableRef{{datasetID: "d", tableID: "a", version: 1}, {datasetID: "d", tableID: "b", version: 2}}
	b := []cacheableTableRef{{datasetID: "d", tableID: "b", version: 2}, {datasetID: "d", tableID: "a", version: 1}}
	if tableSignature(a) != tableSignature(b) {
		t.Fatalf("signature must not depend on ref order: %q vs %q", tableSignature(a), tableSignature(b))
	}
	bumped := []cacheableTableRef{{datasetID: "d", tableID: "a", version: 2}, {datasetID: "d", tableID: "b", version: 2}}
	if tableSignature(a) == tableSignature(bumped) {
		t.Fatalf("a version bump on any one table must change the signature")
	}
	if tableSignature(nil) != "" {
		t.Fatalf("an empty ref set must never produce a non-empty signature (that would make an uncacheable query look cacheable)")
	}
}

func TestAcquireSharedIsAMissThenAHitOnceSignatureIsSet(t *testing.T) {
	p := newSQLEnginePool()
	pe1, hit1, err := p.acquireShared("sig-a")
	if err != nil || hit1 {
		t.Fatalf("first acquireShared for a brand new signature must be a miss: hit=%v err=%v", hit1, err)
	}
	pe1.signature = "sig-a" // what materializeAllResolved does after a successful materialization
	p.releaseShared(pe1)

	pe2, hit2, err := p.acquireShared("sig-a")
	if err != nil || !hit2 {
		t.Fatalf("acquireShared for a signature an idle engine already carries must be a hit: hit=%v err=%v", hit2, err)
	}
	if pe2 != pe1 {
		t.Fatalf("a signature hit must return the exact same engine, not a new one")
	}
}

func TestAcquireSharedLetsConcurrentReadersJoinTheSameEngineBeforeEitherReleases(t *testing.T) {
	p := newSQLEnginePool()
	pe1, hit1, err := p.acquireShared("sig-a")
	if err != nil || hit1 {
		t.Fatalf("first acquire must be a miss: hit=%v err=%v", hit1, err)
	}
	pe1.signature = "sig-a"

	// A second concurrent reader wanting the identical table set must join
	// pe1 directly — refCount goes to 2 — without either reader releasing
	// first. This is the whole point of the shared cache: N concurrent
	// identical queries materialize once, not N times.
	pe2, hit2, err := p.acquireShared("sig-a")
	if err != nil {
		t.Fatalf("second acquireShared: %v", err)
	}
	if pe2 != pe1 {
		t.Fatalf("a concurrent reader wanting the same signature must get the same engine instance")
	}
	if !hit2 {
		t.Fatalf("joining an in-use engine that already carries the wanted signature must report a hit")
	}
	if pe1.refCount != 2 {
		t.Fatalf("expected refCount 2 while both readers hold the engine, got %d", pe1.refCount)
	}

	p.releaseShared(pe1)
	if pe1.refCount != 1 {
		t.Fatalf("expected refCount 1 after one of two readers releases, got %d", pe1.refCount)
	}
	p.releaseShared(pe2)
	if pe1.refCount != 0 {
		t.Fatalf("expected refCount 0 after both readers release, got %d", pe1.refCount)
	}
}

func TestAcquirePrivateNeverJoinsAnInUseEngine(t *testing.T) {
	p := newSQLEnginePool()
	pe1, err := p.acquirePrivate()
	if err != nil {
		t.Fatalf("acquirePrivate: %v", err)
	}
	// A second private lease requested while the first is still held must
	// never be handed the same connection — a mutating statement must have
	// an exclusively-owned connection, full stop.
	pe2, err := p.acquirePrivate()
	if err != nil {
		t.Fatalf("acquirePrivate (second): %v", err)
	}
	if pe1 == pe2 {
		t.Fatalf("two concurrently-held private leases must never be the same engine")
	}
	p.releasePrivate(pe1)
	p.releasePrivate(pe2)
}

func TestAcquireSharedRepurposesADirtyIdleEngineAndClearsItsOldSignature(t *testing.T) {
	p := newSQLEnginePool()
	old, hit, err := p.acquireShared("sig-old")
	if err != nil || hit {
		t.Fatalf("unexpected state acquiring sig-old: hit=%v err=%v", hit, err)
	}
	old.signature = "sig-old"
	old.tables["d.old"] = 1
	p.releaseShared(old)

	reused, hit, err := p.acquireShared("sig-new")
	if err != nil {
		t.Fatalf("acquireShared sig-new: %v", err)
	}
	if hit {
		t.Fatalf("a different signature than any idle engine carries must never be reported as a hit")
	}
	if reused != old {
		t.Fatalf("with only one idle engine available, acquireShared must repurpose it rather than opening a fresh one")
	}
	if reused.signature != "" {
		t.Fatalf("a repurposed engine's stale signature must be cleared until the caller re-materializes and re-tags it, got %q", reused.signature)
	}
	if _, stillTracked := reused.tables["d.old"]; stillTracked {
		t.Fatalf("a repurposed engine must have its previous materialized-table bookkeeping cleared")
	}
}

func TestReleasePrivateDiscardsAnEngineWhoseResetFails(t *testing.T) {
	p := newSQLEnginePool()
	pe, err := p.acquirePrivate()
	if err != nil {
		t.Fatalf("acquirePrivate: %v", err)
	}
	// A table tracked as materialized that doesn't actually exist on the
	// connection is normally harmless (DROP TABLE IF EXISTS) — poison the
	// underlying connection itself instead, so reset's DROP TABLE fails for
	// a reason unrelated to that IF EXISTS guard.
	_ = pe.db.Close()
	pe.tables["d.t"] = 1

	p.releasePrivate(pe)

	p.mu.Lock()
	stillPresent := false
	for _, e := range p.all {
		if e == pe {
			stillPresent = true
		}
	}
	count := len(p.all)
	p.mu.Unlock()
	if stillPresent {
		t.Fatalf("an engine whose reset failed must be discarded, not returned to idle")
	}
	if count != 0 {
		t.Fatalf("expected the pool to hold no engines after discarding the only one, got %d", count)
	}
}

// fillPoolWithSignedIdleEngines leaves exactly sqlEnginePoolMaxIdle distinct,
// idle, signed engines in p — the baseline
// TestEvictOverCapClosesExcessIdleEnginesPreferringBlankOnes pushes past the
// cap from. It must hold all sqlEnginePoolMaxIdle acquires open at once
// before releasing any of them: acquireShared happily repurposes any single
// idle engine for a new signature, so acquiring-then-releasing one signature
// at a time would just keep reusing the same one engine instead of creating
// sqlEnginePoolMaxIdle distinct ones.
func fillPoolWithSignedIdleEngines(t *testing.T, p *sqlEnginePool) {
	t.Helper()
	held := make([]*pooledEngine, 0, sqlEnginePoolMaxIdle)
	for i := 0; i < sqlEnginePoolMaxIdle; i++ {
		sig := "sig-" + string(rune('a'+i))
		pe, _, err := p.acquireShared(sig)
		if err != nil {
			t.Fatalf("acquireShared: %v", err)
		}
		pe.signature = sig
		held = append(held, pe)
	}
	for _, pe := range held {
		p.releaseShared(pe)
	}
}

// poolContainsEngine reports whether pe is still one of p's tracked engines.
func poolContainsEngine(p *sqlEnginePool, pe *pooledEngine) bool {
	for _, e := range p.all {
		if e == pe {
			return true
		}
	}
	return false
}

// poolHasBlankEngine reports whether any of p's tracked engines carries no
// signature ("").
func poolHasBlankEngine(p *sqlEnginePool) bool {
	for _, e := range p.all {
		if e.signature == "" {
			return true
		}
	}
	return false
}

func TestEvictOverCapClosesExcessIdleEnginesPreferringBlankOnes(t *testing.T) {
	p := newSQLEnginePool()
	fillPoolWithSignedIdleEngines(t, p)
	if got := len(p.all); got != sqlEnginePoolMaxIdle {
		t.Fatalf("expected exactly %d idle engines before crossing the cap, got %d", sqlEnginePoolMaxIdle, got)
	}

	blank, err := p.openFresh()
	if err != nil {
		t.Fatalf("openFresh: %v", err)
	}
	p.releasePrivate(blank) // crosses the cap by one; releasing must evict immediately

	if got := len(p.all); got != sqlEnginePoolMaxIdle {
		t.Fatalf("expected release to evict back down to %d immediately, got %d", sqlEnginePoolMaxIdle, got)
	}
	if poolContainsEngine(p, blank) {
		t.Fatalf("the blank engine must be the one evicted, since it is the cheapest idle connection to lose")
	}

	extra, hit, err := p.acquireShared("sig-extra")
	if err != nil || hit {
		t.Fatalf("acquireShared sig-extra: hit=%v err=%v", hit, err)
	}
	extra.signature = "sig-extra"
	p.releaseShared(extra) // crosses the cap by one; must evict exactly one

	if got := len(p.all); got != sqlEnginePoolMaxIdle {
		t.Fatalf("expected eviction to bring the pool back down to %d, got %d", sqlEnginePoolMaxIdle, got)
	}
	if poolHasBlankEngine(p) {
		t.Fatalf("the blank engine should have been evicted before any signed one")
	}
}
