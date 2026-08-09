package server

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestNumericPreservesExactDecimalPrecisionThroughLoadQueryExtract(t *testing.T) {
	s := newTestServer()
	loadNDJSONTable(t, s, "analytics", "numeric_amounts",
		[]map[string]any{
			{"name": "id", "type": "INT64"},
			{"name": "amount", "type": "NUMERIC"},
		},
		`{"id":1,"amount":"123.456789012"}`+"\n"+`{"id":2,"amount":"0.1"}`+"\n")

	out := tableDataRows(t, s, "analytics", "numeric_amounts")
	rows := out["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	f0 := rows[0].(map[string]any)["f"].([]any)
	if f0[1].(map[string]any)["v"] != "123.456789012" {
		t.Fatalf("expected exact decimal preserved in tabledata, got %v", f0[1])
	}

	// Real decimal arithmetic through the query engine: 0.1 + 0.2 must be
	// exactly 0.3, not the float64 rounding error (0.30000000000000004) a
	// FLOAT64 column would produce — proving NUMERIC is a real decimal
	// type in the engine, not silently downgraded to a float.
	code, qout := syncQuery(t, s, "SELECT CAST(0.1 AS NUMERIC) + CAST(0.2 AS NUMERIC) AS total")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	if got := cellsOf(t, qout, 0); got[0] != "0.3" {
		t.Fatalf("expected exact NUMERIC arithmetic 0.1+0.2=0.3, got %v", got)
	}

	// WHERE comparison over a real NUMERIC column.
	code, qout = syncQuery(t, s, "SELECT id FROM p1.analytics.numeric_amounts WHERE amount > 1 ORDER BY id")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	rows2 := qout["rows"].([]any)
	if len(rows2) != 1 || cellsOf(t, qout, 0)[0] != "1" {
		t.Fatalf("expected WHERE amount > 1 to match only id=1, got %v", rows2)
	}
}

func TestBigNumericPreservesFullPrecision(t *testing.T) {
	s := newTestServer()
	loadNDJSONTable(t, s, "analytics", "bignumeric_amounts",
		[]map[string]any{
			{"name": "id", "type": "INT64"},
			{"name": "amount", "type": "BIGNUMERIC"},
		},
		`{"id":1,"amount":"1.2345678901234567890123456789012345678"}`+"\n")

	code, qout := syncQuery(t, s, "SELECT amount FROM p1.analytics.bignumeric_amounts")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	if got := cellsOf(t, qout, 0); got[0] != "1.2345678901234567890123456789012345678" {
		t.Fatalf("expected full BIGNUMERIC precision preserved through the query engine, got %v", got)
	}
}

func TestDateDatetimeTimeTimestampRoundTripThroughTabledataUnchanged(t *testing.T) {
	s := newTestServer()
	loadNDJSONTable(t, s, "analytics", "core_datetime_types",
		[]map[string]any{
			{"name": "d", "type": "DATE"},
			{"name": "dt", "type": "DATETIME"},
			{"name": "tm", "type": "TIME"},
			{"name": "ts", "type": "TIMESTAMP"},
		},
		`{"d":"2026-07-25","dt":"2026-07-25T10:30:00","tm":"10:30:00","ts":"2026-07-25T10:30:00Z"}`+"\n")

	out := tableDataRows(t, s, "analytics", "core_datetime_types")
	f := out["rows"].([]any)[0].(map[string]any)["f"].([]any)
	if f[0].(map[string]any)["v"] != "2026-07-25" {
		t.Fatalf("expected DATE unchanged via tabledata.list, got %v", f[0])
	}
	if f[1].(map[string]any)["v"] != "2026-07-25T10:30:00" {
		t.Fatalf("expected DATETIME unchanged via tabledata.list, got %v", f[1])
	}
	if f[2].(map[string]any)["v"] != "10:30:00" {
		t.Fatalf("expected TIME unchanged via tabledata.list, got %v", f[2])
	}
	wantTimestamp := strconv.FormatInt(time.Date(2026, 7, 25, 10, 30, 0, 0, time.UTC).UnixMicro(), 10)
	if f[3].(map[string]any)["v"] != wantTimestamp {
		t.Fatalf("expected TIMESTAMP as REST epoch microseconds %s, got %v", wantTimestamp, f[3])
	}
}

// TestDateColumnToColumnComparisonWorksCorrectly verifies real DATE
// comparison and filtering through the query engine works when comparing
// one DATE column to another (self-join), sidestepping the known upstream
// literal-parsing bug documented in the test below.
func TestDateColumnToColumnComparisonWorksCorrectly(t *testing.T) {
	s := newTestServer()
	loadNDJSONTable(t, s, "analytics", "date_colcol_a",
		[]map[string]any{{"name": "id", "type": "INT64"}, {"name": "d", "type": "DATE"}},
		`{"id":1,"d":"2026-01-01"}`+"\n"+`{"id":2,"d":"2026-06-15"}`+"\n")
	loadNDJSONTable(t, s, "analytics", "date_colcol_b",
		[]map[string]any{{"name": "id", "type": "INT64"}, {"name": "d", "type": "DATE"}},
		`{"id":10,"d":"2026-06-15"}`+"\n")

	code, qout := syncQuery(t, s, "SELECT a.id, b.id FROM p1.analytics.date_colcol_a a JOIN p1.analytics.date_colcol_b b ON a.d = b.d")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	rows := qout["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 matching pair (id=2/id=10, both 2026-06-15), got %v", rows)
	}
	got := cellsOf(t, qout, 0)
	if got[0] != "2" || got[1] != "10" {
		t.Fatalf("expected [2 10], got %v", got)
	}
}

// TestDateStringLiteralParsesCorrectlyRegardlessOfHostTimezone guards
// against a real bug found via Sesión 85's CI work: constructing a DATE
// value from a string — either `DATE 'YYYY-MM-DD'` literal syntax or
// `CAST('YYYY-MM-DD' AS DATE)` — used to silently produce a date one
// calendar day earlier than the string says, but only on a host machine
// configured with a negative UTC offset (e.g. America/Bogota, UTC-5) —
// confirmed absent on a UTC-configured GitHub Actions runner while still
// reproducing locally, and confirmed fixed by forcing time.Local = time.UTC
// (see internal/server/server.go's init()), not a version-dependent
// property of the embedded query engine itself. This test pins the
// CORRECT, timezone-independent behavior: if it ever regresses, something
// reintroduced ambient-timezone sensitivity into DATE literal handling.
func TestDateStringLiteralParsesCorrectlyRegardlessOfHostTimezone(t *testing.T) {
	s := newTestServer()
	code, qout := syncQuery(t, s, "SELECT DATE '2026-01-01' AS d")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	got := cellsOf(t, qout, 0)[0]
	if got != "2026-01-01" {
		t.Fatalf("expected DATE '2026-01-01' to parse as 2026-01-01 regardless of host timezone, got %v", got)
	}
}
