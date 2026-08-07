package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// sessionDatasetName is the special dataset name a query uses to reference a
// session-scoped temporary table, e.g. `_SESSION.my_temp_table` — mirrors
// real BigQuery's session anonymous dataset name. Only this qualified form
// is supported (see the AST table-path extraction in sql_engine.go, which
// requires a dot-qualified reference); an unqualified bare name is not
// resolved against the session, a narrower scope than real BigQuery.
const sessionDatasetName = "_SESSION"

// sessionTempTable is one _SESSION-scoped temporary table's schema+rows,
// stored in LocaQL's own catalog rather than the query engine's native
// CREATE TEMP TABLE. Verified empirically (a disposable local-support spike,
// deleted after use) that goccy/googlesqlite v0.3.1's CREATE TEMP TABLE
// registration does not survive past the single Exec/Query call that
// created it — not even pinned to one *sql.Conn, not even wrapped in an
// explicit uncommitted transaction — which makes it unusable for state that
// must persist across the separate HTTP requests a real BigQuery session
// spans. This type plus sessionRecord's snapshot/restore below is LocaQL's
// own real implementation of that persistence and of transactional
// atomicity, not a passthrough to the query engine's (also verified
// non-working, see ROLLBACK below) transaction statements.
type sessionTempTable struct {
	Fields []tableField
	Rows   [][]string
}

func cloneSessionTempTable(t *sessionTempTable) *sessionTempTable {
	if t == nil {
		return nil
	}
	rows := make([][]string, len(t.Rows))
	for i, r := range t.Rows {
		rows[i] = append([]string(nil), r...)
	}
	return &sessionTempTable{Fields: cloneTableFields(t.Fields), Rows: rows}
}

func cloneSessionTempTables(m map[string]*sessionTempTable) map[string]*sessionTempTable {
	out := make(map[string]*sessionTempTable, len(m))
	for k, v := range m {
		out[k] = cloneSessionTempTable(v)
	}
	return out
}

// sessionRecord is one BigQuery-style session. TempTables is LocaQL's own
// session-scoped catalog (see sessionTempTable for why); txSnapshot is a
// clone of TempTables taken at BEGIN and restored on ROLLBACK / discarded on
// COMMIT — real, verified atomicity for session temp tables, implemented by
// LocaQL itself rather than passed through to the query engine, since the
// engine's own ROLLBACK statement was verified (same spike) to fail
// unconditionally with "Statement not supported: RollbackStatement".
// DML/DDL against real (non-session) base tables still does not mutate
// LocaQL's persistent catalog at all, matching the existing, independent
// "DML/DDL inside a query job doesn't mutate the catalog" limitation — a
// transaction here only ever governs the session's own temp tables.
type sessionRecord struct {
	mu         sync.Mutex
	ProjectID  string
	SessionID  string
	UserEmail  string
	CreatedAt  time.Time
	LastUsedAt time.Time
	TempTables map[string]*sessionTempTable
	txSnapshot map[string]*sessionTempTable // nil when no transaction is open
}

func (r *sessionRecord) inTransaction() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.txSnapshot != nil
}

func (r *sessionRecord) beginTransaction() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.txSnapshot != nil {
		return fmt.Errorf("a transaction is already active in this session")
	}
	r.txSnapshot = cloneSessionTempTables(r.TempTables)
	return nil
}

func (r *sessionRecord) commitTransaction() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.txSnapshot == nil {
		return fmt.Errorf("no active transaction in this session")
	}
	r.txSnapshot = nil
	return nil
}

func (r *sessionRecord) rollbackTransaction() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.txSnapshot == nil {
		return fmt.Errorf("no active transaction in this session")
	}
	r.TempTables = r.txSnapshot
	r.txSnapshot = nil
	return nil
}

func (r *sessionRecord) getTempTable(name string) (*sessionTempTable, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.TempTables[name]
	return t, ok
}

func (r *sessionRecord) setTempTable(name string, t *sessionTempTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.TempTables[name] = t
}

// sessionService is the sibling of jobService/tableService/etc. for session
// lifecycle: keyed by project+sessionID, with the same "no background sweep
// goroutine, lazily purge an idle-expired entry the next time it's touched"
// convention already established for table expiration (tableService.now).
type sessionService struct {
	mu          sync.RWMutex
	sessions    map[string]*sessionRecord
	counter     int64
	now         func() time.Time
	idleTimeout time.Duration
}

// newSessionService defaults the idle timeout to 24h, matching real
// BigQuery's own session default, overridable via
// LOCAQL_SESSION_IDLE_TIMEOUT_SECONDS for local testing without a real wait.
func newSessionService() *sessionService {
	idleTimeout := 24 * time.Hour
	if raw := strings.TrimSpace(os.Getenv("LOCAQL_SESSION_IDLE_TIMEOUT_SECONDS")); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
			idleTimeout = time.Duration(secs) * time.Second
		}
	}
	return &sessionService{
		sessions:    make(map[string]*sessionRecord),
		now:         time.Now,
		idleTimeout: idleTimeout,
	}
}

func sessionKey(projectID, sessionID string) string {
	return projectID + "\x00" + sessionID
}

func newSessionID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "session_" + hex.EncodeToString(buf), nil
}

func (s *sessionService) create(projectID, userEmail string) (*sessionRecord, error) {
	id, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("generate session id: %w", err)
	}
	now := s.now()
	rec := &sessionRecord{
		ProjectID:  projectID,
		SessionID:  id,
		UserEmail:  userEmail,
		CreatedAt:  now,
		LastUsedAt: now,
		TempTables: make(map[string]*sessionTempTable),
	}
	s.mu.Lock()
	s.sessions[sessionKey(projectID, id)] = rec
	s.mu.Unlock()
	return rec, nil
}

// get looks up a session, lazily purging it first if it has gone idle past
// idleTimeout since its last use; otherwise touches LastUsedAt.
func (s *sessionService) get(projectID, sessionID string) (*sessionRecord, bool) {
	key := sessionKey(projectID, sessionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.sessions[key]
	if !ok {
		return nil, false
	}
	if s.now().Sub(rec.LastUsedAt) > s.idleTimeout {
		delete(s.sessions, key)
		return nil, false
	}
	rec.LastUsedAt = s.now()
	return rec, true
}

// countAll reports the number of non-expired sessions across every project,
// for GET /_emulator/health and /_emulator/metrics — lazily purging any
// idle-expired ones it encounters along the way (same convention as get).
func (s *sessionService) countAll() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for key, rec := range s.sessions {
		if s.now().Sub(rec.LastUsedAt) > s.idleTimeout {
			delete(s.sessions, key)
			continue
		}
		count++
	}
	return count
}

// list returns every non-expired session for a project, lazily purging any
// idle-expired ones it encounters along the way (same convention as get).
func (s *sessionService) list(projectID string) []*sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*sessionRecord
	for key, rec := range s.sessions {
		if rec.ProjectID != projectID {
			continue
		}
		if s.now().Sub(rec.LastUsedAt) > s.idleTimeout {
			delete(s.sessions, key)
			continue
		}
		out = append(out, rec)
	}
	return out
}

// connectionProperty mirrors one entry of BigQuery's REST
// configuration.query.connectionProperties / jobs.query connectionProperties
// array. Only the "session_id" key is recognized (see resolveSessionID);
// any other key is accepted and ignored rather than rejected, matching the
// project's general lenient-JSON-decoding convention.
type connectionProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// resolveSessionID implements the two ways a query request attaches to a
// BigQuery-style session: continuing an existing one via
// connectionProperties=[{key:"session_id", value:"..."}] (fails explicitly,
// not silently, if that session doesn't exist or has expired), or minting a
// brand new one via createSession=true. Neither set → "" (no session), the
// ordinary stateless query path.
func (s *Server) resolveSessionID(projectID, userEmail string, createSession bool, props []connectionProperty) (string, error) {
	for _, p := range props {
		if !strings.EqualFold(strings.TrimSpace(p.Key), "session_id") {
			continue
		}
		sessionID := strings.TrimSpace(p.Value)
		if sessionID == "" {
			continue
		}
		if _, ok := s.sessions.get(projectID, sessionID); !ok {
			return "", fmt.Errorf("session not found or expired: %s", sessionID)
		}
		return sessionID, nil
	}
	if createSession {
		rec, err := s.sessions.create(projectID, userEmail)
		if err != nil {
			return "", err
		}
		return rec.SessionID, nil
	}
	return "", nil
}

var (
	sessionBeginPattern           = regexp.MustCompile(`(?i)^(?:BEGIN|START)\s*(?:TRANSACTION)?$`)
	sessionCommitPattern          = regexp.MustCompile(`(?i)^COMMIT\s*(?:TRANSACTION)?$`)
	sessionRollbackPattern        = regexp.MustCompile(`(?i)^ROLLBACK\s*(?:TRANSACTION)?$`)
	sessionCreateTempTablePattern = regexp.MustCompile("(?is)^CREATE\\s+(?:TEMP|TEMPORARY)\\s+TABLE\\s+`?([A-Za-z0-9_]+)`?\\s+AS\\s+(.+)$")
)

func sessionControlResultSchema() []tableField {
	return []tableField{{Name: "statement_result", Type: "STRING"}}
}

func sessionControlResultRows(msg string) [][]string {
	return [][]string{{msg}}
}

// executeSessionControlStatement recognizes the small set of session-scoped
// statements this bloque supports (BEGIN/COMMIT/ROLLBACK TRANSACTION and
// CREATE TEMP TABLE ... AS <select>) and executes them for real against the
// session's own temp-table catalog. handled=false means stmt is an ordinary
// query the caller should route through the normal engine path instead (with
// rec passed along so `_SESSION.<table>` references resolve); rec is always
// returned non-nil alongside handled=false so the caller doesn't need a
// second session lookup.
func (s *Server) executeSessionControlStatement(projectID, sessionID, stmt string) (rec *sessionRecord, handled bool, schema []tableField, rows [][]string, err error) {
	rec, ok := s.sessions.get(projectID, sessionID)
	if !ok {
		return nil, true, nil, nil, fmt.Errorf("session not found or expired: %s", sessionID)
	}

	switch {
	case sessionBeginPattern.MatchString(stmt):
		if err := rec.beginTransaction(); err != nil {
			return rec, true, nil, nil, err
		}
		return rec, true, sessionControlResultSchema(), sessionControlResultRows("BEGIN TRANSACTION"), nil
	case sessionCommitPattern.MatchString(stmt):
		if err := rec.commitTransaction(); err != nil {
			return rec, true, nil, nil, err
		}
		return rec, true, sessionControlResultSchema(), sessionControlResultRows("COMMIT TRANSACTION"), nil
	case sessionRollbackPattern.MatchString(stmt):
		if err := rec.rollbackTransaction(); err != nil {
			return rec, true, nil, nil, err
		}
		return rec, true, sessionControlResultSchema(), sessionControlResultRows("ROLLBACK TRANSACTION"), nil
	}

	if m := sessionCreateTempTablePattern.FindStringSubmatch(stmt); m != nil {
		name := m[1]
		innerSchema, innerRows, err := s.executeRealSQLQueryVisiting(projectID, m[2], map[string]bool{}, rec)
		if err != nil {
			return rec, true, nil, nil, fmt.Errorf("create temp table %s: %w", name, err)
		}
		rec.setTempTable(name, &sessionTempTable{Fields: innerSchema, Rows: innerRows})
		return rec, true, sessionControlResultSchema(), sessionControlResultRows("CREATE TEMP TABLE " + name), nil
	}

	return rec, false, nil, nil, nil
}
