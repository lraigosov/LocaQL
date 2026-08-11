package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// persistentSQLStatement is the small amount of statement metadata LocaQL
// needs around googlesqlite's analyzer/executor: which catalog resource must
// be locked and committed, plus the BigQuery statementType/statistics shape.
// SQL expressions and mutation semantics remain the embedded engine's job.
type persistentSQLStatement struct {
	statementType string
	target        tableReference
	dml           bool
	create        bool
	drop          bool
	ifExists      bool
	ifNotExists   bool
	orReplace     bool
}

type persistentSQLResult struct {
	schema          []tableField
	rows            [][]string
	statementType   string
	dmlAffectedRows int64
	processedBytes  int64
}

// persistentTargetExpression matches a dataset.table or project.dataset.table
// target, accepting three quoting styles real tools emit: no backticks,
// a single pair around the whole dotted identifier, and a pair around each
// segment individually — a backtick around each of project, dataset and
// table on its own, what dbt-bigquery's
// default table/incremental DDL macros generate). Backticks are stripped per
// segment in parsePersistentTarget rather than assumed to wrap the whole
// match.
const persistentTargetExpression = "(`?[A-Za-z0-9_-]+`?(?:\\.`?[A-Za-z0-9_-]+`?){1,2})"

var (
	insertTargetPattern            = regexp.MustCompile("(?is)^\\s*INSERT\\s+(?:INTO\\s+)?" + persistentTargetExpression + "(?:\\s|\\()")
	updateTargetPattern            = regexp.MustCompile("(?is)^\\s*UPDATE\\s+" + persistentTargetExpression + "(?:\\s|$)")
	deleteTargetPattern            = regexp.MustCompile("(?is)^\\s*DELETE\\s+(?:FROM\\s+)?" + persistentTargetExpression + "(?:\\s|$)")
	mergeTargetPattern             = regexp.MustCompile("(?is)^\\s*MERGE\\s+(?:INTO\\s+)?" + persistentTargetExpression + "(?:\\s|$)")
	truncateTargetPattern          = regexp.MustCompile("(?is)^\\s*TRUNCATE\\s+TABLE\\s+" + persistentTargetExpression + "(?:\\s|$)")
	createTargetPattern            = regexp.MustCompile("(?is)^\\s*CREATE\\s+(OR\\s+REPLACE\\s+)?TABLE\\s+(IF\\s+NOT\\s+EXISTS\\s+)?" + persistentTargetExpression + "(?:\\s|\\(|$)")
	dropTargetPattern              = regexp.MustCompile("(?is)^\\s*DROP\\s+TABLE\\s+(IF\\s+EXISTS\\s+)?" + persistentTargetExpression + "\\s*$")
	createViewTargetPattern        = regexp.MustCompile("(?is)^\\s*CREATE\\s+(OR\\s+REPLACE\\s+)?(MATERIALIZED\\s+)?VIEW\\s+(IF\\s+NOT\\s+EXISTS\\s+)?" + persistentTargetExpression + "\\s*")
	dropViewTargetPattern          = regexp.MustCompile("(?is)^\\s*DROP\\s+(MATERIALIZED\\s+)?VIEW\\s+(IF\\s+EXISTS\\s+)?" + persistentTargetExpression + "\\s*$")
	viewAsClausePattern            = regexp.MustCompile(`(?is)^AS\s+(.+)$`)
	createSchemaTargetPattern      = regexp.MustCompile("(?is)^\\s*CREATE\\s+SCHEMA\\s+(IF\\s+NOT\\s+EXISTS\\s+)?`?([A-Za-z0-9_-]+)`?\\s*$")
	alterTableTargetPattern        = regexp.MustCompile("(?is)^\\s*ALTER\\s+TABLE\\s+(IF\\s+EXISTS\\s+)?" + persistentTargetExpression + "\\s+(.+)$")
	addColumnClausePattern         = regexp.MustCompile("(?is)^ADD\\s+COLUMN\\s+(IF\\s+NOT\\s+EXISTS\\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?\\s+([A-Za-z][A-Za-z0-9_]*)$")
	dropColumnClausePattern        = regexp.MustCompile("(?is)^DROP\\s+COLUMN\\s+(IF\\s+EXISTS\\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?$")
	renameColumnClausePattern      = regexp.MustCompile("(?is)^RENAME\\s+COLUMN\\s+(IF\\s+EXISTS\\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?\\s+TO\\s+`?([A-Za-z_][A-Za-z0-9_]*)`?$")
	renameTableToPattern           = regexp.MustCompile("(?is)^RENAME\\s+TO\\s+`?([A-Za-z0-9_-]+)`?$")
	setOptionsLeadPattern          = regexp.MustCompile(`(?is)^SET\s+OPTIONS\s*\(`)
	persistentLeadPattern          = regexp.MustCompile(`(?is)^\s*(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE|DROP\s+TABLE)\b`)
	unsupportedMutationLeadPattern = regexp.MustCompile(`(?is)^\s*(ALTER\s+TABLE|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+TABLE|(?:TEMP(?:ORARY)?\s+)?(?:SCHEMA|FUNCTION|PROCEDURE|MODEL))|DROP\s+(?:SCHEMA|FUNCTION|PROCEDURE|MODEL)|CALL|GRANT|REVOKE|EXPORT\s+DATA|LOAD\s+DATA|BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK)\b`)
)

// parsePersistentSQLStatement recognizes catalog-mutating single statements.
// Unsupported identifier spellings are rejected explicitly when the leading
// keyword is mutating, rather than being passed to a transient query path that
// could appear successful while discarding its effects.
func parsePersistentSQLStatement(projectID, queryText string) (persistentSQLStatement, bool, error) {
	stmt := trimLeadingSQLComments(queryText)
	stmt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stmt), ";"))
	type candidate struct {
		pattern       *regexp.Regexp
		statementType string
		dml           bool
		create        bool
		drop          bool
		targetGroup   int
	}
	candidates := []candidate{
		{insertTargetPattern, "INSERT", true, false, false, 1},
		{updateTargetPattern, "UPDATE", true, false, false, 1},
		{deleteTargetPattern, "DELETE", true, false, false, 1},
		{mergeTargetPattern, "MERGE", true, false, false, 1},
		{truncateTargetPattern, "TRUNCATE_TABLE", true, false, false, 1},
		{createTargetPattern, "CREATE_TABLE", false, true, false, 3},
		{dropTargetPattern, "DROP_TABLE", false, false, true, 2},
	}
	for _, c := range candidates {
		m := c.pattern.FindStringSubmatch(stmt)
		if m == nil {
			continue
		}
		target, err := parsePersistentTarget(projectID, m[c.targetGroup])
		if err != nil {
			return persistentSQLStatement{}, true, err
		}
		out := persistentSQLStatement{
			statementType: c.statementType,
			target:        target,
			dml:           c.dml,
			create:        c.create,
			drop:          c.drop,
		}
		if c.create {
			out.orReplace = strings.TrimSpace(m[1]) != ""
			out.ifNotExists = strings.TrimSpace(m[2]) != ""
		}
		if c.drop {
			out.ifExists = strings.TrimSpace(m[1]) != ""
		}
		return out, true, nil
	}
	if persistentLeadPattern.MatchString(stmt) {
		return persistentSQLStatement{}, true, fmt.Errorf("unsupported DDL/DML target syntax; use dataset.table or project.dataset.table, with backticks either around the whole identifier or around each segment")
	}
	if unsupportedMutationLeadPattern.MatchString(stmt) {
		return persistentSQLStatement{}, true, fmt.Errorf("unsupported persistent SQL statement; supported single-statement mutations are INSERT, UPDATE, DELETE, MERGE, TRUNCATE TABLE, CREATE [OR REPLACE] TABLE [AS SELECT], and DROP TABLE")
	}
	return persistentSQLStatement{}, false, nil
}

// trimLeadingSQLComments handles the comments commonly injected by dbt,
// Dataform and query tracing before a statement. Classification must see past
// them or a real mutation could fall through to the transient SELECT path.
func trimLeadingSQLComments(queryText string) string {
	remaining := strings.TrimSpace(queryText)
	for {
		switch {
		case strings.HasPrefix(remaining, "--"):
			newline := strings.IndexByte(remaining, '\n')
			if newline < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[newline+1:])
		case strings.HasPrefix(remaining, "/*"):
			end := strings.Index(remaining[2:], "*/")
			if end < 0 {
				return remaining
			}
			remaining = strings.TrimSpace(remaining[end+4:])
		default:
			return remaining
		}
	}
}

func parsePersistentTarget(defaultProjectID, raw string) (tableReference, error) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	for i, p := range parts {
		parts[i] = strings.Trim(strings.TrimSpace(p), "`")
	}
	switch len(parts) {
	case 2:
		return tableReference{ProjectID: defaultProjectID, DatasetID: parts[0], TableID: parts[1]}, nil
	case 3:
		return tableReference{ProjectID: parts[0], DatasetID: parts[1], TableID: parts[2]}, nil
	default:
		return tableReference{}, fmt.Errorf("invalid DDL/DML target %q: expected dataset.table or project.dataset.table", raw)
	}
}

// persistentViewStatement is CREATE/DROP [MATERIALIZED] VIEW's parsed shape —
// deliberately separate from persistentSQLStatement because a view is never
// executed through the embedded SQL engine (googlesqlite has no concept of
// this project's own view mechanism); it goes straight to the same
// tables.insert-style validate-and-store path real BigQuery's REST
// tables.insert with view.query already uses (see bigquery.go).
type persistentViewStatement struct {
	target       tableReference
	materialized bool
	orReplace    bool
	ifNotExists  bool
	ifExists     bool
	drop         bool
	selectBody   string
}

// parsePersistentViewStatement recognizes CREATE [OR REPLACE] [MATERIALIZED]
// VIEW ... AS <select> and DROP [MATERIALIZED] VIEW, the two DDL forms real
// tools (dbt's default "view" materialization, notably) emit that this
// project previously rejected outright with "unsupported persistent SQL
// statement" even though views are otherwise a real, supported resource via
// REST. A trailing OPTIONS(...) clause between the target and AS is
// recognized and discarded (bounded scope, declared explicitly: no field
// inside it — description, labels, expiration — is applied), rather than
// causing a parse failure.
func parsePersistentViewStatement(projectID, queryText string) (persistentViewStatement, bool, error) {
	stmt := trimLeadingSQLComments(queryText)
	stmt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stmt), ";"))

	if m := dropViewTargetPattern.FindStringSubmatch(stmt); m != nil {
		target, err := parsePersistentTarget(projectID, m[3])
		if err != nil {
			return persistentViewStatement{}, true, err
		}
		return persistentViewStatement{
			target:       target,
			materialized: strings.TrimSpace(m[1]) != "",
			ifExists:     strings.TrimSpace(m[2]) != "",
			drop:         true,
		}, true, nil
	}

	m := createViewTargetPattern.FindStringSubmatch(stmt)
	if m == nil {
		return persistentViewStatement{}, false, nil
	}
	target, err := parsePersistentTarget(projectID, m[4])
	if err != nil {
		return persistentViewStatement{}, true, err
	}
	rest := strings.TrimSpace(stmt[len(m[0]):])
	rest = stripLeadingOptionsClause(rest)
	asMatch := viewAsClausePattern.FindStringSubmatch(rest)
	if asMatch == nil {
		return persistentViewStatement{}, true, fmt.Errorf("invalid CREATE VIEW statement: expected AS <select> after the view name/options")
	}
	return persistentViewStatement{
		target:       target,
		materialized: strings.TrimSpace(m[2]) != "",
		orReplace:    strings.TrimSpace(m[1]) != "",
		ifNotExists:  strings.TrimSpace(m[3]) != "",
		selectBody:   strings.TrimSpace(asMatch[1]),
	}, true, nil
}

// stripLeadingOptionsClause removes a leading "OPTIONS(...)" clause (as
// BigQuery's own CREATE VIEW syntax allows between the view name and AS),
// matching parentheses by depth rather than a non-nesting regex so option
// values that themselves contain parens (e.g. labels=[("k","v")]) don't
// truncate the strip early. Returns text unchanged if it doesn't start with
// OPTIONS(.
func stripLeadingOptionsClause(text string) string {
	const prefix = "options"
	trimmed := strings.TrimSpace(text)
	if len(trimmed) <= len(prefix) || !strings.EqualFold(trimmed[:len(prefix)], prefix) {
		return text
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	if !strings.HasPrefix(rest, "(") {
		return text
	}
	depth := 0
	for i, r := range rest {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(rest[i+1:])
			}
		}
	}
	return text
}

// executePersistentViewStatement runs a recognized CREATE/DROP [MATERIALIZED]
// VIEW statement, reusing exactly the same schema-derivation/validation
// tables.insert's view.query path already applies at REST creation time —
// the query is executed once for real to both validate it and infer its
// schema, never accepted as an unverifiable definition.
func (s *Server) executePersistentViewStatement(projectID, queryText string, sess *sessionRecord) (persistentSQLResult, bool, error) {
	stmt, handled, err := parsePersistentViewStatement(projectID, queryText)
	if !handled || err != nil {
		return persistentSQLResult{}, handled, err
	}
	if !strings.EqualFold(stmt.target.ProjectID, projectID) {
		return persistentSQLResult{}, true, fmt.Errorf("cross-project DDL target %s.%s.%s is not supported", stmt.target.ProjectID, stmt.target.DatasetID, stmt.target.TableID)
	}
	if sess != nil && sess.inTransaction() {
		return persistentSQLResult{}, true, fmt.Errorf("persistent DDL inside a session transaction is not supported yet; rollback or commit the transaction first")
	}

	statementType := "CREATE_VIEW"
	if stmt.materialized {
		statementType = "CREATE_MATERIALIZED_VIEW"
	}
	existing, exists, version := s.tables.get(projectID, stmt.target.DatasetID, stmt.target.TableID)

	if stmt.drop {
		statementType = "DROP_VIEW"
		if stmt.materialized {
			statementType = "DROP_MATERIALIZED_VIEW"
		}
		if !exists {
			if stmt.ifExists {
				return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: statementType}, true, nil
			}
			return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
		}
		if existing.View == nil {
			return persistentSQLResult{}, true, fmt.Errorf("%s.%s is not a view", stmt.target.DatasetID, stmt.target.TableID)
		}
		if err := s.tables.deleteIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version); err != nil {
			return persistentSQLResult{}, true, err
		}
		return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: statementType}, true, nil
	}

	if exists && stmt.ifNotExists {
		return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: statementType}, true, nil
	}
	if exists && !stmt.orReplace {
		return persistentSQLResult{}, true, fmt.Errorf("table already exists: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
	}
	if !s.datasets.exists(projectID, stmt.target.DatasetID) {
		return persistentSQLResult{}, true, fmt.Errorf("dataset not found: %s", stmt.target.DatasetID)
	}

	derivedSchema, _, err := s.executeRealSQLQuery(projectID, stmt.selectBody, nil)
	if err != nil {
		return persistentSQLResult{}, true, fmt.Errorf("invalid view query: %w", err)
	}

	if exists {
		if err := s.tables.replaceViewIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version, derivedSchema, stmt.selectBody, stmt.materialized); err != nil {
			return persistentSQLResult{}, true, err
		}
	} else {
		insert := tableInsert{
			ProjectID: projectID, DatasetID: stmt.target.DatasetID, TableID: stmt.target.TableID,
			Schema: derivedSchema, View: &viewConfig{Query: stmt.selectBody, Materialized: stmt.materialized},
		}
		if _, created := s.tables.insert(insert); !created {
			return persistentSQLResult{}, true, fmt.Errorf("table %s.%s was created concurrently; retry the statement", stmt.target.DatasetID, stmt.target.TableID)
		}
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: statementType}, true, nil
}

// parseCreateSchemaStatement recognizes CREATE SCHEMA [IF NOT EXISTS]
// dataset_id, split out from executePersistentCreateSchemaStatement so
// computeQueryJobResultRows' early-poll mutating-statement guard can reuse
// the exact same recognition rather than drifting from it.
func parseCreateSchemaStatement(queryText string) (datasetID string, ifNotExists, handled bool) {
	stmt := trimLeadingSQLComments(queryText)
	stmt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stmt), ";"))
	m := createSchemaTargetPattern.FindStringSubmatch(stmt)
	if m == nil {
		return "", false, false
	}
	return m[2], strings.TrimSpace(m[1]) != "", true
}

// executePersistentCreateSchemaStatement runs a recognized
// CREATE SCHEMA [IF NOT EXISTS] dataset_id statement — real BigQuery's SQL
// spelling of dataset creation, otherwise only reachable via the REST
// datasets.insert call. SQLMesh's own state-store bootstrap (creating its
// "sqlmesh" dataset) issues exactly this before this project supported it,
// failing with the generic "unsupported persistent SQL statement" error.
// Anything beyond the bare name — an OPTIONS(...) clause, DEFAULT COLLATE,
// or CREATE SCHEMA without IF NOT EXISTS colliding with DROP SCHEMA support
// — is intentionally left unhandled, falling through to the existing
// unsupportedMutationLeadPattern rejection rather than silently accepted and
// ignored.
func (s *Server) executePersistentCreateSchemaStatement(projectID, queryText string) (persistentSQLResult, bool, error) {
	datasetID, ifNotExists, handled := parseCreateSchemaStatement(queryText)
	if !handled {
		return persistentSQLResult{}, false, nil
	}
	if s.datasets.exists(projectID, datasetID) {
		if ifNotExists {
			return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "CREATE_SCHEMA"}, true, nil
		}
		return persistentSQLResult{}, true, fmt.Errorf("dataset already exists: %s", datasetID)
	}
	if _, created := s.datasets.insert(datasetInsert{ProjectID: projectID, DatasetID: datasetID}); !created {
		return persistentSQLResult{}, true, fmt.Errorf("dataset %s was created concurrently; retry the statement", datasetID)
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "CREATE_SCHEMA"}, true, nil
}

// alterTableAddColumnClause is one parsed "ADD COLUMN [IF NOT EXISTS] name
// type" clause; ifNotExists means a column already present under that name
// is silently kept as-is rather than rejected as a conflict.
// alterTableLead is the shared "target" prefix of every ALTER TABLE action
// this project recognizes (ADD/DROP/RENAME COLUMN, RENAME TO, SET OPTIONS)
// — factored out so each action-specific parser below only deals with its
// own action grammar, not target/IF EXISTS parsing repeated for each one.
// Real BigQuery grammar allows several actions comma-separated in a single
// ALTER TABLE; that compound form is intentionally not supported here — one
// action kind per statement — and falls through to the generic unsupported-
// ALTER-TABLE rejection like any other unrecognized shape.
type alterTableLead struct {
	target   tableReference
	ifExists bool
	rest     string
}

func matchAlterTableLead(projectID, queryText string) (alterTableLead, bool, error) {
	stmt := trimLeadingSQLComments(queryText)
	stmt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stmt), ";"))
	m := alterTableTargetPattern.FindStringSubmatch(stmt)
	if m == nil {
		return alterTableLead{}, false, nil
	}
	target, err := parsePersistentTarget(projectID, m[2])
	if err != nil {
		return alterTableLead{}, true, err
	}
	return alterTableLead{
		target:   target,
		ifExists: strings.TrimSpace(m[1]) != "",
		rest:     strings.TrimSpace(m[3]),
	}, true, nil
}

type alterTableAddColumnClause struct {
	field       tableField
	ifNotExists bool
}

// persistentAlterTableAddColumns is ALTER TABLE ADD COLUMN's parsed shape.
// Only one or more comma-separated ADD COLUMN clauses naming a plain scalar
// type are recognized — ALTER COLUMN is the one action this project has no
// support for at all (see the sibling parsers below for DROP/RENAME COLUMN,
// RENAME TO and SET OPTIONS).
type persistentAlterTableAddColumns struct {
	target   tableReference
	ifExists bool
	clauses  []alterTableAddColumnClause
}

// parsePersistentAlterTableAddColumns recognizes
// ALTER TABLE [IF EXISTS] target ADD COLUMN [IF NOT EXISTS] name type
// [, ADD COLUMN ...] — real BigQuery DDL for schema evolution that SQLMesh's
// own migration framework issues against its state tables, and that dbt's
// "on_schema_change: append_new_columns" incremental setting issues against
// user tables. A column type with parameters (e.g. STRING(10), NUMERIC(10,2))
// or a STRUCT/ARRAY type is intentionally not accepted — a bounded scope
// declared explicitly rather than mishandled.
func parsePersistentAlterTableAddColumns(projectID, queryText string) (persistentAlterTableAddColumns, bool, error) {
	lead, handled, err := matchAlterTableLead(projectID, queryText)
	if !handled || err != nil {
		return persistentAlterTableAddColumns{}, handled, err
	}
	var clauses []alterTableAddColumnClause
	for _, clause := range strings.Split(lead.rest, ",") {
		cm := addColumnClausePattern.FindStringSubmatch(strings.TrimSpace(clause))
		if cm == nil {
			return persistentAlterTableAddColumns{}, false, nil
		}
		clauses = append(clauses, alterTableAddColumnClause{
			field: tableField{
				Name: cm[2],
				Type: normalizeAlterTableColumnType(cm[3]),
				Mode: "NULLABLE",
			},
			ifNotExists: strings.TrimSpace(cm[1]) != "",
		})
	}
	if len(clauses) == 0 {
		return persistentAlterTableAddColumns{}, false, nil
	}
	return persistentAlterTableAddColumns{
		target:   lead.target,
		ifExists: lead.ifExists,
		clauses:  clauses,
	}, true, nil
}

type alterTableDropColumnClause struct {
	name     string
	ifExists bool
}

type persistentAlterTableDropColumns struct {
	target   tableReference
	ifExists bool
	clauses  []alterTableDropColumnClause
}

// parsePersistentAlterTableDropColumns recognizes
// ALTER TABLE [IF EXISTS] target DROP COLUMN [IF EXISTS] name [, DROP COLUMN ...].
func parsePersistentAlterTableDropColumns(projectID, queryText string) (persistentAlterTableDropColumns, bool, error) {
	lead, handled, err := matchAlterTableLead(projectID, queryText)
	if !handled || err != nil {
		return persistentAlterTableDropColumns{}, handled, err
	}
	var clauses []alterTableDropColumnClause
	for _, clause := range strings.Split(lead.rest, ",") {
		cm := dropColumnClausePattern.FindStringSubmatch(strings.TrimSpace(clause))
		if cm == nil {
			return persistentAlterTableDropColumns{}, false, nil
		}
		clauses = append(clauses, alterTableDropColumnClause{
			name:     cm[2],
			ifExists: strings.TrimSpace(cm[1]) != "",
		})
	}
	if len(clauses) == 0 {
		return persistentAlterTableDropColumns{}, false, nil
	}
	return persistentAlterTableDropColumns{target: lead.target, ifExists: lead.ifExists, clauses: clauses}, true, nil
}

type alterTableRenameColumnClause struct {
	oldName  string
	newName  string
	ifExists bool
}

type persistentAlterTableRenameColumns struct {
	target   tableReference
	ifExists bool
	clauses  []alterTableRenameColumnClause
}

// parsePersistentAlterTableRenameColumns recognizes
// ALTER TABLE [IF EXISTS] target RENAME COLUMN [IF EXISTS] old TO new
// [, RENAME COLUMN ...].
func parsePersistentAlterTableRenameColumns(projectID, queryText string) (persistentAlterTableRenameColumns, bool, error) {
	lead, handled, err := matchAlterTableLead(projectID, queryText)
	if !handled || err != nil {
		return persistentAlterTableRenameColumns{}, handled, err
	}
	var clauses []alterTableRenameColumnClause
	for _, clause := range strings.Split(lead.rest, ",") {
		cm := renameColumnClausePattern.FindStringSubmatch(strings.TrimSpace(clause))
		if cm == nil {
			return persistentAlterTableRenameColumns{}, false, nil
		}
		clauses = append(clauses, alterTableRenameColumnClause{
			ifExists: strings.TrimSpace(cm[1]) != "",
			oldName:  cm[2],
			newName:  cm[3],
		})
	}
	if len(clauses) == 0 {
		return persistentAlterTableRenameColumns{}, false, nil
	}
	return persistentAlterTableRenameColumns{target: lead.target, ifExists: lead.ifExists, clauses: clauses}, true, nil
}

type persistentAlterTableRenameTo struct {
	target     tableReference
	ifExists   bool
	newTableID string
}

// parsePersistentAlterTableRenameTo recognizes
// ALTER TABLE [IF EXISTS] target RENAME TO new_table_name — a single action,
// not a comma-separated clause list like the column-level actions above.
func parsePersistentAlterTableRenameTo(projectID, queryText string) (persistentAlterTableRenameTo, bool, error) {
	lead, handled, err := matchAlterTableLead(projectID, queryText)
	if !handled || err != nil {
		return persistentAlterTableRenameTo{}, handled, err
	}
	m := renameTableToPattern.FindStringSubmatch(lead.rest)
	if m == nil {
		return persistentAlterTableRenameTo{}, false, nil
	}
	return persistentAlterTableRenameTo{target: lead.target, ifExists: lead.ifExists, newTableID: m[1]}, true, nil
}

type persistentAlterTableSetOptions struct {
	target   tableReference
	ifExists bool
}

// parsePersistentAlterTableSetOptions recognizes
// ALTER TABLE [IF EXISTS] target SET OPTIONS(...) — the option key/value
// pairs are matched (balanced-paren, so a value containing its own parens
// doesn't truncate the match early) and discarded entirely, the same
// declared-bounded-scope treatment CREATE VIEW's own OPTIONS(...) clause
// already gets (see stripLeadingOptionsClause): no field inside it
// (description, labels, expiration, ...) is applied to the table.
func parsePersistentAlterTableSetOptions(projectID, queryText string) (persistentAlterTableSetOptions, bool, error) {
	lead, handled, err := matchAlterTableLead(projectID, queryText)
	if !handled || err != nil {
		return persistentAlterTableSetOptions{}, handled, err
	}
	if !setOptionsLeadPattern.MatchString(lead.rest) {
		return persistentAlterTableSetOptions{}, false, nil
	}
	afterSet := strings.TrimSpace(lead.rest[len("SET"):])
	remainder := stripLeadingOptionsClause(afterSet)
	if strings.TrimSpace(remainder) != "" {
		// Trailing text after the OPTIONS(...) clause is not part of this
		// project's bounded grammar for it.
		return persistentAlterTableSetOptions{}, false, nil
	}
	return persistentAlterTableSetOptions{target: lead.target, ifExists: lead.ifExists}, true, nil
}

// normalizeAlterTableColumnType maps the handful of alternate spellings real
// BigQuery DDL accepts as synonyms onto this project's canonical schema type
// names; anything else passes through unchanged.
func normalizeAlterTableColumnType(t string) string {
	switch strings.ToUpper(t) {
	case "BOOLEAN":
		return "BOOL"
	case "INTEGER":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	default:
		return strings.ToUpper(t)
	}
}

// executePersistentAlterTableAddColumnsStatement runs a recognized
// ALTER TABLE ADD COLUMN statement, appending the new nullable column(s) to
// the table's schema and a NULL cell to every existing row — schema
// evolution, not a full CREATE OR REPLACE, so partitioning/clustering/view
// identity are left untouched (see tables_service.go's addColumnsIfVersion).
func (s *Server) executePersistentAlterTableAddColumnsStatement(projectID, queryText string) (persistentSQLResult, bool, error) {
	stmt, handled, err := parsePersistentAlterTableAddColumns(projectID, queryText)
	if !handled || err != nil {
		return persistentSQLResult{}, handled, err
	}
	if !strings.EqualFold(stmt.target.ProjectID, projectID) {
		return persistentSQLResult{}, true, fmt.Errorf("cross-project DDL target %s.%s.%s is not supported", stmt.target.ProjectID, stmt.target.DatasetID, stmt.target.TableID)
	}
	existing, exists, version := s.tables.get(projectID, stmt.target.DatasetID, stmt.target.TableID)
	if !exists {
		if stmt.ifExists {
			return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
		}
		return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
	}
	if existing.View != nil {
		return persistentSQLResult{}, true, fmt.Errorf("ALTER TABLE target %s.%s is a view", stmt.target.DatasetID, stmt.target.TableID)
	}
	newFields, err := resolveAlterTableNewFields(existing.Schema, stmt.clauses)
	if err != nil {
		return persistentSQLResult{}, true, err
	}
	if len(newFields) == 0 {
		return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
	}
	if err := s.tables.addColumnsIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version, newFields); err != nil {
		return persistentSQLResult{}, true, err
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
}

// resolveAlterTableNewFields filters each ADD COLUMN clause against the
// table's current schema: a clause naming an already-present column is
// dropped silently when it carries IF NOT EXISTS, or rejected otherwise.
func resolveAlterTableNewFields(existingSchema []tableField, clauses []alterTableAddColumnClause) ([]tableField, error) {
	newFields := make([]tableField, 0, len(clauses))
	for _, clause := range clauses {
		alreadyExists := false
		for _, existingField := range existingSchema {
			if strings.EqualFold(existingField.Name, clause.field.Name) {
				alreadyExists = true
				break
			}
		}
		if alreadyExists {
			if clause.ifNotExists {
				continue
			}
			return nil, fmt.Errorf("column already exists: %s", clause.field.Name)
		}
		newFields = append(newFields, clause.field)
	}
	return newFields, nil
}

// resolveAlterTableDropNames filters each DROP COLUMN clause against the
// table's current schema: a clause naming a column that isn't present is
// dropped silently when it carries IF EXISTS, or rejected otherwise.
func resolveAlterTableDropNames(existingSchema []tableField, clauses []alterTableDropColumnClause) ([]string, error) {
	dropNames := make([]string, 0, len(clauses))
	for _, clause := range clauses {
		found := false
		for _, existingField := range existingSchema {
			if strings.EqualFold(existingField.Name, clause.name) {
				found = true
				break
			}
		}
		if !found {
			if clause.ifExists {
				continue
			}
			return nil, fmt.Errorf("column not found: %s", clause.name)
		}
		dropNames = append(dropNames, clause.name)
	}
	return dropNames, nil
}

// resolveAlterTableRenames filters each RENAME COLUMN clause against the
// table's current schema: a clause whose old name isn't present is dropped
// silently when it carries IF EXISTS, or rejected otherwise; a clause whose
// new name already exists on the table is always rejected, IF EXISTS or not.
func resolveAlterTableRenames(existingSchema []tableField, clauses []alterTableRenameColumnClause) (map[string]string, error) {
	renames := make(map[string]string, len(clauses))
	for _, clause := range clauses {
		found := false
		for _, existingField := range existingSchema {
			if strings.EqualFold(existingField.Name, clause.newName) {
				return nil, fmt.Errorf("column already exists: %s", clause.newName)
			}
			if strings.EqualFold(existingField.Name, clause.oldName) {
				found = true
			}
		}
		if !found {
			if clause.ifExists {
				continue
			}
			return nil, fmt.Errorf("column not found: %s", clause.oldName)
		}
		renames[clause.oldName] = clause.newName
	}
	return renames, nil
}

// isPersistentAlterTableStatement reports whether queryText matches ANY of
// this project's recognized ALTER TABLE actions (ADD/DROP/RENAME COLUMN,
// RENAME TO, SET OPTIONS) — used by computeQueryJobResultRows' early-poll
// guard, which must recognize every one of them, not just ADD COLUMN, or the
// same "a getQueryResults poll re-runs a pending mutation early" race this
// project already fixed once for CREATE VIEW would reopen for whichever
// ALTER TABLE variant got missed.
func isPersistentAlterTableStatement(projectID, queryText string) bool {
	if _, handled, _ := parsePersistentAlterTableAddColumns(projectID, queryText); handled {
		return true
	}
	if _, handled, _ := parsePersistentAlterTableDropColumns(projectID, queryText); handled {
		return true
	}
	if _, handled, _ := parsePersistentAlterTableRenameColumns(projectID, queryText); handled {
		return true
	}
	if _, handled, _ := parsePersistentAlterTableRenameTo(projectID, queryText); handled {
		return true
	}
	_, handled, _ := parsePersistentAlterTableSetOptions(projectID, queryText)
	return handled
}

// executePersistentAlterTableStatement is ALTER TABLE's single entry point,
// trying each action-specific parser in turn (ADD COLUMN, DROP COLUMN,
// RENAME COLUMN, RENAME TO, SET OPTIONS) and dispatching to whichever one
// actually recognizes the statement. An ALTER TABLE that matches none of
// them (ALTER COLUMN, a compound multi-action statement, or any other
// shape) returns handled=false, falling through to the generic
// unsupported-ALTER-TABLE rejection in executePersistentSQLStatement.
func (s *Server) executePersistentAlterTableStatement(projectID, queryText string) (persistentSQLResult, bool, error) {
	if result, handled, err := s.executePersistentAlterTableAddColumnsStatement(projectID, queryText); handled {
		return result, true, err
	}
	if result, handled, err := s.executePersistentAlterTableDropColumnsStatement(projectID, queryText); handled {
		return result, true, err
	}
	if result, handled, err := s.executePersistentAlterTableRenameColumnsStatement(projectID, queryText); handled {
		return result, true, err
	}
	if result, handled, err := s.executePersistentAlterTableRenameToStatement(projectID, queryText); handled {
		return result, true, err
	}
	return s.executePersistentAlterTableSetOptionsStatement(projectID, queryText)
}

// executePersistentAlterTableDropColumnsStatement runs a recognized
// ALTER TABLE DROP COLUMN statement: removes the named column(s) from the
// table's schema and the corresponding cell from every existing row.
func (s *Server) executePersistentAlterTableDropColumnsStatement(projectID, queryText string) (persistentSQLResult, bool, error) {
	stmt, handled, err := parsePersistentAlterTableDropColumns(projectID, queryText)
	if !handled || err != nil {
		return persistentSQLResult{}, handled, err
	}
	if !strings.EqualFold(stmt.target.ProjectID, projectID) {
		return persistentSQLResult{}, true, fmt.Errorf("cross-project DDL target %s.%s.%s is not supported", stmt.target.ProjectID, stmt.target.DatasetID, stmt.target.TableID)
	}
	existing, exists, version := s.tables.get(projectID, stmt.target.DatasetID, stmt.target.TableID)
	if !exists {
		if stmt.ifExists {
			return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
		}
		return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
	}
	if existing.View != nil {
		return persistentSQLResult{}, true, fmt.Errorf("ALTER TABLE target %s.%s is a view", stmt.target.DatasetID, stmt.target.TableID)
	}
	dropNames, err := resolveAlterTableDropNames(existing.Schema, stmt.clauses)
	if err != nil {
		return persistentSQLResult{}, true, err
	}
	if len(dropNames) == 0 {
		return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
	}
	if err := s.tables.dropColumnsIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version, dropNames); err != nil {
		return persistentSQLResult{}, true, err
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
}

// executePersistentAlterTableRenameColumnsStatement runs a recognized
// ALTER TABLE RENAME COLUMN statement: only the schema field's Name changes,
// since renaming never moves data between columns.
func (s *Server) executePersistentAlterTableRenameColumnsStatement(projectID, queryText string) (persistentSQLResult, bool, error) {
	stmt, handled, err := parsePersistentAlterTableRenameColumns(projectID, queryText)
	if !handled || err != nil {
		return persistentSQLResult{}, handled, err
	}
	if !strings.EqualFold(stmt.target.ProjectID, projectID) {
		return persistentSQLResult{}, true, fmt.Errorf("cross-project DDL target %s.%s.%s is not supported", stmt.target.ProjectID, stmt.target.DatasetID, stmt.target.TableID)
	}
	existing, exists, version := s.tables.get(projectID, stmt.target.DatasetID, stmt.target.TableID)
	if !exists {
		if stmt.ifExists {
			return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
		}
		return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
	}
	if existing.View != nil {
		return persistentSQLResult{}, true, fmt.Errorf("ALTER TABLE target %s.%s is a view", stmt.target.DatasetID, stmt.target.TableID)
	}
	renames, err := resolveAlterTableRenames(existing.Schema, stmt.clauses)
	if err != nil {
		return persistentSQLResult{}, true, err
	}
	if len(renames) == 0 {
		return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
	}
	if err := s.tables.renameColumnsIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version, renames); err != nil {
		return persistentSQLResult{}, true, err
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
}

// executePersistentAlterTableRenameToStatement runs a recognized
// ALTER TABLE RENAME TO statement: the table moves to a new name within the
// same dataset, keeping its schema/rows/partitioning/view identity intact.
func (s *Server) executePersistentAlterTableRenameToStatement(projectID, queryText string) (persistentSQLResult, bool, error) {
	stmt, handled, err := parsePersistentAlterTableRenameTo(projectID, queryText)
	if !handled || err != nil {
		return persistentSQLResult{}, handled, err
	}
	if !strings.EqualFold(stmt.target.ProjectID, projectID) {
		return persistentSQLResult{}, true, fmt.Errorf("cross-project DDL target %s.%s.%s is not supported", stmt.target.ProjectID, stmt.target.DatasetID, stmt.target.TableID)
	}
	_, exists, version := s.tables.get(projectID, stmt.target.DatasetID, stmt.target.TableID)
	if !exists {
		if stmt.ifExists {
			return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
		}
		return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
	}
	if err := s.tables.renameTableIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, stmt.newTableID, version); err != nil {
		return persistentSQLResult{}, true, err
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
}

// executePersistentAlterTableSetOptionsStatement runs a recognized
// ALTER TABLE SET OPTIONS statement. The options themselves are parsed and
// discarded (see parsePersistentAlterTableSetOptions); the only real effect
// is validating the target exists, matching the "syntax accepted, bounded
// scope declared" treatment already used for CREATE VIEW's own OPTIONS(...).
func (s *Server) executePersistentAlterTableSetOptionsStatement(projectID, queryText string) (persistentSQLResult, bool, error) {
	stmt, handled, err := parsePersistentAlterTableSetOptions(projectID, queryText)
	if !handled || err != nil {
		return persistentSQLResult{}, handled, err
	}
	if !strings.EqualFold(stmt.target.ProjectID, projectID) {
		return persistentSQLResult{}, true, fmt.Errorf("cross-project DDL target %s.%s.%s is not supported", stmt.target.ProjectID, stmt.target.DatasetID, stmt.target.TableID)
	}
	_, exists, _ := s.tables.get(projectID, stmt.target.DatasetID, stmt.target.TableID)
	if !exists {
		if stmt.ifExists {
			return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
		}
		return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: "ALTER_TABLE"}, true, nil
}

// executePersistentSQLStatement runs a recognized DDL/DML statement against
// the same real GoogleSQL engine as SELECT, then atomically commits the final
// table image into LocaQL's catalog. The engine database stays isolated until
// every SQL and schema/REQUIRED validation succeeds, giving statement-level
// atomicity even when the analyzer fails halfway through.
func (s *Server) executePersistentSQLStatement(projectID, queryText string, sess *sessionRecord, paramMode string, params []storedQueryParameter) (persistentSQLResult, bool, error) {
	stmt, handled, err := parsePersistentSQLStatement(projectID, queryText)
	if !handled || err != nil {
		return persistentSQLResult{}, handled, err
	}
	if !strings.EqualFold(stmt.target.ProjectID, projectID) {
		return persistentSQLResult{}, true, fmt.Errorf("cross-project DDL/DML target %s.%s.%s is not supported", stmt.target.ProjectID, stmt.target.DatasetID, stmt.target.TableID)
	}
	if strings.EqualFold(stmt.target.DatasetID, sessionDatasetName) {
		return persistentSQLResult{}, true, fmt.Errorf("persistent DDL/DML cannot target _SESSION; use CREATE TEMP TABLE for session-scoped data")
	}
	if sess != nil && sess.inTransaction() {
		return persistentSQLResult{}, true, fmt.Errorf("persistent DDL/DML inside a session transaction is not supported yet; rollback or commit the transaction first")
	}
	if !s.datasets.exists(projectID, stmt.target.DatasetID) {
		return persistentSQLResult{}, true, fmt.Errorf("dataset not found: %s", stmt.target.DatasetID)
	}

	existing, exists, version := s.tables.get(projectID, stmt.target.DatasetID, stmt.target.TableID)
	if stmt.drop {
		if !exists {
			if stmt.ifExists {
				return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: stmt.statementType}, true, nil
			}
			return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
		}
		if existing.View != nil {
			return persistentSQLResult{}, true, fmt.Errorf("DROP TABLE target %s.%s is a view; use DROP VIEW", stmt.target.DatasetID, stmt.target.TableID)
		}
		if err := s.tables.deleteIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version); err != nil {
			return persistentSQLResult{}, true, err
		}
		return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: stmt.statementType}, true, nil
	}

	if stmt.create && exists && stmt.ifNotExists {
		return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: stmt.statementType}, true, nil
	}
	if stmt.create && exists && !stmt.orReplace {
		return persistentSQLResult{}, true, fmt.Errorf("table already exists: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
	}
	if stmt.dml {
		if !exists {
			return persistentSQLResult{}, true, fmt.Errorf("table not found: %s.%s", stmt.target.DatasetID, stmt.target.TableID)
		}
		if existing.View != nil {
			return persistentSQLResult{}, true, fmt.Errorf("DML target %s.%s is a view", stmt.target.DatasetID, stmt.target.TableID)
		}
		if existing.External != nil {
			return persistentSQLResult{}, true, fmt.Errorf("DML target %s.%s is an external table", stmt.target.DatasetID, stmt.target.TableID)
		}
	}

	ref := datasetTableRef{datasetID: stmt.target.DatasetID, tableID: stmt.target.TableID}
	db, release, processedBytes, err := s.openMaterializedSQLDatabase(projectID, queryText, map[string]bool{}, sess, []datasetTableRef{ref}, false)
	if err != nil {
		return persistentSQLResult{}, true, err
	}
	defer release()
	args, err := buildQueryArgs(paramMode, params)
	if err != nil {
		return persistentSQLResult{}, true, err
	}
	engineStatement := stripProjectPrefix(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(queryText), ";")), projectID)
	if stmt.dml && existing.TimePartitioning != nil && existing.TimePartitioning.Field == "" {
		if err := validateIngestionPseudocolumnWrites(engineStatement); err != nil {
			return persistentSQLResult{}, true, err
		}
		engineStatement = addImplicitInsertColumns(engineStatement, existing.Schema)
	}
	engineStatement = rewriteIngestionPseudocolumns(engineStatement)
	if stmt.statementType == "MERGE" {
		rewritten, cleanup, mergeErr := s.rewriteMergeUsingSubquery(db, stmt.target.DatasetID, engineStatement)
		if mergeErr != nil {
			return persistentSQLResult{}, true, mergeErr
		}
		engineStatement = rewritten
		if cleanup != nil {
			defer cleanup()
		}
	}
	result, err := db.Exec(engineStatement, args...)
	if err != nil {
		return persistentSQLResult{}, true, err
	}
	affectedRows := rowsAffected(result)

	var existingTimePartitioning *timePartitioningConfig
	if existing != nil {
		existingTimePartitioning = existing.TimePartitioning
	}
	finalSchema, finalRows, finalPartitions, err := readEngineTable(db, stmt.target.DatasetID, stmt.target.TableID, existingTimePartitioning, s.tables.now().UTC())
	if err != nil {
		return persistentSQLResult{}, true, fmt.Errorf("read final table %s.%s: %w", stmt.target.DatasetID, stmt.target.TableID, err)
	}
	if stmt.dml {
		affectedRows = effectiveDMLAffectedRows(stmt.statementType, affectedRows, existing.Rows, finalRows)
		if err := validateStoredRows(existing.Schema, finalRows); err != nil {
			return persistentSQLResult{}, true, err
		}
		if len(finalSchema) != len(existing.Schema) {
			return persistentSQLResult{}, true, fmt.Errorf("DML unexpectedly changed schema for %s.%s", stmt.target.DatasetID, stmt.target.TableID)
		}
		if err := s.tables.replaceRowsIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version, finalRows, finalPartitions); err != nil {
			return persistentSQLResult{}, true, err
		}
	} else if exists {
		if err := s.tables.replaceTableIfVersion(projectID, stmt.target.DatasetID, stmt.target.TableID, version, finalSchema, finalRows); err != nil {
			return persistentSQLResult{}, true, err
		}
	} else {
		if err := validateStoredRows(finalSchema, finalRows); err != nil {
			return persistentSQLResult{}, true, err
		}
		if _, created := s.tables.insert(tableInsert{ProjectID: projectID, DatasetID: stmt.target.DatasetID, TableID: stmt.target.TableID, Schema: finalSchema, Rows: finalRows}); !created {
			return persistentSQLResult{}, true, fmt.Errorf("table %s.%s was created concurrently; retry the statement", stmt.target.DatasetID, stmt.target.TableID)
		}
	}
	return persistentSQLResult{schema: []tableField{}, rows: [][]string{}, statementType: stmt.statementType, dmlAffectedRows: affectedRows, processedBytes: processedBytes}, true, nil
}

func readEngineTable(db *sql.DB, datasetID, tableID string, timePartitioning *timePartitioningConfig, now time.Time) ([]tableField, [][]string, []string, error) {
	rows, err := db.Query("SELECT * FROM " + quoteIdent(datasetID) + "." + quoteIdent(tableID))
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()
	schema, values, err := scanRealSQLRows(rows)
	if err != nil {
		return nil, nil, nil, err
	}
	schema, values, partitions := partitionIDsFromEngineRows(schema, values, timePartitioning, now)
	return schema, values, partitions, nil
}

var ingestionPseudocolumnWritePattern = regexp.MustCompile(`(?is)(?:\bSET\b|,)\s*(?:` + "`?[A-Za-z_][A-Za-z0-9_]*`?\\." + `)?` + "`?_PARTITION(?:TIME|DATE)`?" + `\s*=`)
var ingestionPseudocolumnInsertPattern = regexp.MustCompile(`(?is)\bINSERT\s*(?:INTO\s+[^\s(]+\s*)?\([^)]*` + "`?_PARTITION(?:TIME|DATE)`?" + `\)`)

func validateIngestionPseudocolumnWrites(queryText string) error {
	if ingestionPseudocolumnWritePattern.MatchString(queryText) || ingestionPseudocolumnInsertPattern.MatchString(queryText) {
		return fmt.Errorf("_PARTITIONTIME and _PARTITIONDATE are read-only pseudocolumns")
	}
	return nil
}

// addImplicitInsertColumns keeps INSERT ... VALUES/SELECT compatible with an
// engine table that carries two extra internal columns. BigQuery's public
// schema excludes pseudocolumns, so an omitted target column list refers only
// to the user-declared fields.
func addImplicitInsertColumns(queryText string, schema []tableField) string {
	match := insertTargetPattern.FindStringSubmatchIndex(queryText)
	if match == nil || len(match) < 4 || match[3] <= match[2] {
		return queryText
	}
	boundary := match[3]
	if strings.HasPrefix(strings.TrimSpace(queryText[boundary:]), "(") {
		return queryText
	}
	columns := make([]string, len(schema))
	for i, field := range schema {
		columns[i] = quoteIdent(field.Name)
	}
	return queryText[:boundary] + " (" + strings.Join(columns, ", ") + ")" + queryText[boundary:]
}

func rowsAffected(result sql.Result) int64 {
	if result == nil {
		return 0
	}
	n, err := result.RowsAffected()
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// effectiveDMLAffectedRows compensates for statements such as MERGE where
// googlesqlite performs the mutation but its driver reports RowsAffected=0.
// The multiset delta counts an UPDATE as one removed old row plus one added new
// row collapsed to max(removed, added), while INSERT/DELETE/TRUNCATE use their
// natural row-count deltas. A positive driver result remains authoritative.
func effectiveDMLAffectedRows(statementType string, driverRows int64, before, after [][]string) int64 {
	if driverRows > 0 {
		return driverRows
	}
	switch statementType {
	case "INSERT":
		if len(after) > len(before) {
			return int64(len(after) - len(before))
		}
	case "DELETE", "TRUNCATE_TABLE":
		if len(before) > len(after) {
			return int64(len(before) - len(after))
		}
	case "MERGE":
		removed, added := storedRowMultisetDelta(before, after)
		if removed > added {
			return removed
		}
		return added
	}
	return driverRows
}

func storedRowMultisetDelta(before, after [][]string) (removed, added int64) {
	counts := make(map[string]int, len(before)+len(after))
	for _, row := range before {
		key, _ := json.Marshal(row)
		counts[string(key)]++
	}
	for _, row := range after {
		key, _ := json.Marshal(row)
		counts[string(key)]--
	}
	for _, count := range counts {
		if count > 0 {
			removed += int64(count)
		} else if count < 0 {
			added += int64(-count)
		}
	}
	return removed, added
}
