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

const persistentTargetExpression = "`?([A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+){1,2})`?"

var (
	insertTargetPattern            = regexp.MustCompile("(?is)^\\s*INSERT\\s+(?:INTO\\s+)?" + persistentTargetExpression + "(?:\\s|\\()")
	updateTargetPattern            = regexp.MustCompile("(?is)^\\s*UPDATE\\s+" + persistentTargetExpression + "(?:\\s|$)")
	deleteTargetPattern            = regexp.MustCompile("(?is)^\\s*DELETE\\s+(?:FROM\\s+)?" + persistentTargetExpression + "(?:\\s|$)")
	mergeTargetPattern             = regexp.MustCompile("(?is)^\\s*MERGE\\s+(?:INTO\\s+)?" + persistentTargetExpression + "(?:\\s|$)")
	truncateTargetPattern          = regexp.MustCompile("(?is)^\\s*TRUNCATE\\s+TABLE\\s+" + persistentTargetExpression + "(?:\\s|$)")
	createTargetPattern            = regexp.MustCompile("(?is)^\\s*CREATE\\s+(OR\\s+REPLACE\\s+)?TABLE\\s+(IF\\s+NOT\\s+EXISTS\\s+)?" + persistentTargetExpression + "(?:\\s|\\(|$)")
	dropTargetPattern              = regexp.MustCompile("(?is)^\\s*DROP\\s+TABLE\\s+(IF\\s+EXISTS\\s+)?" + persistentTargetExpression + "\\s*$")
	persistentLeadPattern          = regexp.MustCompile(`(?is)^\s*(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE|DROP\s+TABLE)\b`)
	unsupportedMutationLeadPattern = regexp.MustCompile(`(?is)^\s*(ALTER\s+TABLE|CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:MATERIALIZED\s+)?VIEW|TEMP(?:ORARY)?\s+TABLE|(?:TEMP(?:ORARY)?\s+)?(?:SCHEMA|FUNCTION|PROCEDURE|MODEL))|DROP\s+(?:MATERIALIZED\s+)?VIEW|DROP\s+(?:SCHEMA|FUNCTION|PROCEDURE|MODEL)|CALL|GRANT|REVOKE|EXPORT\s+DATA|LOAD\s+DATA|BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK)\b`)
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
		return persistentSQLStatement{}, true, fmt.Errorf("unsupported DDL/DML target syntax; use dataset.table or project.dataset.table, optionally enclosed in one pair of backticks")
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
	switch len(parts) {
	case 2:
		return tableReference{ProjectID: defaultProjectID, DatasetID: parts[0], TableID: parts[1]}, nil
	case 3:
		return tableReference{ProjectID: parts[0], DatasetID: parts[1], TableID: parts[2]}, nil
	default:
		return tableReference{}, fmt.Errorf("invalid DDL/DML target %q: expected dataset.table or project.dataset.table", raw)
	}
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
	db, processedBytes, err := s.openMaterializedSQLDatabase(projectID, queryText, map[string]bool{}, sess, []datasetTableRef{ref})
	if err != nil {
		return persistentSQLResult{}, true, err
	}
	defer db.Close()
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
