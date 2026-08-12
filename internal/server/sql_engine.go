package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	_ "github.com/goccy/googlesqlite"
)

// realSQLScalarTypes are the tableField.Type values passed straight through
// to CREATE TABLE as GoogleSQL column types, confirmed accepted by
// goccy/googlesqlite. Anything else falls back to STRING rather than
// failing materialization outright, since nothing upstream validates
// tableField.Type against a fixed enum today.
var realSQLScalarTypes = map[string]bool{
	"INT64": true, "FLOAT64": true, "STRING": true, "BOOL": true,
	"BYTES": true, "DATE": true, "DATETIME": true, "TIME": true, "TIMESTAMP": true,
	"NUMERIC": true, "BIGNUMERIC": true,
}

// isRecordType reports whether a schema field's Type names a nested
// STRUCT/RECORD (BigQuery calls the schema-declared type RECORD; GoogleSQL
// itself calls the type STRUCT — both spellings are accepted as input and
// treated identically here).
func isRecordType(t string) bool {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "RECORD", "STRUCT":
		return true
	}
	return false
}

// sqliteColumnType renders a tableField as a GoogleSQL column type for
// CREATE TABLE: RECORD becomes STRUCT<...> (recursively), REPEATED wraps
// the base (non-repeated) type in ARRAY<...>, and unrecognized scalar names
// fall back to STRING rather than failing materialization outright.
func sqliteColumnType(field tableField) string {
	base := field
	base.Mode = ""
	var rendered string
	if isRecordType(field.Type) {
		parts := make([]string, len(field.Fields))
		for i, f := range field.Fields {
			parts[i] = quoteIdent(f.Name) + " " + sqliteColumnType(f)
		}
		rendered = "STRUCT<" + strings.Join(parts, ", ") + ">"
	} else {
		t := strings.ToUpper(strings.TrimSpace(field.Type))
		if !realSQLScalarTypes[t] {
			t = "STRING"
		}
		rendered = t
	}
	if field.Mode == "REPEATED" {
		return "ARRAY<" + rendered + ">"
	}
	return rendered
}

func quoteIdent(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

// convertScalarForInsert parses one lossless stored scalar cell into a typed
// value for a parameterized INSERT. The explicit null tag is SQL NULL for
// every type; an empty STRING remains a real empty string. Empty numeric and
// boolean cells retain the legacy-null behavior because those types cannot
// represent an empty string and old CSV-backed rows used that convention.
func convertScalarForInsert(fieldType, cell string) (any, error) {
	var isNull bool
	cell, isNull = loadStoredCell(cell)
	if isNull {
		return nil, nil
	}
	switch strings.ToUpper(strings.TrimSpace(fieldType)) {
	case "INT64":
		if cell == "" {
			return nil, nil
		}
		return strconv.ParseInt(cell, 10, 64)
	case "FLOAT64":
		if cell == "" {
			return nil, nil
		}
		return strconv.ParseFloat(cell, 64)
	case "BOOL":
		if cell == "" {
			return nil, nil
		}
		return strconv.ParseBool(cell)
	default:
		return cell, nil
	}
}

// buildInsertValueExpr renders one top-level column's stored cell into a SQL
// value expression plus the bound parameters it references, for a column
// that is RECORD and/or REPEATED. RECORD cells are stored as a canonical
// JSON object (keyed by field name); REPEATED cells as a canonical JSON
// array (see cellFromScannedValue) — both parsed back here.
//
// Every scalar leaf is bound as CAST(? AS <type>), not a bare "?": verified
// empirically that a bare "?" inside an ARRAY/STRUCT literal is typed by the
// analyzer before the bound value is known and silently defaults to INT64,
// producing wrong results (or an analysis error) for any other type — this
// is not a Go-side concern (map/slice parameter binding was tried first and
// found unreliable for the same underlying reason) but a real quirk of this
// query engine's placeholder-type inference inside literal expressions.
func buildInsertValueExpr(field tableField, cell string) (string, []any, error) {
	decodedCell, isNull := loadStoredCell(cell)
	if isNull {
		return "NULL", nil, nil
	}
	cell = decodedCell
	if field.Mode == "REPEATED" {
		if cell == "" {
			return "NULL", nil, nil
		}
		var decoded []any
		if err := json.Unmarshal([]byte(cell), &decoded); err != nil {
			return "", nil, fmt.Errorf("column %s: invalid REPEATED JSON: %w", field.Name, err)
		}
		if len(decoded) == 0 {
			return "[]", nil, nil
		}
		base := field
		base.Mode = ""
		parts := make([]string, len(decoded))
		var params []any
		for i, e := range decoded {
			expr, p, err := exprForDecodedValue(base, e)
			if err != nil {
				return "", nil, err
			}
			parts[i] = expr
			params = append(params, p...)
		}
		return "[" + strings.Join(parts, ", ") + "]", params, nil
	}
	if isRecordType(field.Type) {
		if cell == "" {
			return "NULL", nil, nil
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(cell), &decoded); err != nil {
			return "", nil, fmt.Errorf("column %s: invalid RECORD JSON: %w", field.Name, err)
		}
		return structExprForDecodedValue(field, decoded)
	}
	v, err := convertScalarForInsert(field.Type, cell)
	if err != nil {
		return "", nil, err
	}
	return "CAST(? AS " + sqliteColumnType(tableField{Type: field.Type}) + ")", []any{v}, nil
}

// structExprForDecodedValue renders a decoded JSON object as a
// STRUCT(<expr> AS name, ...) literal, recursively.
func structExprForDecodedValue(field tableField, obj map[string]any) (string, []any, error) {
	parts := make([]string, len(field.Fields))
	var params []any
	for i, sub := range field.Fields {
		expr, p, err := exprForDecodedValue(sub, obj[sub.Name])
		if err != nil {
			return "", nil, err
		}
		parts[i] = expr + " AS " + quoteIdent(sub.Name)
		params = append(params, p...)
	}
	return "STRUCT(" + strings.Join(parts, ", ") + ")", params, nil
}

// exprForDecodedValue is buildInsertValueExpr's counterpart for a value that
// has already been json.Unmarshal-decoded (used for elements inside a
// REPEATED array and for RECORD field values, neither of which has a
// separate raw string of its own). A JSON number is always float64, per
// encoding/json.
func exprForDecodedValue(field tableField, decoded any) (string, []any, error) {
	if decoded == nil {
		return "NULL", nil, nil
	}
	if isRecordType(field.Type) {
		obj, ok := decoded.(map[string]any)
		if !ok {
			return "", nil, fmt.Errorf("column %s: expected a JSON object for RECORD, got %T", field.Name, decoded)
		}
		return structExprForDecodedValue(field, obj)
	}
	var v any
	switch strings.ToUpper(strings.TrimSpace(field.Type)) {
	case "INT64":
		switch d := decoded.(type) {
		case float64:
			v = int64(d)
		case string:
			parsed, err := strconv.ParseInt(d, 10, 64)
			if err != nil {
				return "", nil, err
			}
			v = parsed
		default:
			v = decoded
		}
	case "FLOAT64":
		if d, ok := decoded.(string); ok {
			parsed, err := strconv.ParseFloat(d, 64)
			if err != nil {
				return "", nil, err
			}
			v = parsed
		} else {
			v = decoded
		}
	case "BOOL":
		if d, ok := decoded.(string); ok {
			parsed, err := strconv.ParseBool(d)
			if err != nil {
				return "", nil, err
			}
			v = parsed
		} else {
			v = decoded
		}
	default:
		v = fmt.Sprintf("%v", decoded)
	}
	return "CAST(? AS " + sqliteColumnType(tableField{Type: field.Type}) + ")", []any{v}, nil
}

// legacyTableRefPattern is only a compatibility fallback when go-googlesql
// cannot parse a statement. Valid GoogleSQL uses the AST traversal below, so
// comma joins, nested subqueries and CTE bodies are discovered without
// matching table-like text inside comments, literals or USING column lists.
var legacyTableRefPattern = regexp.MustCompile("(?i)\\b(?:FROM|JOIN)\\s+`?([A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+){1,2})`?")

type datasetTableRef struct {
	datasetID string
	tableID   string
}

// referencedTables extracts every real table path from the parsed statement's
// complete AST. A 3-part reference is only kept when its leading component
// matches projectID (case-insensitive); other projects are not materialized,
// so a genuine cross-project reference fails naturally with "table not found"
// from the real engine rather than silently resolving against the wrong
// project. A parser failure retains the legacy regex behavior so the embedded
// engine, rather than table discovery, remains authoritative for the final SQL
// error and for syntax accepted by googlesqlite ahead of this parser wrapper.
func referencedTables(queryText, projectID string) []datasetTableRef {
	refs, err := referencedTablesFromAST(queryText, projectID)
	if err == nil {
		return refs
	}
	return referencedTablesLegacy(queryText, projectID)
}

func referencedTablesFromAST(queryText, projectID string) ([]datasetTableRef, error) {
	statement, err := parseGoogleSQLStatement(queryText)
	if err != nil {
		return nil, err
	}

	seen := map[datasetTableRef]bool{}
	var refs []datasetTableRef
	err = walkGoogleSQLAST(statement, func(node googlesql.ASTNode) error {
		if tablePath, ok := node.(*googlesql.ASTTablePathExpression); ok {
			path, pathErr := tablePath.PathExpr()
			if pathErr != nil {
				return pathErr
			}
			if path != nil {
				parts, vectorErr := path.ToIdentifierVector()
				if vectorErr != nil {
					return vectorErr
				}
				appendDatasetTableRef(parts, projectID, seen, &refs)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

func parseGoogleSQLStatement(queryText string) (googlesql.ASTStatementNode, error) {
	// The database/sql driver initializes this same global runtime lazily on
	// its first connection. AST discovery runs before that connection exists,
	// so initialize it here as well; Init is explicitly sync.Once-backed and
	// safe to call repeatedly and concurrently.
	if err := googlesql.Init(); err != nil {
		return nil, err
	}
	opts, err := googlesql.NewParserOptions()
	if err != nil {
		return nil, err
	}
	parsed, err := googlesql.ParseStatement(queryText, opts)
	if err != nil {
		return nil, err
	}
	return parsed.Statement()
}

func walkGoogleSQLAST(root googlesql.ASTNode, visit func(googlesql.ASTNode) error) error {
	var walk func(googlesql.ASTNode) error
	walk = func(node googlesql.ASTNode) error {
		if err := visit(node); err != nil {
			return err
		}
		children, childErr := node.NumChildren()
		if childErr != nil {
			return childErr
		}
		for i := int32(0); i < children; i++ {
			child, err := node.Child(i)
			if err != nil {
				return err
			}
			if child != nil {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(root)
}

func referencedTablesLegacy(queryText, projectID string) []datasetTableRef {
	seen := map[datasetTableRef]bool{}
	var refs []datasetTableRef
	for _, m := range legacyTableRefPattern.FindAllStringSubmatch(queryText, -1) {
		appendDatasetTableRef(strings.Split(m[1], "."), projectID, seen, &refs)
	}
	return refs
}

func appendDatasetTableRef(parts []string, projectID string, seen map[datasetTableRef]bool, refs *[]datasetTableRef) {
	// BigQuery's canonical quoted spelling (`project.dataset.table`) is
	// represented by this parser version as one identifier containing dots,
	// while the unquoted spelling is represented as three identifiers.
	if len(parts) == 1 && strings.Contains(parts[0], ".") {
		parts = strings.Split(parts[0], ".")
	}
	var ref datasetTableRef
	switch len(parts) {
	case 2:
		ref = datasetTableRef{datasetID: parts[0], tableID: parts[1]}
	case 3:
		if !strings.EqualFold(parts[0], projectID) {
			return
		}
		ref = datasetTableRef{datasetID: parts[1], tableID: parts[2]}
	default:
		return
	}
	if !seen[ref] {
		seen[ref] = true
		*refs = append(*refs, ref)
	}
}

// stripProjectPrefix rewrites project.dataset.table references down to
// dataset.table, since the real engine models one schema per dataset and has
// no concept of a project level. Three quoting styles are handled: bare, a
// single pair of backticks around the whole reference, and a pair around
// each segment individually (dbt-bigquery's default DDL macros emit this
// last style — a backtick around each of project, dataset and table
// individually. Leaving that style
// unstripped is not just a cosmetic miss: a 3-part project-qualified
// CREATE/REPLACE reaching the embedded engine directly was found to leave
// its catalog in a state where a same-named 2-part table materialized fresh
// afterward (this project's own re-materialize-per-statement pattern, see
// pooledEngine.reset) silently retained the earlier statement's row(s)
// alongside the new ones — a real, reproducible upstream duplication bug
// (see this project's own test coverage and devlog) triggered specifically
// by that unstripped 3-part form, not merely an unsupported syntax error.
func stripProjectPrefix(queryText, projectID string) string {
	if projectID == "" {
		return queryText
	}
	project := regexp.QuoteMeta(projectID)
	// A single pair of backticks quotes the whole BigQuery reference. Consume
	// both of them together; the previous optional-backtick expression removed
	// only the opening tick and left an invalid trailing tick behind.
	wholeQuoted := regexp.MustCompile("(?i)`" + project + "\\.([A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+)`")
	queryText = wholeQuoted.ReplaceAllString(queryText, "$1")
	// Per-segment quoting: the project's own backticks are consumed together
	// with the following dot; the dataset/table segments are left exactly as
	// spelled (still individually backtick-quoted or not) since that part of
	// the reference is already a form the engine accepts natively.
	perSegmentQuoted := regexp.MustCompile("(?i)`" + project + "`\\.(`?[A-Za-z0-9_-]+`?\\.`?[A-Za-z0-9_-]+`?)")
	queryText = perSegmentQuoted.ReplaceAllString(queryText, "$1")
	bare := regexp.MustCompile("(?i)\\b" + project + "\\.([A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+)\\b")
	return bare.ReplaceAllString(queryText, "$1")
}

// sqlTypeDescriptor mirrors the JSON shape of googlesqlite's
// ColumnType.DatabaseTypeName(): a scalar has just Name; ARRAY<T> has
// ElementType set to T's descriptor; STRUCT<...> has FieldTypes set to its
// nested named fields (each recursively the same shape).
type sqlTypeDescriptor struct {
	Name        string             `json:"name"`
	ElementType *sqlTypeDescriptor `json:"elementType"`
	FieldTypes  []sqlNamedType     `json:"fieldTypes"`
}

type sqlNamedType struct {
	Name string            `json:"name"`
	Type sqlTypeDescriptor `json:"type"`
}

// sqlColumnToTableField parses a googlesqlite DatabaseTypeName() JSON blob
// into a real tableField — recursively, so a query result column that is
// STRUCT or ARRAY<...> (including ARRAY<STRUCT<...>>) gets a genuine nested
// schema (Type/Mode/Fields), the same shape a real table's schema uses, not
// just a flattened type-name string.
func sqlColumnToTableField(name, databaseTypeName string) tableField {
	var desc sqlTypeDescriptor
	if err := json.Unmarshal([]byte(databaseTypeName), &desc); err != nil || desc.Name == "" {
		return tableField{Name: name, Type: "STRING"}
	}
	return sqlTypeDescriptorToField(name, desc)
}

func sqlTypeDescriptorToField(name string, desc sqlTypeDescriptor) tableField {
	if desc.ElementType != nil {
		elem := sqlTypeDescriptorToField(name, *desc.ElementType)
		elem.Mode = "REPEATED"
		return elem
	}
	if len(desc.FieldTypes) > 0 {
		fields := make([]tableField, len(desc.FieldTypes))
		for i, ft := range desc.FieldTypes {
			fields[i] = sqlTypeDescriptorToField(ft.Name, ft.Type)
		}
		return tableField{Name: name, Type: "RECORD", Fields: fields}
	}
	t := desc.Name
	if t == "DOUBLE" {
		t = "FLOAT64"
	}
	return tableField{Name: name, Type: t}
}

// executeRealSQLQuery runs queryText for real against a fresh in-memory
// GoogleSQL engine (github.com/goccy/googlesqlite): every table the query
// references is materialized into it first, so WHERE, column projection,
// JOIN, aggregation, ORDER BY and LIMIT are real GoogleSQL semantics rather
// than the previous regex-matched full-table-dump/fabricated fallback. sess
// is nil unless the query is running within a BigQuery-style session, in
// which case a `_SESSION.<table>` reference resolves against that session's
// own temp-table catalog instead of the real dataset catalog (see
// session_service.go).
func (s *Server) executeRealSQLQuery(projectID, queryText string, sess *sessionRecord) ([]tableField, [][]string, error) {
	return s.executeRealSQLQueryVisiting(projectID, queryText, map[string]bool{}, sess)
}

// executeRealSQLQueryWithParams is executeRealSQLQuery plus real BigQuery
// queryParameters (see query_parameters.go) bound into the query via Go's
// standard database/sql mechanism (sql.Named for NAMED mode, positional
// args for POSITIONAL) rather than left for the client's `@name`/`?`
// placeholders to fail unbound in the analyzer.
func (s *Server) executeRealSQLQueryWithParams(projectID, queryText string, sess *sessionRecord, paramMode string, params []storedQueryParameter) ([]tableField, [][]string, error) {
	return s.executeRealSQLQueryVisitingWithParams(projectID, queryText, map[string]bool{}, sess, paramMode, params)
}

// executeRealSQLQueryVisiting is executeRealSQLQuery plus the view-cycle
// guard threaded through resolveTableRowsVisiting: a referenced table may
// itself be a view, which is resolved by recursively executing its own
// query here.
func (s *Server) executeRealSQLQueryVisiting(projectID, queryText string, visiting map[string]bool, sess *sessionRecord) ([]tableField, [][]string, error) {
	return s.executeRealSQLQueryVisitingWithParams(projectID, queryText, visiting, sess, "", nil)
}

// executeRealSQLQueryVisitingWithParams is executeRealSQLQueryVisiting plus
// query parameter binding; every other caller (views, Storage Read, session
// temp-table resolution) has no client-supplied parameters and goes through
// executeRealSQLQueryVisiting/executeRealSQLQuery with none.
func (s *Server) executeRealSQLQueryVisitingWithParams(projectID, queryText string, visiting map[string]bool, sess *sessionRecord, paramMode string, params []storedQueryParameter) ([]tableField, [][]string, error) {
	schema, rows, _, err := s.executeRealSQLQueryVisitingWithParamsAndStats(projectID, queryText, visiting, sess, paramMode, params)
	return schema, rows, err
}

// executeRealSQLQueryVisitingWithParamsAndStats additionally reports the
// logical bytes read from catalog/session source rows. This is deliberately
// separate from output row bytes: SELECT COUNT(*) over a large table scans the
// table even though its result is tiny. Proven equality predicates can prune
// logical partitions; physical column pruning and billing parity remain out of
// scope for this metric.
func (s *Server) executeRealSQLQueryVisitingWithParamsAndStats(projectID, queryText string, visiting map[string]bool, sess *sessionRecord, paramMode string, params []storedQueryParameter) ([]tableField, [][]string, int64, error) {
	db, release, processedBytes, err := s.openMaterializedSQLDatabase(projectID, queryText, visiting, sess, nil, true)
	if err != nil {
		return nil, nil, 0, err
	}
	defer release()

	args, err := buildQueryArgs(paramMode, params)
	if err != nil {
		return nil, nil, 0, err
	}
	engineQuery := rewriteIngestionPseudocolumns(stripProjectPrefix(queryText, projectID))
	rows, err := db.Query(engineQuery, args...)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()
	schema, resultRows, err := scanRealSQLRows(rows)
	if err != nil {
		return nil, nil, 0, err
	}
	schema, resultRows, err = hideWildcardIngestionPseudocolumns(queryText, schema, resultRows)
	return schema, resultRows, processedBytes, err
}

// expandWildcardTableRefs replaces a wildcard table reference
// (`dataset.prefix*`, real BigQuery's `_TABLE_SUFFIX` syntax) with the
// concrete real tables in that dataset whose name starts with prefix, so
// each one gets materialized individually through the normal per-ref path
// below. This is not this project's own union logic: the embedded engine
// (goccy/googlesqlite, see its internal/wildcard_table.go) already resolves
// a `*`-suffixed table name natively into a `UNION ALL` with a computed
// `_TABLE_SUFFIX` column, but only once every matching real table already
// exists in its own catalog for that dataset — LocaQL's job is only to
// decide, correctly, which real tables that is. Non-wildcard refs pass
// through unchanged. Views and external tables are never matched: real
// BigQuery wildcard tables only span native tables. A wildcard matching zero
// tables is not an error here — the embedded engine reports its own clear
// error ("failed to find matched tables by wildcard") when the query
// actually runs against an empty schema, which is more informative than
// this project fabricating an earlier one from a catalog listing.
func (s *Server) expandWildcardTableRefs(projectID string, refs []datasetTableRef) []datasetTableRef {
	out := make([]datasetTableRef, 0, len(refs))
	for _, ref := range refs {
		prefix, isWildcard := strings.CutSuffix(ref.tableID, "*")
		if !isWildcard {
			out = append(out, ref)
			continue
		}
		for _, t := range s.tables.listAll(projectID, ref.datasetID) {
			if t.View != nil || t.External != nil {
				continue
			}
			if strings.HasPrefix(t.TableID, prefix) {
				out = append(out, datasetTableRef{datasetID: ref.datasetID, tableID: t.TableID})
			}
		}
	}
	return out
}

// openMaterializedSQLDatabase builds the isolated GoogleSQL database used by
// both read-only queries and persistent DDL/DML. extraRefs is primarily the
// mutation target: INSERT/UPDATE/MERGE targets do not necessarily occur after
// FROM/JOIN, while a CREATE TABLE target needs its dataset schema created even
// though the table does not exist yet. allowPartitionPruning must remain false
// for persistent mutations so their materialized source cannot be narrowed.
// resolvedTableMaterialization is what one referenced table resolves to,
// computed before any engine is acquired — see openMaterializedSQLDatabase.
type resolvedTableMaterialization struct {
	ref            datasetTableRef
	fields         []tableField
	rows           [][]string
	found          bool
	version        int
	hasVersion     bool // false for session-scoped temp tables, or a ref not found in the catalog at all
	processedBytes int64
}

// materializationPlan groups the inputs to resolveTableForMaterialization
// that stay constant across every referenced table in one
// openMaterializedSQLDatabase call, so per-ref calls only need to add what
// actually varies per ref.
type materializationPlan struct {
	projectID             string
	queryText             string
	sess                  *sessionRecord
	visiting              map[string]bool
	allowPartitionPruning bool
	singleRef             bool
}

// catalogTableForRef resolves ref's catalogTable (nil for a session-scoped
// ref, or one not yet in the catalog) and enforces requirePartitionFilter
// before any row data is read — exactly the two lookups
// openMaterializedSQLDatabase's per-ref loop always did.
func (s *Server) catalogTableForRef(plan materializationPlan, ref datasetTableRef, requireFilterCheck bool) (*tableRecord, error) {
	if strings.EqualFold(ref.datasetID, sessionDatasetName) {
		return nil, nil
	}
	table, ok, _ := s.tables.get(plan.projectID, ref.datasetID, ref.tableID)
	if !ok {
		return nil, nil
	}
	if requireFilterCheck && table.RequirePartitionFilter && !queryHasPartitionFilter(plan.queryText, table) {
		return nil, fmt.Errorf("cannot query over table %s.%s without a filter on its partitioning column", ref.datasetID, ref.tableID)
	}
	return table, nil
}

// pruneIfEligible applies this project's existing single-table partition
// pruning and reports whether it actually narrowed rows — a pruned result
// must never be cached (see resolveTableForMaterialization), since it is a
// subset of the table, not the table itself.
func pruneIfEligible(plan materializationPlan, ref datasetTableRef, catalogTable *tableRecord, rows [][]string) ([][]string, *tableRecord, bool) {
	if !plan.allowPartitionPruning || catalogTable == nil || !plan.singleRef {
		return rows, catalogTable, false
	}
	prunedRows, prunedPartitions, pruned := prunePartitionedRows(plan.queryText, plan.projectID, ref, catalogTable, rows)
	if !pruned {
		return rows, catalogTable, false
	}
	if catalogTable.TimePartitioning != nil && catalogTable.TimePartitioning.Field == "" {
		materializedTable := *catalogTable
		materializedTable.IngestionPartitions = prunedPartitions
		catalogTable = &materializedTable
	}
	return prunedRows, catalogTable, true
}

// resolveTableForMaterialization reproduces exactly what
// openMaterializedSQLDatabase's per-ref loop always did — the
// partition-filter check, session-temp-vs-catalog resolution, and partition
// pruning — but returns its result instead of writing straight into a
// pooled engine, so the caller can decide *which* engine to write it into
// (a fresh/private one, or one already shared and up to date) only after
// every ref has been resolved.
func (s *Server) resolveTableForMaterialization(plan materializationPlan, ref datasetTableRef, requireFilterCheck bool) (resolvedTableMaterialization, error) {
	catalogTable, err := s.catalogTableForRef(plan, ref, requireFilterCheck)
	if err != nil {
		return resolvedTableMaterialization{}, err
	}

	r := resolvedTableMaterialization{ref: ref}
	if plan.sess != nil && strings.EqualFold(ref.datasetID, sessionDatasetName) {
		if t, ok := plan.sess.getTempTable(ref.tableID); ok {
			r.fields, r.rows, r.found = t.Fields, t.Rows, true
		}
		return r, nil // session-scoped: hasVersion stays false, never cacheable
	}

	fields, rows, found, err := s.resolveTableRowsVisiting(plan.projectID, ref.datasetID, ref.tableID, plan.visiting)
	if err != nil {
		return resolvedTableMaterialization{}, err
	}
	r.found = found
	if !found {
		return r, nil
	}
	if catalogTable != nil && catalogTable.External == nil && catalogTable.View == nil {
		// Two cases whose Version cannot stand in for "the materialized
		// rows are still correct": an external table's Version never
		// changes when its underlying file does (see
		// TestExternalTableQueryReflectsLiveFileContents), and a view's
		// Version only changes when the view's own query text changes, not
		// when a base table it selects from does — resolveTableRowsVisiting
		// always re-executes the view query fresh regardless, so disabling
		// the cache here costs nothing beyond that recursive resolution,
		// which itself still benefits from the shared cache on the base
		// tables it references.
		r.version, r.hasVersion = catalogTable.Version, true
	}
	var pruned bool
	rows, catalogTable, pruned = pruneIfEligible(plan, ref, catalogTable, rows)
	if pruned {
		r.hasVersion = false // a pruned subset must never be mistaken for the whole table by a later cache lookup
	}
	r.processedBytes = estimateRowsByteSize(rows)
	r.fields, r.rows = materializeIngestionPseudocolumns(catalogTable, fields, rows)
	return r, nil
}

// openMaterializedSQLDatabase resolves every table queryText references,
// then acquires an embedded-engine connection to run it against: a private,
// exclusively-owned one for anything that can mutate a base table or touches
// session-scoped/pruned data (matching this project's original,
// materialize-fresh-every-time behavior exactly), or — for a plain read-only
// query whose referenced tables are all still at the exact catalog version
// they were last materialized at — a connection possibly already shared
// with other concurrent read-only queries wanting the identical table set,
// skipping materialization entirely on a hit. See sql_engine_pool.go for
// why table versions make this safe, and docs/benchmarks.md for the
// profiling that motivated it: materializing a 50k-row table into a fresh
// engine on every single query was 74% of that query's total time.
func (s *Server) openMaterializedSQLDatabase(projectID, queryText string, visiting map[string]bool, sess *sessionRecord, extraRefs []datasetTableRef, allowPartitionPruning bool) (db *sql.DB, release func(), processedBytes int64, err error) {
	combined, requireFilterCheck, singleRef := combinedTableRefs(s, projectID, queryText, extraRefs)
	plan := materializationPlan{projectID: projectID, queryText: queryText, sess: sess, visiting: visiting, allowPartitionPruning: allowPartitionPruning, singleRef: singleRef}
	resolved, cacheable, resolveErr := s.resolveAllRefs(plan, combined, requireFilterCheck, !isMutatingOrSessionControlStatement(projectID, queryText))
	if resolveErr != nil {
		return nil, nil, 0, resolveErr
	}

	sig := cacheSignature(cacheable, resolved)
	pe, hit, shared, err := s.sqlEngines.acquire(sig)
	if err != nil {
		return nil, nil, 0, err
	}
	release = releaseFunc(s.sqlEngines, pe, shared)

	for _, r := range resolved {
		processedBytes += r.processedBytes
	}
	if hit {
		return pe.db, release, processedBytes, nil
	}
	if err := materializeAllResolved(pe, resolved, shared); err != nil {
		release()
		return nil, nil, 0, err
	}
	if shared {
		s.sqlEngines.tagSignature(pe, sig)
	}
	return pe.db, release, processedBytes, nil
}

// combinedTableRefs merges queryText's wildcard-expanded table references
// with extraRefs (deduplicated), and reports which of them need the
// require-partition-filter check (every directly referenced table, plus an
// UPDATE/DELETE/MERGE target) and whether refs resolved to exactly one
// table — the condition under which single-table partition pruning applies.
func combinedTableRefs(s *Server, projectID, queryText string, extraRefs []datasetTableRef) (combined []datasetTableRef, requireFilterCheck map[datasetTableRef]bool, singleRef bool) {
	refs := s.expandWildcardTableRefs(projectID, referencedTables(queryText, projectID))
	requireFilterCheck = make(map[datasetTableRef]bool, len(refs)+len(extraRefs))
	for _, ref := range refs {
		requireFilterCheck[ref] = true
	}
	if statement, handled, parseErr := parsePersistentSQLStatement(projectID, queryText); handled && parseErr == nil {
		switch statement.statementType {
		case "UPDATE", "DELETE", "MERGE":
			requireFilterCheck[datasetTableRef{datasetID: statement.target.DatasetID, tableID: statement.target.TableID}] = true
		}
	}
	seen := make(map[datasetTableRef]bool, len(refs)+len(extraRefs))
	combined = make([]datasetTableRef, 0, len(refs)+len(extraRefs))
	for _, ref := range append(refs, extraRefs...) {
		if seen[ref] {
			continue
		}
		seen[ref] = true
		combined = append(combined, ref)
	}
	return combined, requireFilterCheck, len(refs) == 1
}

// resolveAllRefs resolves every ref in combined and reports whether the
// whole set remains cacheable — it stops being so the moment any resolved
// ref lacks a stable catalog version (session-scoped or partition-pruned).
func (s *Server) resolveAllRefs(plan materializationPlan, combined []datasetTableRef, requireFilterCheck map[datasetTableRef]bool, cacheable bool) ([]resolvedTableMaterialization, bool, error) {
	resolved := make([]resolvedTableMaterialization, 0, len(combined))
	for _, ref := range combined {
		r, err := s.resolveTableForMaterialization(plan, ref, requireFilterCheck[ref])
		if err != nil {
			return nil, false, err
		}
		if !r.hasVersion {
			cacheable = false
		}
		resolved = append(resolved, r)
	}
	return resolved, cacheable, nil
}

// cacheSignature computes the shared-cache signature for resolved, or ""
// when the set isn't cacheable at all — callers treat an empty signature as
// "always acquire a private, exclusively-owned connection."
func cacheSignature(cacheable bool, resolved []resolvedTableMaterialization) string {
	if !cacheable {
		return ""
	}
	cacheRefs := make([]cacheableTableRef, 0, len(resolved))
	for _, r := range resolved {
		cacheRefs = append(cacheRefs, cacheableTableRef{datasetID: r.ref.datasetID, tableID: r.ref.tableID, version: r.version})
	}
	return tableSignature(cacheRefs)
}

// releaseFunc closes over which release path pe must go through, so
// openMaterializedSQLDatabase's callers can defer it without knowing
// whether the connection ended up shared or private.
func releaseFunc(pool *sqlEnginePool, pe *pooledEngine, shared bool) func() {
	return func() {
		if shared {
			pool.releaseShared(pe)
		} else {
			pool.releasePrivate(pe)
		}
	}
}

// materializeAllResolved reconciles pe (for a shared connection being
// repurposed to a new signature) and then materializes every resolved ref
// pe doesn't already have at the right version.
func materializeAllResolved(pe *pooledEngine, resolved []resolvedTableMaterialization, shared bool) error {
	if shared {
		if err := reconcileSharedEngine(pe, resolved); err != nil {
			return fmt.Errorf("reconcile shared engine: %w", err)
		}
	}
	for _, r := range resolved {
		if err := materializeResolvedRef(pe, r); err != nil {
			return err
		}
	}
	return nil
}

// reconcileSharedEngine drops every table pe currently has materialized that
// is not part of wanted — necessary before reusing a connection that
// previously served a *different* signature (a fresh or exact-signature-hit
// connection never has anything extra, by construction).
func reconcileSharedEngine(pe *pooledEngine, wanted []resolvedTableMaterialization) error {
	want := make(map[string]bool, len(wanted))
	for _, r := range wanted {
		want[r.ref.datasetID+"."+r.ref.tableID] = true
	}
	for key := range pe.tables {
		if want[key] {
			continue
		}
		datasetID, tableID := splitTableKey(key)
		if err := pe.dropMaterialized(datasetID, tableID); err != nil {
			return err
		}
	}
	return nil
}

// materializeResolvedRef brings pe's copy of r's table in line with r,
// skipping the real materialization work entirely when pe already has it at
// the exact version r resolved to.
func materializeResolvedRef(pe *pooledEngine, r resolvedTableMaterialization) error {
	if existingVersion, ok := pe.materializedVersion(r.ref.datasetID, r.ref.tableID); ok && r.hasVersion && existingVersion == r.version {
		return nil
	}
	if err := pe.ensureSchema(r.ref.datasetID); err != nil {
		return fmt.Errorf("materialize dataset %s: %w", r.ref.datasetID, err)
	}
	// Marked unconditionally, not only after a successful materializeTable
	// below: a DDL/DML target that does not exist yet in LocaQL's own
	// catalog (found == false, e.g. a fresh CREATE TABLE) is never passed to
	// materializeTable, but the statement the caller runs against this same
	// connection right after openMaterializedSQLDatabase returns creates it
	// for real on the engine regardless — release must drop it before this
	// connection is reused, or the next reuse's CREATE TABLE for the same
	// name fails with "table already exists" (a real bug caught by
	// TestPersistentCreateTableSchemaAndReplace while building this).
	pe.markTableMaterialized(r.ref.datasetID, r.ref.tableID, r.version)
	if !r.found {
		return nil
	}
	if err := materializeTable(pe.db, r.ref.datasetID, r.ref.tableID, r.fields, r.rows); err != nil {
		return fmt.Errorf("materialize table %s.%s: %w", r.ref.datasetID, r.ref.tableID, err)
	}
	return nil
}

func splitTableKey(key string) (datasetID, tableID string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

// rewriteIngestionPseudocolumns replaces public pseudocolumn identifiers only
// in executable SQL tokens. String literals and comments are copied verbatim,
// so a value such as '_PARTITIONDATE' does not accidentally become different
// data. Backtick-quoted standalone pseudocolumn identifiers are supported too.
func rewriteIngestionPseudocolumns(queryText string) string {
	var out strings.Builder
	out.Grow(len(queryText))
	for i := 0; i < len(queryText); {
		if i+1 < len(queryText) && queryText[i] == '-' && queryText[i+1] == '-' {
			end := strings.IndexByte(queryText[i+2:], '\n')
			if end < 0 {
				out.WriteString(queryText[i:])
				break
			}
			end += i + 3
			out.WriteString(queryText[i:end])
			i = end
			continue
		}
		if i+1 < len(queryText) && queryText[i] == '/' && queryText[i+1] == '*' {
			end := strings.Index(queryText[i+2:], "*/")
			if end < 0 {
				out.WriteString(queryText[i:])
				break
			}
			end += i + 4
			out.WriteString(queryText[i:end])
			i = end
			continue
		}
		if queryText[i] == '\'' || queryText[i] == '"' {
			quote := queryText[i]
			start := i
			i++
			for i < len(queryText) {
				if queryText[i] == quote {
					if i+1 < len(queryText) && queryText[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			out.WriteString(queryText[start:i])
			continue
		}
		if queryText[i] == '`' {
			end := strings.IndexByte(queryText[i+1:], '`')
			if end < 0 {
				out.WriteString(queryText[i:])
				break
			}
			end += i + 1
			identifier := queryText[i+1 : end]
			switch strings.ToUpper(identifier) {
			case "_PARTITIONTIME":
				out.WriteString(quoteIdent(ingestionPartitionTimeColumn))
			case "_PARTITIONDATE":
				out.WriteString(quoteIdent(ingestionPartitionDateColumn))
			default:
				out.WriteString(queryText[i : end+1])
			}
			i = end + 1
			continue
		}
		if isSQLIdentifierByte(queryText[i]) {
			start := i
			for i < len(queryText) && isSQLIdentifierByte(queryText[i]) {
				i++
			}
			token := queryText[start:i]
			switch strings.ToUpper(token) {
			case "_PARTITIONTIME":
				out.WriteString(ingestionPartitionTimeColumn)
			case "_PARTITIONDATE":
				out.WriteString(ingestionPartitionDateColumn)
			default:
				out.WriteString(token)
			}
			continue
		}
		out.WriteByte(queryText[i])
		i++
	}
	return out.String()
}

func isSQLIdentifierByte(b byte) bool {
	return b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

// hideWildcardIngestionPseudocolumns enforces BigQuery's pseudocolumn
// wildcard rule. The embedded engine sees physical internal columns and would
// otherwise include them in SELECT *; direct SELECT _PARTITIONDATE/TIME
// projections are retained and renamed to their public schema names.
func hideWildcardIngestionPseudocolumns(queryText string, schema []tableField, rows [][]string) ([]tableField, [][]string, error) {
	keepTime, keepDate := directPseudocolumnProjectionCounts(queryText)
	keptIndexes := make([]int, 0, len(schema))
	keptSchema := make([]tableField, 0, len(schema))
	for i, field := range schema {
		name := strings.ToLower(field.Name)
		switch name {
		case ingestionPartitionTimeColumn:
			if keepTime <= 0 {
				continue
			}
			keepTime--
			field.Name = "_PARTITIONTIME"
		case ingestionPartitionDateColumn:
			if keepDate <= 0 {
				continue
			}
			keepDate--
			field.Name = "_PARTITIONDATE"
		}
		keptIndexes = append(keptIndexes, i)
		keptSchema = append(keptSchema, field)
	}
	if len(keptIndexes) == len(schema) {
		return keptSchema, rows, nil
	}
	keptRows := make([][]string, len(rows))
	for rowIndex, row := range rows {
		keptRows[rowIndex] = make([]string, 0, len(keptIndexes))
		for _, columnIndex := range keptIndexes {
			if columnIndex < len(row) {
				keptRows[rowIndex] = append(keptRows[rowIndex], row[columnIndex])
			} else {
				keptRows[rowIndex] = append(keptRows[rowIndex], storedNullCell)
			}
		}
	}
	return keptSchema, keptRows, nil
}

var directPartitionTimeProjection = regexp.MustCompile(`(?i)^(?:` + "`?[A-Za-z_][A-Za-z0-9_]*`?\\." + `)?` + "`?_PARTITIONTIME`?" + `$`)
var directPartitionDateProjection = regexp.MustCompile(`(?i)^(?:` + "`?[A-Za-z_][A-Za-z0-9_]*`?\\." + `)?` + "`?_PARTITIONDATE`?" + `$`)

func directPseudocolumnProjectionCounts(queryText string) (timeCount, dateCount int) {
	upper := strings.ToUpper(queryText)
	selectAt := strings.Index(upper, "SELECT")
	if selectAt < 0 {
		return 0, 0
	}
	projectionStart := selectAt + len("SELECT")
	fromAt := findTopLevelSQLKeyword(queryText, projectionStart, "FROM")
	if fromAt < 0 {
		fromAt = len(queryText)
	}
	projection := strings.TrimSpace(queryText[projectionStart:fromAt])
	projection = strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(projection), "DISTINCT"))
	for _, item := range splitTopLevelSQLList(projection) {
		item = strings.TrimSpace(item)
		switch {
		case directPartitionTimeProjection.MatchString(item):
			timeCount++
		case directPartitionDateProjection.MatchString(item):
			dateCount++
		}
	}
	return timeCount, dateCount
}

// findMatchingParen returns the index of the ')' matching the '(' at
// openIdx, skipping over quoted literals the same way findTopLevelSQLKeyword
// does. Returns -1 if the parens are unbalanced from openIdx onward.
func findMatchingParen(sqlText string, openIdx int) int {
	depth := 0
	for i := openIdx; i < len(sqlText); {
		switch sqlText[i] {
		case '\'', '"', '`':
			quote := sqlText[i]
			i++
			for i < len(sqlText) {
				if sqlText[i] == quote {
					i++
					if i < len(sqlText) && sqlText[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
			i++
		default:
			i++
		}
	}
	return -1
}

// rewriteMergeUsingSubquery works around a real limitation in the embedded
// GoogleSQL engine (goccy/googlesqlite): its MERGE implementation only
// accepts a bare table reference as the USING source, rejecting a derived
// table with "MERGE: source must be a single-table reference" (see that
// module's internal/analyzer.go, newMergeStmtAction — a deliberate, documented
// scope limit in the engine itself, not a parsing bug). Real BigQuery has no
// such restriction, and dbt-bigquery's default incremental "merge" strategy
// always emits `USING (<select>) AS DBT_INTERNAL_SOURCE`, so every dbt
// incremental model would fail here otherwise. The subquery is materialized
// into a real table in the same engine connection under a name the caller's
// statement cannot already reference, the USING clause is rewritten to name
// it directly, and the returned cleanup func drops it once the caller is done
// with the (already-executed) MERGE. A statement without a parenthesized
// USING source, or one whose parenthesized source is not itself a query
// (e.g. `USING other_table AS x`, already a bare table reference the engine
// accepts natively), is returned unchanged with a nil cleanup.
func (s *Server) rewriteMergeUsingSubquery(db *sql.DB, datasetID, engineStatement string) (string, func(), error) {
	usingAt := findTopLevelSQLKeyword(engineStatement, 0, "USING")
	if usingAt < 0 {
		return engineStatement, nil, nil
	}
	afterKeyword := usingAt + len("USING")
	trimmed := strings.TrimLeft(engineStatement[afterKeyword:], " \t\r\n")
	openAt := afterKeyword + (len(engineStatement[afterKeyword:]) - len(trimmed))
	if openAt >= len(engineStatement) || engineStatement[openAt] != '(' {
		return engineStatement, nil, nil
	}
	closeAt := findMatchingParen(engineStatement, openAt)
	if closeAt < 0 {
		return engineStatement, nil, nil
	}
	subquery := strings.TrimSpace(engineStatement[openAt+1 : closeAt])
	upperSubquery := strings.ToUpper(subquery)
	if !strings.HasPrefix(upperSubquery, "SELECT") && !strings.HasPrefix(upperSubquery, "WITH") {
		return engineStatement, nil, nil
	}

	ephemeralName := fmt.Sprintf("__locaql_merge_src_%d", s.mergeSourceCounter.Add(1))
	qualified := quoteIdent(datasetID) + "." + quoteIdent(ephemeralName)
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s AS (%s)", qualified, subquery)); err != nil {
		return "", nil, fmt.Errorf("MERGE: failed to materialize USING subquery: %w", err)
	}
	cleanup := func() {
		_, _ = db.Exec("DROP TABLE " + qualified)
	}
	rewritten := engineStatement[:openAt] + qualified + engineStatement[closeAt+1:]
	return rewritten, cleanup, nil
}

func findTopLevelSQLKeyword(sqlText string, start int, keyword string) int {
	depth := 0
	for i := start; i < len(sqlText); {
		switch sqlText[i] {
		case '\'', '"', '`':
			quote := sqlText[i]
			i++
			for i < len(sqlText) {
				if sqlText[i] == quote {
					i++
					if i < len(sqlText) && sqlText[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
		case '(':
			depth++
			i++
		case ')':
			if depth > 0 {
				depth--
			}
			i++
		default:
			if depth == 0 && isSQLIdentifierByte(sqlText[i]) {
				tokenStart := i
				for i < len(sqlText) && isSQLIdentifierByte(sqlText[i]) {
					i++
				}
				if strings.EqualFold(sqlText[tokenStart:i], keyword) {
					return tokenStart
				}
				continue
			}
			i++
		}
	}
	return -1
}

func splitTopLevelSQLList(value string) []string {
	var result []string
	start, depth := 0, 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\'', '"', '`':
			quote := value[i]
			for i++; i < len(value); i++ {
				if value[i] == quote {
					if i+1 < len(value) && value[i+1] == quote {
						i++
						continue
					}
					break
				}
			}
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				result = append(result, value[start:i])
				start = i + 1
			}
		}
	}
	return append(result, value[start:])
}

func materializeTable(db *sql.DB, datasetID, tableID string, fields []tableField, rows [][]string) error {
	qualified := quoteIdent(datasetID) + "." + quoteIdent(tableID)
	cols := make([]string, len(fields))
	hasNested := false
	for i, f := range fields {
		cols[i] = quoteIdent(f.Name) + " " + sqliteColumnType(f)
		if f.Mode == "REPEATED" || isRecordType(f.Type) {
			hasNested = true
		}
	}
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (%s)", qualified, strings.Join(cols, ", "))); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if hasNested {
		return materializeNestedRows(db, qualified, fields, rows)
	}
	return materializeScalarRows(db, qualified, fields, rows)
}

// materializeScalarRows bulk-inserts rows with no RECORD/REPEATED column
// inside one explicit transaction, one stmt.Exec per row against a single
// reused prepared statement. Measured directly (go test -bench
// BenchmarkSyncQueryLargeTable -cpuprofile=cpu.prof, then go tool pprof -top
// -cum): at 50k rows, a naive one-Exec-per-row loop with no explicit
// transaction spent 74% of total per-query time here — wrapping it in one
// transaction (avoiding a separate SQLite commit per row) measurably helps
// (~11% faster overall in the same benchmark). A multi-row batched INSERT
// (many rows' values folded into one larger VALUES list per statement) was
// also tried and measured, expecting a further win by amortizing the fixed
// per-stmt.Exec cost (value encoding/type resolution inside the
// WASM-transpiled engine) across more rows — it instead made this
// benchmark ~5.5x *slower*: the engine's own parse/analyze cost for one
// large, unprepared statement (hundreds of bound parameters) scales far
// worse than linearly with statement complexity, dwarfing the per-call
// overhead it was meant to amortize. Kept as one-row-at-a-time on purpose;
// do not reintroduce multi-row batching here without re-measuring.
func materializeScalarRows(db *sql.DB, qualified string, fields []tableField, rows [][]string) error {
	placeholders := make([]string, len(fields))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin materialize transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)", qualified, strings.Join(placeholders, ", ")))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range rows {
		values, err := scalarRowValues(fields, row)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(values...); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit materialize transaction: %w", err)
	}
	committed = true
	return nil
}

// scalarRowValues converts one stored string-cell row into the driver
// argument list for materializeScalarRows' prepared INSERT, in column order.
func scalarRowValues(fields []tableField, row []string) ([]any, error) {
	values := make([]any, len(fields))
	for i, f := range fields {
		cell := storedNullCell
		if i < len(row) {
			cell = row[i]
		}
		v, err := convertScalarForInsert(f.Type, cell)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", f.Name, err)
		}
		values[i] = v
	}
	return values, nil
}

// materializeNestedRows inserts rows for a schema that has at least one
// RECORD/REPEATED column: each row's INSERT statement is built dynamically
// with literal STRUCT(...)/[...] expressions per buildInsertValueExpr,
// rather than a single reusable prepared statement, since the CAST(? AS
// type) wrapping needed for reliability (see buildInsertValueExpr) means
// the parameter count and expression text are shaped by each row's actual
// data. This trades the prepared-statement fast path for correctness; rows
// in this emulator are local-dev-sized, not BigQuery-scale.
func materializeNestedRows(db *sql.DB, qualified string, fields []tableField, rows [][]string) error {
	for _, row := range rows {
		exprs := make([]string, len(fields))
		var params []any
		for i, f := range fields {
			cell := storedNullCell
			if i < len(row) {
				cell = row[i]
			}
			expr, p, err := buildInsertValueExpr(f, cell)
			if err != nil {
				return fmt.Errorf("column %s: %w", f.Name, err)
			}
			exprs[i] = expr
			params = append(params, p...)
		}
		stmt := fmt.Sprintf("INSERT INTO %s VALUES (%s)", qualified, strings.Join(exprs, ", "))
		if _, err := db.Exec(stmt, params...); err != nil {
			return err
		}
	}
	return nil
}

func scanRealSQLRows(rows *sql.Rows) ([]tableField, [][]string, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	schema := make([]tableField, len(colTypes))
	for i, ct := range colTypes {
		schema[i] = sqlColumnToTableField(ct.Name(), ct.DatabaseTypeName())
	}

	var result [][]string
	for rows.Next() {
		scanned := make([]any, len(colTypes))
		ptrs := make([]any, len(colTypes))
		for i := range scanned {
			ptrs[i] = &scanned[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make([]string, len(scanned))
		for i, v := range scanned {
			cell, err := cellFromScannedValue(schema[i], v)
			if err != nil {
				return nil, nil, err
			}
			row[i] = cell
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return schema, result, nil
}

// cellFromScannedValue converts one driver-scanned column value into this
// project's stored string-cell convention: a plain scalar string for scalar
// fields (unchanged, via scalarValueToString), or a canonical JSON encoding
// for RECORD/REPEATED fields. The driver scans a STRUCT as a positional
// []any (matching field.Fields order, not keyed by name — verified
// empirically), so RECORD cells are re-keyed by field name here to match
// this project's storage convention (used consistently for insert/extract).
func cellFromScannedValue(field tableField, v any) (string, error) {
	if field.Mode != "REPEATED" && !isRecordType(field.Type) {
		return scalarValueToString(v), nil
	}
	if v == nil {
		return storedNullCell, nil
	}
	normalized, err := normalizeScannedValue(field, v)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func normalizeScannedValue(field tableField, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if field.Mode == "REPEATED" {
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("column %s: expected a REPEATED array from the engine, got %T", field.Name, v)
		}
		base := field
		base.Mode = ""
		out := make([]any, len(arr))
		for i, e := range arr {
			n, err := normalizeScannedValue(base, e)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	}
	if isRecordType(field.Type) {
		positional, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("column %s: expected a positional STRUCT value from the engine, got %T", field.Name, v)
		}
		obj := make(map[string]any, len(field.Fields))
		for i, sub := range field.Fields {
			if i >= len(positional) {
				continue
			}
			n, err := normalizeScannedValue(sub, positional[i])
			if err != nil {
				return nil, err
			}
			obj[sub.Name] = jsonNormalizedScalar(sub, n)
		}
		return obj, nil
	}
	return jsonNormalizedScalar(field, v), nil
}

// jsonNormalizedScalar converts a scanned scalar Go value into a
// JSON-marshalable form using the same conventions as scalarValueToString,
// but keeping it typed (not stringified) so it round-trips as a real JSON
// number/boolean rather than a quoted string inside a nested cell.
func jsonNormalizedScalar(field tableField, v any) any {
	if isRecordType(field.Type) || field.Mode == "REPEATED" {
		return v
	}
	switch val := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(val)
	default:
		return val
	}
}
