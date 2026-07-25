package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

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

// convertScalarForInsert parses a BigQuery REST-convention scalar string
// cell into a typed value for a parameterized INSERT. An empty string is
// treated as NULL for INT64/FLOAT64/BOOL, since none of those types can
// otherwise hold "" — this does not change STRING behavior, which keeps
// empty strings as-is.
func convertScalarForInsert(fieldType, cell string) (any, error) {
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

// tableRefPattern finds dotted table references following FROM/JOIN, with
// 2 parts (dataset.table) or 3 (project.dataset.table), optionally wrapped
// in a single pair of backticks around the whole reference.
var tableRefPattern = regexp.MustCompile("(?i)\\b(?:FROM|JOIN)\\s+`?([A-Za-z0-9_]+(?:\\.[A-Za-z0-9_]+){1,2})`?")

type datasetTableRef struct {
	datasetID string
	tableID   string
}

// referencedTables extracts every dataset.table pair a query text mentions
// after FROM/JOIN. A 3-part reference is only kept when its leading
// component matches projectID (case-insensitive); other projects are not
// materialized, so a genuine cross-project reference fails naturally with
// "table not found" from the real engine rather than silently resolving
// against the wrong project.
func referencedTables(queryText, projectID string) []datasetTableRef {
	seen := map[datasetTableRef]bool{}
	var refs []datasetTableRef
	for _, m := range tableRefPattern.FindAllStringSubmatch(queryText, -1) {
		parts := strings.Split(m[1], ".")
		var ref datasetTableRef
		switch len(parts) {
		case 2:
			ref = datasetTableRef{datasetID: parts[0], tableID: parts[1]}
		case 3:
			if !strings.EqualFold(parts[0], projectID) {
				continue
			}
			ref = datasetTableRef{datasetID: parts[1], tableID: parts[2]}
		default:
			continue
		}
		if !seen[ref] {
			seen[ref] = true
			refs = append(refs, ref)
		}
	}
	return refs
}

// stripProjectPrefix rewrites project.dataset.table references (bare or
// backtick-wrapped) down to dataset.table, since the real engine models one
// schema per dataset and has no concept of a project level.
func stripProjectPrefix(queryText, projectID string) string {
	if projectID == "" {
		return queryText
	}
	pattern := regexp.MustCompile("(?i)`?" + regexp.QuoteMeta(projectID) + "`?\\.([A-Za-z0-9_]+\\.[A-Za-z0-9_]+)")
	return pattern.ReplaceAllString(queryText, "$1")
}

// sqlTypeDescriptor mirrors the JSON shape of googlesqlite's
// ColumnType.DatabaseTypeName(): a scalar has just Name; ARRAY<T> has
// ElementType set to T's descriptor; STRUCT<...> has FieldTypes set to its
// nested named fields (each recursively the same shape).
type sqlTypeDescriptor struct {
	Name        string         `json:"name"`
	ElementType *sqlTypeDescriptor `json:"elementType"`
	FieldTypes  []sqlNamedType `json:"fieldTypes"`
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
// than the previous regex-matched full-table-dump/fabricated fallback.
func (s *Server) executeRealSQLQuery(projectID, queryText string) ([]tableField, [][]string, error) {
	return s.executeRealSQLQueryVisiting(projectID, queryText, map[string]bool{})
}

// executeRealSQLQueryVisiting is executeRealSQLQuery plus the view-cycle
// guard threaded through resolveTableRowsVisiting: a referenced table may
// itself be a view, which is resolved by recursively executing its own
// query here.
func (s *Server) executeRealSQLQueryVisiting(projectID, queryText string, visiting map[string]bool) ([]tableField, [][]string, error) {
	db, err := sql.Open("googlesqlite", ":memory:")
	if err != nil {
		return nil, nil, fmt.Errorf("open real SQL engine: %w", err)
	}
	defer db.Close()

	createdSchemas := map[string]bool{}
	for _, ref := range referencedTables(queryText, projectID) {
		fields, rows, ok, err := s.resolveTableRowsVisiting(projectID, ref.datasetID, ref.tableID, visiting)
		if !ok {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if !createdSchemas[ref.datasetID] {
			if _, err := db.Exec("CREATE SCHEMA " + quoteIdent(ref.datasetID)); err != nil {
				return nil, nil, fmt.Errorf("materialize dataset %s: %w", ref.datasetID, err)
			}
			createdSchemas[ref.datasetID] = true
		}
		if err := materializeTable(db, ref.datasetID, ref.tableID, fields, rows); err != nil {
			return nil, nil, fmt.Errorf("materialize table %s.%s: %w", ref.datasetID, ref.tableID, err)
		}
	}

	rows, err := db.Query(stripProjectPrefix(queryText, projectID))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	return scanRealSQLRows(rows)
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

	placeholders := make([]string, len(fields))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)", qualified, strings.Join(placeholders, ", ")))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range rows {
		values := make([]any, len(fields))
		for i, f := range fields {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			v, err := convertScalarForInsert(f.Type, cell)
			if err != nil {
				return fmt.Errorf("column %s: %w", f.Name, err)
			}
			values[i] = v
		}
		if _, err := stmt.Exec(values...); err != nil {
			return err
		}
	}
	return nil
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
			cell := ""
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
		return "", nil
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
