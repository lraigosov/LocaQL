package server

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/linkedin/goavro/v2"
	"github.com/parquet-go/parquet-go"
)

// autodetectSampleLimit bounds how many rows schema autodetect reads from a
// NEWLINE_DELIMITED_JSON or CSV source before inferring a schema — a
// declared, bounded sample rather than an unbounded full-file scan, the same
// trade-off real BigQuery's own autodetect makes. AVRO and PARQUET don't need
// a sample at all: both formats embed their schema in the file itself, so
// autodetect for those two formats is exact, not inferred.
const autodetectSampleLimit = 500

// autodetectResult is what every format detector produces: the inferred
// schema, plus (CSV only) how many additional leading rows were consumed as
// a detected header. A caller that stores/reuses skipLeadingRows (external
// table config, load job config) must add HeaderRowsDetected to whatever
// skipLeadingRows it already applied before detection, or the real
// row-reading path will re-read that header as a bogus data row — detection
// and row-reading share the exact same skipLeadingRows-driven CSV reader
// (parseCSVRows), so this is the only way the two stay in sync.
type autodetectResult struct {
	Schema             []tableField
	HeaderRowsDetected int
}

// detectSchemaFromBytes is the single entry point load jobs and external
// tables both call when schema.fields is omitted and autodetect is
// requested. It mirrors loadRowsParser's format dispatch so the two stay in
// sync, and applies the same GZIP handling CSV/NDJSON already use for actual
// row parsing.
func detectSchemaFromBytes(sourceFormat string, data []byte, fieldDelimiter string, skipLeadingRows int, compression string) (autodetectResult, error) {
	sourceFormat = strings.ToUpper(strings.TrimSpace(sourceFormat))
	switch sourceFormat {
	case "", "CSV":
		data, err := maybeGunzip(data, normalizeCompressionForAutodetect(compression))
		if err != nil {
			return autodetectResult{}, err
		}
		schema, headerRows, err := detectSchemaFromCSV(data, fieldDelimiter, skipLeadingRows)
		return autodetectResult{Schema: schema, HeaderRowsDetected: headerRows}, err
	case "NEWLINE_DELIMITED_JSON":
		data, err := maybeGunzip(data, normalizeCompressionForAutodetect(compression))
		if err != nil {
			return autodetectResult{}, err
		}
		schema, err := detectSchemaFromNDJSON(data)
		return autodetectResult{Schema: schema}, err
	case "AVRO":
		schema, err := detectSchemaFromAvro(data)
		return autodetectResult{Schema: schema}, err
	case "PARQUET":
		schema, err := detectSchemaFromParquet(data)
		return autodetectResult{Schema: schema}, err
	default:
		return autodetectResult{}, fmt.Errorf("sourceFormat %q is not supported; schema autodetect currently supports NEWLINE_DELIMITED_JSON, CSV, AVRO and PARQUET", sourceFormat)
	}
}

// normalizeCompressionForAutodetect tolerates an empty/unset compression the
// same way loadRowsParser's normalizeLoadCompression does, without importing
// that function's stricter validation (autodetect runs before the rest of
// load/external-table validation, so a bad compression value should surface
// through the normal path's own error, not autodetect's).
func normalizeCompressionForAutodetect(compression string) string {
	return strings.ToUpper(strings.TrimSpace(compression))
}

// widenScalarType combines two observed scalar type classifications for the
// same column into the narrowest type both are safely representable as.
// "" means "no observation yet" and is absorbed by whatever the other side
// is. INT64 widens to FLOAT64 on a mixed int/float column, matching
// BigQuery's own numeric promotion; any other mismatch (bool vs. number,
// anything vs. an actual string) falls back to STRING rather than guessing —
// consistent with this project's "fail toward the safe, explicit choice"
// convention applied here to type inference instead of an error.
func widenScalarType(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case a == b:
		return a
	case (a == "INT64" && b == "FLOAT64") || (a == "FLOAT64" && b == "INT64"):
		return "FLOAT64"
	default:
		return "STRING"
	}
}

// classifyJSONScalar returns this project's scalar type name for one decoded
// NDJSON value ("" for a JSON null, which carries no type information). A
// nested object or array is rejected explicitly: schema autodetect covers
// top-level scalar columns only, matching the bounded scope already declared
// for RECORD/REPEATED elsewhere in load/external tables (rejectNestedFields).
func classifyJSONScalar(v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "", nil
	case bool:
		return "BOOL", nil
	case string:
		return "STRING", nil
	case float64: // encoding/json decodes every JSON number as float64
		if !math.IsInf(val, 0) && val == math.Trunc(val) {
			return "INT64", nil
		}
		return "FLOAT64", nil
	case map[string]any, []any:
		return "", fmt.Errorf("nested/repeated values are not supported by schema autodetect yet; provide schema.fields explicitly")
	default:
		return "STRING", nil
	}
}

// detectSchemaFromNDJSON infers a flat schema from up to autodetectSampleLimit
// sample rows: field order follows first appearance across the sample, and
// each field's type is the widened union of every value observed for it.
// Every detected field is NULLABLE — this project never infers REQUIRED,
// since a sample proving a field was always present is weaker evidence than
// an explicit schema declaration, and a false REQUIRED would reject a
// legitimate later null.
func detectSchemaFromNDJSON(data []byte) ([]tableField, error) {
	var order []string
	types := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	sampled := 0
	for sampled < autodetectSampleLimit && scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("invalid NDJSON row while detecting schema: %w", err)
		}
		sampled++
		for key, value := range record {
			if _, known := types[key]; !known {
				order = append(order, key)
			}
			observed, err := classifyJSONScalar(value)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
			types[key] = widenScalarType(types[key], observed)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan NDJSON while detecting schema: %w", err)
	}
	if sampled == 0 {
		return nil, fmt.Errorf("no rows found to detect a schema from")
	}

	schema := make([]tableField, 0, len(order))
	for _, key := range order {
		t := types[key]
		if t == "" { // every sampled value for this key was JSON null
			t = "STRING"
		}
		schema = append(schema, tableField{Name: key, Type: t, Mode: "NULLABLE"})
	}
	return schema, nil
}

// classifyCSVScalar returns this project's scalar type name for one raw CSV
// cell, or "" for an empty cell (no signal either way — an all-empty column
// still resolves to STRING as the final fallback in the caller).
func classifyCSVScalar(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return "INT64"
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return "FLOAT64"
	}
	if _, err := strconv.ParseBool(strings.ToLower(v)); err == nil {
		return "BOOL"
	}
	return "STRING"
}

// detectSchemaFromCSV infers column names and types from up to
// autodetectSampleLimit sample rows. Header detection follows the same
// signal real BigQuery's own documented autodetect uses: compare row 1
// against the rest of the sample per column, and treat row 1 as a header
// only where at least one column's row-1 value is a non-numeric/non-bool
// string while the rest of that column widens to a real (non-STRING) type —
// i.e. row 1 looks like a label, not data. With fewer than two sample rows
// there isn't enough signal to compare, so row 1 is treated as data (the
// safer default: a false header guess silently drops a real data row, a
// false "no header" guess only produces a generic string_field_N column
// name, which is visible and correctable). Columns without a detected
// header are named string_field_0, string_field_1, ... matching real
// BigQuery's own naming convention for the same case.
func detectSchemaFromCSV(data []byte, fieldDelimiter string, skipLeadingRows int) ([]tableField, int, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1
	if delim := []rune(fieldDelimiter); len(delim) > 0 {
		reader.Comma = delim[0]
	}
	records, err := reader.ReadAll()
	if err != nil {
		return nil, 0, fmt.Errorf("invalid CSV while detecting schema: %w", err)
	}
	if skipLeadingRows > len(records) {
		skipLeadingRows = len(records)
	}
	records = records[skipLeadingRows:]
	if len(records) == 0 {
		return nil, 0, fmt.Errorf("no rows found to detect a schema from")
	}

	sample := records
	if len(sample) > autodetectSampleLimit {
		sample = sample[:autodetectSampleLimit]
	}
	width := len(sample[0])

	names, startRow := detectCSVHeader(sample, width)
	dataTypes := inferCSVColumnTypes(sample[startRow:], width)

	schema := make([]tableField, width)
	for i := 0; i < width; i++ {
		t := dataTypes[i]
		if t == "" {
			t = "STRING"
		}
		schema[i] = tableField{Name: names[i], Type: t, Mode: "NULLABLE"}
	}
	return schema, startRow, nil
}

// inferCSVColumnTypes widens the observed scalar type across every row for
// each of the first width columns; a column with fewer values than width in
// a given (jagged) row is simply skipped for that row.
func inferCSVColumnTypes(rows [][]string, width int) []string {
	types := make([]string, width)
	for _, row := range rows {
		for col := 0; col < width && col < len(row); col++ {
			types[col] = widenScalarType(types[col], classifyCSVScalar(row[col]))
		}
	}
	return types
}

// detectCSVHeader decides whether sample[0] is a header row by comparing its
// per-column type against the type inferred from the rest of the sample (see
// detectSchemaFromCSV's doc comment for the exact signal and why). It
// returns the resolved column names and the index of the first real data
// row (0 if no header was detected, 1 if it was).
func detectCSVHeader(sample [][]string, width int) (names []string, startRow int) {
	if len(sample) < 2 || !looksLikeCSVHeader(sample[0], inferCSVColumnTypes(sample[1:], width), width) {
		return genericCSVColumnNames(width), 0
	}
	names = make([]string, width)
	copy(names, sample[0])
	return names, 1
}

// looksLikeCSVHeader reports whether firstRow is a label row rather than
// data: true when at least one column's first-row value is a plain string
// while the rest of the sample widens that same column to a real
// (non-STRING) scalar type — i.e. the label doesn't fit the data's own type.
func looksLikeCSVHeader(firstRow, dataTypes []string, width int) bool {
	for col := 0; col < width && col < len(firstRow); col++ {
		if dataTypes[col] != "" && dataTypes[col] != "STRING" && classifyCSVScalar(firstRow[col]) == "STRING" {
			return true
		}
	}
	return false
}

func genericCSVColumnNames(width int) []string {
	names := make([]string, width)
	for i := range names {
		names[i] = fmt.Sprintf("string_field_%d", i)
	}
	return names
}

// avroSchemaField/avroSchemaRecord decode just enough of an Avro schema's own
// JSON representation (Codec.Schema()) to walk its top-level fields; this is
// deliberately narrower than a full Avro schema parser, matching the bounded
// scope below.
type avroSchemaField struct {
	Name string `json:"name"`
	Type any    `json:"type"`
}

type avroSchemaRecord struct {
	Type   string            `json:"type"`
	Fields []avroSchemaField `json:"fields"`
}

// detectSchemaFromAvro reads the schema embedded in the Avro Object Container
// File itself (goavro's Codec.Schema()) rather than inferring anything from
// sample data — Avro is self-describing, so this is exact, not a heuristic.
func detectSchemaFromAvro(data []byte) ([]tableField, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("invalid Avro OCF while detecting schema: %w", err)
	}
	codec := reader.Codec()
	if codec == nil {
		return nil, fmt.Errorf("Avro file has no embedded schema")
	}

	var rec avroSchemaRecord
	if err := json.Unmarshal([]byte(codec.Schema()), &rec); err != nil {
		return nil, fmt.Errorf("failed to parse embedded Avro schema: %w", err)
	}
	if rec.Type != "record" {
		return nil, fmt.Errorf("embedded Avro schema is not a top-level record; schema autodetect requires a record schema")
	}
	if len(rec.Fields) == 0 {
		return nil, fmt.Errorf("embedded Avro schema has no fields")
	}

	fields := make([]tableField, 0, len(rec.Fields))
	for _, f := range rec.Fields {
		bqType, nullable, err := bigQueryTypeForAvroFieldType(f.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		mode := "REQUIRED"
		if nullable {
			mode = "NULLABLE"
		}
		fields = append(fields, tableField{Name: f.Name, Type: bqType, Mode: mode})
	}
	return fields, nil
}

// bigQueryTypeForAvroFieldType maps one Avro field type to a BigQuery scalar.
// Only two shapes are accepted: a bare primitive name, or a two-branch
// ["null", primitive] union — the only two shapes this project's own
// encodeAvro ever produces (see buildAvroSchemaJSON) and the common shape
// most real-world Avro producers use for a nullable column. Records, arrays,
// maps, enums, fixed types, and unions with more than one non-null branch
// are rejected explicitly: nested/complex Avro schema autodetect is out of
// scope, matching rejectNestedFields elsewhere in this project.
func bigQueryTypeForAvroFieldType(t any) (bqType string, nullable bool, err error) {
	switch v := t.(type) {
	case string:
		bqType, err := bigQueryScalarForAvroPrimitive(v)
		return bqType, false, err
	case []any:
		return bigQueryTypeForAvroUnion(v)
	default:
		return "", false, fmt.Errorf("Avro field type is not a primitive or nullable-union type; nested/complex Avro types are not supported by schema autodetect")
	}
}

// bigQueryTypeForAvroUnion resolves the ["null", primitive] union shape out
// of bigQueryTypeForAvroFieldType, kept separate to keep each function's own
// branching simple.
func bigQueryTypeForAvroUnion(branches []any) (bqType string, nullable bool, err error) {
	primitive, err := soleNonNullAvroBranch(branches)
	if err != nil {
		return "", false, err
	}
	bqType, err = bigQueryScalarForAvroPrimitive(primitive)
	return bqType, true, err
}

// soleNonNullAvroBranch extracts the single non-null primitive name out of
// an Avro union, requiring exactly one "null" branch and exactly one
// primitive branch — any other shape (no null, two+ non-null branches, a
// non-string/complex branch) is rejected explicitly.
func soleNonNullAvroBranch(branches []any) (string, error) {
	var primitive string
	sawNull := false
	for _, branch := range branches {
		name, ok := branch.(string)
		if !ok {
			return "", fmt.Errorf("Avro union contains a non-primitive branch; nested/complex Avro types are not supported by schema autodetect")
		}
		if name == "null" {
			sawNull = true
			continue
		}
		if primitive != "" {
			return "", fmt.Errorf("Avro union has more than one non-null branch, which is not supported by schema autodetect")
		}
		primitive = name
	}
	if !sawNull || primitive == "" {
		return "", fmt.Errorf(`Avro union must be exactly ["null", <primitive>] to be autodetected`)
	}
	return primitive, nil
}

// bigQueryScalarForAvroPrimitive covers exactly the Avro primitives this
// project's own Avro encode path (avroTypeFor) round-trips today
// (INT64/FLOAT64/BOOL/STRING) plus "int"/"float"/"bytes", which are valid
// Avro primitives this project doesn't emit itself but a real external
// producer might; "bytes" maps to STRING to match scalarValueToString's
// existing []byte-to-string handling elsewhere in this codebase, not a new
// BYTES type this project's Avro path doesn't otherwise support.
func bigQueryScalarForAvroPrimitive(t string) (string, error) {
	switch t {
	case "string", "bytes":
		return "STRING", nil
	case "long", "int":
		return "INT64", nil
	case "double", "float":
		return "FLOAT64", nil
	case "boolean":
		return "BOOL", nil
	default:
		return "", fmt.Errorf("Avro primitive type %q is not supported by schema autodetect", t)
	}
}

// detectSchemaFromParquet reads the schema embedded in the Parquet file's own
// footer (parquet.NewReader with no explicit schema auto-discovers it) —
// exact, not inferred, the same reasoning as detectSchemaFromAvro.
func detectSchemaFromParquet(data []byte) ([]tableField, error) {
	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	schema := reader.Schema()
	if schema == nil {
		return nil, fmt.Errorf("failed to read embedded Parquet schema")
	}
	fields := schema.Fields()
	if len(fields) == 0 {
		return nil, fmt.Errorf("embedded Parquet schema has no columns")
	}

	out := make([]tableField, 0, len(fields))
	for _, f := range fields {
		if !f.Leaf() {
			return nil, fmt.Errorf("field %q: nested Parquet group types are not supported by schema autodetect", f.Name())
		}
		if f.Repeated() {
			return nil, fmt.Errorf("field %q: repeated Parquet fields are not supported by schema autodetect", f.Name())
		}
		bqType, err := bigQueryTypeForParquetKind(f.Type().Kind())
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name(), err)
		}
		mode := "NULLABLE"
		if f.Required() {
			mode = "REQUIRED"
		}
		out = append(out, tableField{Name: f.Name(), Type: bqType, Mode: mode})
	}
	return out, nil
}

// bigQueryTypeForParquetKind covers exactly the physical types this
// project's own Parquet encode path (parquetNodeFor) round-trips today
// (INT64/FLOAT64/BOOL/STRING via ByteArray); Int96 (a legacy, deprecated
// timestamp encoding) and FixedLenByteArray beyond plain string use are
// rejected explicitly rather than guessed at.
func bigQueryTypeForParquetKind(k parquet.Kind) (string, error) {
	switch k {
	case parquet.Boolean:
		return "BOOL", nil
	case parquet.Int32, parquet.Int64:
		return "INT64", nil
	case parquet.Float, parquet.Double:
		return "FLOAT64", nil
	case parquet.ByteArray:
		return "STRING", nil
	default:
		return "", fmt.Errorf("Parquet physical type %v is not supported by schema autodetect", k)
	}
}
