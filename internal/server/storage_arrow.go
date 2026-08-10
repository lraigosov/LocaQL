package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// arrowFieldType maps a BigQuery column type to its real BigQuery Storage
// Read API Arrow equivalent, per Google's own documented mapping (INT64 ->
// Int64, FLOAT64 -> Float64/Double, BOOL -> Boolean, BYTES -> Binary, STRING
// -> Utf8, DATE -> 32-bit days-since-epoch, DATETIME -> microsecond
// timestamp with no timezone, TIMESTAMP -> microsecond timestamp in UTC,
// TIME -> microsecond time-of-day). NUMERIC/BIGNUMERIC (real BigQuery maps
// these to Decimal128/Decimal256, which this emulator does not build yet),
// RECORD/REPEATED, GEOGRAPHY, JSON and RANGE are explicitly unsupported for
// this format rather than silently downgraded to a string column — a client
// that needs those can still use DATA_FORMAT_AVRO. See
// capabilities/registry.yaml (grpc.storage.read.arrow) and
// KNOWN-DIVERGENCES.md for the exact scope.
func arrowFieldType(bqType string) (arrow.DataType, error) {
	switch strings.ToUpper(bqType) {
	case "BOOL", "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, nil
	case "INT64", "INTEGER":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT64", "FLOAT":
		return arrow.PrimitiveTypes.Float64, nil
	case "STRING":
		return arrow.BinaryTypes.String, nil
	case "BYTES":
		return arrow.BinaryTypes.Binary, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "DATETIME":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "TIME":
		return &arrow.Time64Type{Unit: arrow.Microsecond}, nil
	default:
		return nil, fmt.Errorf("column type %q has no Arrow mapping in this emulator yet (NUMERIC, BIGNUMERIC, RECORD/REPEATED, GEOGRAPHY, JSON and RANGE are explicitly out of scope for DATA_FORMAT_ARROW); request DATA_FORMAT_AVRO instead", bqType)
	}
}

// buildArrowSchema renders schema fields into a real Arrow schema, failing
// explicitly (not silently truncating columns) the moment any field has no
// Arrow mapping yet.
func buildArrowSchema(fields []tableField) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, len(fields))
	for i, f := range fields {
		if f.Mode == "REPEATED" || isRecordType(f.Type) {
			return nil, fmt.Errorf("column %q: RECORD/REPEATED fields are not supported for DATA_FORMAT_ARROW yet; request DATA_FORMAT_AVRO instead", f.Name)
		}
		dt, err := arrowFieldType(f.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", f.Name, err)
		}
		arrowFields[i] = arrow.Field{Name: f.Name, Type: dt, Nullable: normalizeMode(f.Mode) != "REQUIRED"}
	}
	return arrow.NewSchema(arrowFields, nil), nil
}

// buildArrowRecord converts stored string-per-cell rows into a real Arrow
// record batch, reusing the same loadStoredCell NULL convention every other
// output format (REST, Avro, CSV, NDJSON) already uses.
func buildArrowRecord(mem memory.Allocator, schema *arrow.Schema, fields []tableField, rows [][]string) (arrow.Record, error) {
	builders := make([]array.Builder, len(fields))
	for i, f := range schema.Fields() {
		builders[i] = array.NewBuilder(mem, f.Type)
		defer builders[i].Release()
	}

	for _, row := range rows {
		for i, field := range fields {
			var raw string
			if i < len(row) {
				raw = row[i]
			}
			value, isNull := loadStoredCell(raw)
			if isNull {
				builders[i].AppendNull()
				continue
			}
			if err := appendArrowValue(builders[i], field.Type, value); err != nil {
				return nil, fmt.Errorf("column %q: %w", field.Name, err)
			}
		}
	}

	columns := make([]arrow.Array, len(builders))
	for i, b := range builders {
		columns[i] = b.NewArray()
		defer columns[i].Release()
	}
	return array.NewRecord(schema, columns, int64(len(rows))), nil
}

// appendArrowValue parses one stored cell string into the builder's native
// Arrow representation. A value that fails to parse as its declared type
// fails the whole request explicitly instead of silently substituting a
// zero value, unlike this emulator's more lenient Avro/CSV encoders — Arrow
// consumers (pandas/polars DataFrames) expect a column's dtype to be
// trustworthy, not a best-effort guess.
func appendArrowValue(b array.Builder, bqType, value string) error {
	switch strings.ToUpper(bqType) {
	case "BOOL", "BOOLEAN":
		return appendArrowBool(b.(*array.BooleanBuilder), value)
	case "INT64", "INTEGER":
		return appendArrowInt64(b.(*array.Int64Builder), value)
	case "FLOAT64", "FLOAT":
		return appendArrowFloat64(b.(*array.Float64Builder), value)
	case "STRING":
		b.(*array.StringBuilder).Append(value)
		return nil
	case "BYTES":
		// BYTES cells are stored as the raw byte content itself (see
		// scalarValueToPlainString's `case []byte: return string(val)`), not
		// base64 text, so no decoding step is needed here.
		b.(*array.BinaryBuilder).Append([]byte(value))
		return nil
	case "DATE":
		return appendArrowDate(b.(*array.Date32Builder), value)
	case "DATETIME":
		return appendArrowTimestamp(b.(*array.TimestampBuilder), value, false)
	case "TIMESTAMP":
		return appendArrowTimestamp(b.(*array.TimestampBuilder), value, true)
	case "TIME":
		return appendArrowTimeOfDay(b.(*array.Time64Builder), value)
	default:
		return fmt.Errorf("column type %q has no Arrow mapping in this emulator yet", bqType)
	}
}

func appendArrowBool(b *array.BooleanBuilder, value string) error {
	v, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("invalid BOOL value %q: %w", value, err)
	}
	b.Append(v)
	return nil
}

func appendArrowInt64(b *array.Int64Builder, value string) error {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid INT64 value %q: %w", value, err)
	}
	b.Append(v)
	return nil
}

func appendArrowFloat64(b *array.Float64Builder, value string) error {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("invalid FLOAT64 value %q: %w", value, err)
	}
	b.Append(v)
	return nil
}

func appendArrowDate(b *array.Date32Builder, value string) error {
	t, err := parseStoredDate(value)
	if err != nil {
		return err
	}
	b.Append(arrow.Date32FromTime(t))
	return nil
}

// appendArrowTimestamp handles both DATETIME (utc=false, no timezone) and
// TIMESTAMP (utc=true) — the two BigQuery types real BigQuery itself maps to
// the same Arrow TimestampType shape, differing only in the schema's
// TimeZone field (see arrowFieldType) and, here, whether the parsed instant
// is normalized to UTC before conversion.
func appendArrowTimestamp(b *array.TimestampBuilder, value string, utc bool) error {
	t, err := parsePartitionTimestamp(value)
	if err != nil {
		kind := "DATETIME"
		if utc {
			kind = "TIMESTAMP"
		}
		return fmt.Errorf("invalid %s value %q: %w", kind, value, err)
	}
	if utc {
		t = t.UTC()
	}
	ts, err := arrow.TimestampFromTime(t, arrow.Microsecond)
	if err != nil {
		return err
	}
	b.Append(ts)
	return nil
}

func appendArrowTimeOfDay(b *array.Time64Builder, value string) error {
	t, err := parseStoredTimeOfDay(value)
	if err != nil {
		return err
	}
	micros := int64(t.Hour())*3600e6 + int64(t.Minute())*60e6 + int64(t.Second())*1e6 + int64(t.Nanosecond())/1000
	b.Append(arrow.Time64(micros))
	return nil
}

// parseStoredDate parses a stored DATE cell (BigQuery's canonical
// "YYYY-MM-DD" form).
func parseStoredDate(value string) (time.Time, error) {
	t, err := time.Parse("2006-01-02", strings.TrimSpace(strings.Trim(value, `"`)))
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid DATE value %q: %w", value, err)
	}
	return t, nil
}

// parseStoredTimeOfDay parses a stored TIME cell (BigQuery's canonical
// "HH:MM:SS[.ffffff]" form, with or without fractional seconds).
func parseStoredTimeOfDay(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(strings.Trim(value, `"`))
	for _, layout := range []string{"15:04:05.999999999", "15:04:05"} {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid TIME value %q", value)
}

// The Arrow IPC "encapsulated message" wire format (continuation marker,
// padded length-prefixed metadata flatbuffer, then body) is a stable, public
// part of the Arrow columnar format spec
// (arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format).
// arrow-go's own writer only exposes it through an unexported function
// (ipc.streamWriter's writeIPCPayload), so serializeArrowIPCMessage
// reimplements just that framing directly against arrow-go's exported
// ipc.Payload accessors (Meta/SerializeBody) rather than reaching into
// unexported internals. This is exactly the shape real BigQuery's own
// ArrowSchema.serialized_schema/ArrowRecordBatch.serialized_record_batch
// use: each is independently decodable (confirmed against Google's own Java
// sample, which calls MessageSerializer.deserializeSchema/
// deserializeRecordBatch on each separately, never concatenated) — verified
// here by round-tripping through arrow-go's own stream reader, see
// storage_arrow_test.go.
const (
	arrowIPCContinuationToken uint32 = 0xFFFFFFFF
	arrowIPCAlignment                = 8
)

func serializeArrowIPCMessage(p *ipc.Payload) ([]byte, error) {
	meta := p.Meta()
	defer meta.Release()

	var buf bytes.Buffer
	metaLen := int32(meta.Len())
	paddedLen := metaLen + 8
	if rem := paddedLen % arrowIPCAlignment; rem != 0 {
		paddedLen += arrowIPCAlignment - rem
	}

	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], arrowIPCContinuationToken)
	if _, err := buf.Write(tmp[:]); err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(tmp[:], uint32(paddedLen-8))
	if _, err := buf.Write(tmp[:]); err != nil {
		return nil, err
	}
	if _, err := buf.Write(meta.Bytes()); err != nil {
		return nil, err
	}
	if padding := paddedLen - metaLen - 8; padding > 0 {
		if _, err := buf.Write(make([]byte, padding)); err != nil {
			return nil, err
		}
	}
	if err := p.SerializeBody(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// serializeArrowSchemaMessage renders a standalone Arrow IPC schema message
// for ArrowSchema.serialized_schema.
func serializeArrowSchemaMessage(schema *arrow.Schema, mem memory.Allocator) ([]byte, error) {
	payload := ipc.GetSchemaPayload(schema, mem)
	defer payload.Release()
	return serializeArrowIPCMessage(&payload)
}

// serializeArrowRecordBatchMessage renders a standalone Arrow IPC record
// batch message for ArrowRecordBatch.serialized_record_batch.
func serializeArrowRecordBatchMessage(rec arrow.Record, mem memory.Allocator) ([]byte, error) {
	payload, err := ipc.GetRecordBatchPayload(rec, ipc.WithAllocator(mem))
	if err != nil {
		return nil, err
	}
	defer payload.Release()
	return serializeArrowIPCMessage(&payload)
}

// arrowIPCEOSMarker is the end-of-stream marker ipc.Writer's Close() appends:
// a bare continuation token with a zero-length body.
var arrowIPCEOSMarker = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00}

// decodeArrowSchemaMessage decodes a standalone Arrow IPC schema message
// (AppendRowsRequest.ArrowData.WriterSchema.SerializedSchema, exactly the
// same self-contained shape this file's serializeArrowSchemaMessage
// produces) back into a real *arrow.Schema.
func decodeArrowSchemaMessage(schemaBytes []byte, mem memory.Allocator) (*arrow.Schema, error) {
	return flight.DeserializeSchema(schemaBytes, mem)
}

// decodeArrowRecordBatch decodes a standalone Arrow IPC record-batch message
// (AppendRowsRequest.ArrowData.Rows.SerializedRecordBatch) against its
// already-known schema bytes. There is no exported single-message decode
// entry point in arrow-go for "one record batch against a schema I already
// have" (the mirror of what GetRecordBatchPayload/serializeArrowIPCMessage
// build), so this reconstructs an ordinary Arrow IPC *stream* — schema
// message, then this one record-batch message, then an end-of-stream marker
// — and decodes it with arrow-go's own, real ipc.NewReader. The returned
// record is Retain()-ed past the reader's own Release() and must be
// Release()-d by the caller.
func decodeArrowRecordBatch(schemaBytes, batchBytes []byte, mem memory.Allocator) (arrow.Record, error) {
	var stream bytes.Buffer
	stream.Write(schemaBytes)
	stream.Write(batchBytes)
	stream.Write(arrowIPCEOSMarker)

	reader, err := ipc.NewReader(bytes.NewReader(stream.Bytes()), ipc.WithAllocator(mem))
	if err != nil {
		return nil, fmt.Errorf("decode arrow record batch: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("decode arrow record batch: %w", err)
		}
		return nil, fmt.Errorf("expected exactly one arrow record batch, got none")
	}
	rec := reader.Record()
	rec.Retain()
	return rec, nil
}

// arrowRecordToRows converts a decoded Arrow record batch back into this
// project's stored string-per-cell rows, matched against the destination
// table's columns by name (like protoMessageToRow's proto_rows path) rather
// than position — a client's Arrow schema field order need not match this
// project's column order. A column present in fields but absent from the
// incoming Arrow schema fails if REQUIRED, otherwise becomes a stored NULL.
func arrowRecordToRows(rec arrow.Record, fields []tableField) ([][]string, error) {
	schema := rec.Schema()
	columns := make([]arrow.Array, len(fields))
	for i, f := range fields {
		idx := schema.FieldIndices(f.Name)
		if len(idx) == 0 {
			if normalizeMode(f.Mode) == "REQUIRED" {
				return nil, fmt.Errorf("column %s is REQUIRED but absent from the Arrow record batch", f.Name)
			}
			continue
		}
		columns[i] = rec.Column(idx[0])
	}

	rows := make([][]string, rec.NumRows())
	for r := 0; r < int(rec.NumRows()); r++ {
		row := make([]string, len(fields))
		for i, f := range fields {
			if columns[i] == nil {
				row[i] = storedNullCell
				continue
			}
			value, err := arrowColumnValueToString(columns[i], r, f.Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", f.Name, err)
			}
			row[i] = value
		}
		rows[r] = row
	}
	return rows, nil
}

// arrowColumnValueToString reads one cell out of an Arrow column at rowIdx
// and renders it into this project's stored string-cell form, the exact
// inverse of appendArrowValue.
func arrowColumnValueToString(col arrow.Array, rowIdx int, bqType string) (string, error) {
	if col.IsNull(rowIdx) {
		return storedNullCell, nil
	}
	switch strings.ToUpper(bqType) {
	case "BOOL", "BOOLEAN":
		return strconv.FormatBool(col.(*array.Boolean).Value(rowIdx)), nil
	case "INT64", "INTEGER":
		return strconv.FormatInt(col.(*array.Int64).Value(rowIdx), 10), nil
	case "FLOAT64", "FLOAT":
		return strconv.FormatFloat(col.(*array.Float64).Value(rowIdx), 'g', -1, 64), nil
	case "STRING":
		return storeStringCell(col.(*array.String).Value(rowIdx)), nil
	case "BYTES":
		return storeStringCell(string(col.(*array.Binary).Value(rowIdx))), nil
	case "DATE":
		return col.(*array.Date32).Value(rowIdx).ToTime().Format("2006-01-02"), nil
	case "DATETIME":
		return col.(*array.Timestamp).Value(rowIdx).ToTime(arrow.Microsecond).Format("2006-01-02 15:04:05.999999999"), nil
	case "TIMESTAMP":
		return col.(*array.Timestamp).Value(rowIdx).ToTime(arrow.Microsecond).UTC().Format("2006-01-02 15:04:05.999999999"), nil
	case "TIME":
		return col.(*array.Time64).Value(rowIdx).ToTime(arrow.Microsecond).Format("15:04:05.999999999"), nil
	default:
		return "", fmt.Errorf("column type %q has no Arrow mapping in this emulator yet", bqType)
	}
}
