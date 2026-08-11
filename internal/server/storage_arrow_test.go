package server

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestArrowIPCMessageFraming_RoundTrip proves serializeArrowIPCMessage
// produces byte-correct, standalone Arrow IPC messages — not by hand-checking
// padding math, but by concatenating a schema-only message with a
// record-batch-only message and feeding the result into arrow-go's own
// ipc.NewReader (an ordinary Arrow IPC *stream* is defined as exactly that
// sequence: one schema message, then zero or more record-batch messages,
// then an end-of-stream marker). If arrow-go's own reader — code this
// project does not control — can parse the concatenation back into the
// original data, the framing this file writes independently (see the
// package doc comment on serializeArrowIPCMessage for why it can't just call
// arrow-go's unexported writeIPCPayload) is provably correct against the
// real spec, matching how the official Java BigQuery Storage sample decodes
// ArrowSchema.serialized_schema and ArrowRecordBatch.serialized_record_batch
// independently of each other.
func TestArrowIPCMessageFraming_RoundTrip(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	fields := []tableField{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "name", Type: "STRING", Mode: "NULLABLE"},
		{Name: "active", Type: "BOOL", Mode: "REQUIRED"},
	}
	rows := [][]string{
		{"1", "one", "true"},
		{"2", storedNullCell, "false"},
	}

	rec, err := buildArrowRecord(mem, schema, fields, rows)
	if err != nil {
		t.Fatalf("buildArrowRecord: %v", err)
	}
	defer rec.Release()

	schemaMsg, err := serializeArrowSchemaMessage(schema, mem)
	if err != nil {
		t.Fatalf("serializeArrowSchemaMessage: %v", err)
	}
	batchMsg, err := serializeArrowRecordBatchMessage(rec, mem)
	if err != nil {
		t.Fatalf("serializeArrowRecordBatchMessage: %v", err)
	}

	// A schema message on its own must also be independently decodable via
	// flight.DeserializeSchema, exactly as the official Java sample does for
	// ReadSession.GetArrowSchema().GetSerializedSchema().
	decodedSchema, err := flight.DeserializeSchema(schemaMsg, mem)
	if err != nil {
		t.Fatalf("flight.DeserializeSchema(schemaMsg): %v", err)
	}
	if !decodedSchema.Equal(schema) {
		t.Fatalf("decoded schema mismatch:\n got: %v\nwant: %v", decodedSchema, schema)
	}

	// EOS marker: a bare continuation token with a zero-length body, exactly
	// what ipc.NewWriter's Close() appends to end a stream.
	eos := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00}

	var stream bytes.Buffer
	stream.Write(schemaMsg)
	stream.Write(batchMsg)
	stream.Write(eos)

	reader, err := ipc.NewReader(bytes.NewReader(stream.Bytes()), ipc.WithAllocator(mem))
	if err != nil {
		t.Fatalf("ipc.NewReader on concatenated messages: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatalf("expected one record batch, got none (err: %v)", reader.Err())
	}
	got := reader.Record()
	if got.NumRows() != 2 || got.NumCols() != 3 {
		t.Fatalf("unexpected record shape: %d rows, %d cols", got.NumRows(), got.NumCols())
	}

	idCol := got.Column(0).(*array.Int64)
	if idCol.Value(0) != 1 || idCol.Value(1) != 2 {
		t.Fatalf("id column mismatch: got [%d, %d], want [1, 2]", idCol.Value(0), idCol.Value(1))
	}

	nameCol := got.Column(1).(*array.String)
	if nameCol.Value(0) != "one" || !nameCol.IsNull(1) {
		t.Fatalf("name column mismatch: got [%q, null=%v], want [\"one\", null=true]", nameCol.Value(0), nameCol.IsNull(1))
	}

	activeCol := got.Column(2).(*array.Boolean)
	if activeCol.Value(0) != true || activeCol.Value(1) != false {
		t.Fatalf("active column mismatch: got [%v, %v], want [true, false]", activeCol.Value(0), activeCol.Value(1))
	}

	if reader.Next() {
		t.Fatalf("expected exactly one record batch, got a second one")
	}
}
