package server

import (
	"bytes"
	"context"
	"net"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newTestStorageGRPCConn boots a real grpc.Server backed by s (over an
// in-memory bufconn listener, no real socket needed) with both the Read and
// Write services registered (NewStorageGRPCServer registers both), and
// returns a real *grpc.ClientConn talking to it — this exercises the actual
// wire protocol (protobuf/grpc framing, not just direct Go method calls),
// the same fidelity bar httptest.NewRecorder gives the REST handlers
// elsewhere in this test suite.
func newTestStorageGRPCConn(t *testing.T, s *Server) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	gs := s.NewStorageGRPCServer()
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newTestStorageReadClient(t *testing.T, s *Server) storagepb.BigQueryReadClient {
	t.Helper()
	return storagepb.NewBigQueryReadClient(newTestStorageGRPCConn(t, s))
}

func readAllStorageRows(t *testing.T, resp *storagepb.ReadRowsResponse) []map[string]any {
	t.Helper()
	avroRows := resp.GetAvroRows()
	if avroRows == nil {
		t.Fatalf("expected AvroRows in response, got %v", resp.GetRows())
	}
	schemaJSON := resp.GetAvroSchema().GetSchema()
	if schemaJSON == "" {
		t.Fatalf("expected an avro schema on the first ReadRowsResponse")
	}
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		t.Fatalf("build avro codec from returned schema: %v", err)
	}

	var rows []map[string]any
	buf := avroRows.GetSerializedBinaryRows()
	for len(buf) > 0 {
		native, rest, err := codec.NativeFromBinary(buf)
		if err != nil {
			t.Fatalf("decode avro row: %v", err)
		}
		row := native.(map[string]any)
		for name, value := range row {
			row[name] = unwrapAvroUnion(value)
		}
		rows = append(rows, row)
		buf = rest
	}
	if int64(len(rows)) != resp.GetRowCount() {
		t.Fatalf("decoded %d rows but RowCount said %d", len(rows), resp.GetRowCount())
	}
	return rows
}

func storageTableName(datasetID, tableID string) string {
	return "projects/p1/datasets/" + datasetID + "/tables/" + tableID
}

func idNameFields() []map[string]any {
	return []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "name", "type": "STRING"},
	}
}

func TestStorageReadSessionAndReadRowsRoundTrip(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_events", idNameFields(),
		"id,name\n1,alpha\n2,beta\n3,gamma\n")

	client := newTestStorageReadClient(t, s)
	ctx := context.Background()

	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent:      "projects/p1",
		ReadSession: &storagepb.ReadSession{Table: storageTableName("analytics", "storage_events")},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	if session.GetDataFormat() != storagepb.DataFormat_AVRO {
		t.Fatalf("expected DATA_FORMAT_AVRO, got %v", session.GetDataFormat())
	}
	if len(session.GetStreams()) != 1 {
		t.Fatalf("expected exactly 1 stream (bounded scope), got %d", len(session.GetStreams()))
	}
	if session.GetEstimatedRowCount() != 3 {
		t.Fatalf("expected EstimatedRowCount=3, got %d", session.GetEstimatedRowCount())
	}

	rowsClient, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	resp, err := rowsClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rows := readAllStorageRows(t, resp)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r["name"].(string)] = true
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		if !names[want] {
			t.Errorf("expected row %q in the decoded avro rows, got %v", want, rows)
		}
	}
}

func TestStorageReadSessionAppliesColumnProjection(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_projection", idNameFields(),
		"id,name\n1,alpha\n2,beta\n")

	client := newTestStorageReadClient(t, s)
	ctx := context.Background()

	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table:       storageTableName("analytics", "storage_projection"),
			ReadOptions: &storagepb.ReadSession_TableReadOptions{SelectedFields: []string{"name"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}

	rowsClient, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	resp, err := rowsClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rows := readAllStorageRows(t, resp)
	for _, r := range rows {
		if _, hasID := r["id"]; hasID {
			t.Fatalf("expected only the projected 'name' column, got id in row %v", r)
		}
		if _, hasName := r["name"]; !hasName {
			t.Fatalf("expected the projected 'name' column, got %v", r)
		}
	}
}

func TestStorageReadSessionAppliesRowRestriction(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_restriction", idNameFields(),
		"id,name\n1,alpha\n2,beta\n3,gamma\n")

	client := newTestStorageReadClient(t, s)
	ctx := context.Background()

	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table:       storageTableName("analytics", "storage_restriction"),
			ReadOptions: &storagepb.ReadSession_TableReadOptions{RowRestriction: "id > 1"},
		},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	if session.GetEstimatedRowCount() != 2 {
		t.Fatalf("expected row_restriction to leave 2 rows, got %d", session.GetEstimatedRowCount())
	}

	rowsClient, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	resp, err := rowsClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rows := readAllStorageRows(t, resp)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after row_restriction, got %d", len(rows))
	}
	for _, r := range rows {
		if r["name"] == "alpha" {
			t.Fatalf("expected row_restriction 'id > 1' to exclude id=1 (alpha), got %v", rows)
		}
	}
}

func TestStorageReadSessionTableNotFoundReturnsNotFound(t *testing.T) {
	s := newTestServer()
	client := newTestStorageReadClient(t, s)

	_, err := client.CreateReadSession(context.Background(), &storagepb.CreateReadSessionRequest{
		Parent:      "projects/p1",
		ReadSession: &storagepb.ReadSession{Table: storageTableName("analytics", "does_not_exist")},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// TestStorageReadSessionArrowFormatRoundTrip exercises real
// DATA_FORMAT_ARROW end to end over a real gRPC connection: request Arrow
// explicitly, decode ReadSession.GetArrowSchema() and the ReadRowsResponse's
// ArrowRecordBatch with arrow-go's own reader (the same library any real
// client would use), and verify the actual decoded values — not just that
// the call didn't fail.
func TestStorageReadSessionArrowFormatRoundTrip(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_arrow", idNameFields(), "id,name\n1,alpha\n2,beta\n")

	client := newTestStorageReadClient(t, s)
	ctx := context.Background()

	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table:      storageTableName("analytics", "storage_arrow"),
			DataFormat: storagepb.DataFormat_ARROW,
		},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	if session.GetDataFormat() != storagepb.DataFormat_ARROW {
		t.Fatalf("expected DATA_FORMAT_ARROW, got %v", session.GetDataFormat())
	}
	schemaMsg := session.GetArrowSchema().GetSerializedSchema()
	if len(schemaMsg) == 0 {
		t.Fatalf("expected a non-empty serialized Arrow schema")
	}
	mem := memory.NewGoAllocator()
	schema, err := flight.DeserializeSchema(schemaMsg, mem)
	if err != nil {
		t.Fatalf("decode ReadSession arrow schema: %v", err)
	}
	if schema.Field(0).Name != "id" || schema.Field(1).Name != "name" {
		t.Fatalf("unexpected schema fields: %v", schema)
	}

	rowsClient, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	resp, err := rowsClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	batch := resp.GetArrowRecordBatch()
	if batch == nil {
		t.Fatalf("expected an ArrowRecordBatch, got %v", resp.GetRows())
	}
	if batch.GetRowCount() != 2 {
		t.Fatalf("expected RowCount=2, got %d", batch.GetRowCount())
	}

	// Decode the standalone record-batch message the same way a real client
	// does: concatenate with the schema message and feed to arrow-go's own
	// stream reader (see storage_arrow_test.go for why this is the correct
	// equivalence check).
	var stream bytes.Buffer
	stream.Write(schemaMsg)
	stream.Write(batch.GetSerializedRecordBatch())
	stream.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00})
	reader, err := ipc.NewReader(bytes.NewReader(stream.Bytes()), ipc.WithAllocator(mem))
	if err != nil {
		t.Fatalf("ipc.NewReader: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatalf("expected a record batch, got none (err: %v)", reader.Err())
	}
	rec := reader.Record()
	idCol := rec.Column(0).(*array.Int64)
	nameCol := rec.Column(1).(*array.String)
	if idCol.Value(0) != 1 || nameCol.Value(0) != "alpha" || idCol.Value(1) != 2 || nameCol.Value(1) != "beta" {
		t.Fatalf("unexpected decoded rows: id=[%d,%d] name=[%q,%q]", idCol.Value(0), idCol.Value(1), nameCol.Value(0), nameCol.Value(1))
	}
}

// TestStorageReadSessionArrowRejectsBufferCompression documents the one
// declared bound on Arrow support: buffer_compression is not implemented.
func TestStorageReadSessionArrowRejectsBufferCompression(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_arrow_compressed", idNameFields(), "id,name\n1,alpha\n")

	client := newTestStorageReadClient(t, s)
	_, err := client.CreateReadSession(context.Background(), &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table:      storageTableName("analytics", "storage_arrow_compressed"),
			DataFormat: storagepb.DataFormat_ARROW,
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				OutputFormatSerializationOptions: &storagepb.ReadSession_TableReadOptions_ArrowSerializationOptions{
					ArrowSerializationOptions: &storagepb.ArrowSerializationOptions{
						BufferCompression: storagepb.ArrowSerializationOptions_LZ4_FRAME,
					},
				},
			},
		},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented for Arrow buffer compression, got %v", err)
	}
}

// TestStorageReadSessionArrowRejectsUnsupportedColumnType documents that a
// NUMERIC column (real BigQuery maps this to Arrow Decimal128, not built by
// this emulator yet) fails DATA_FORMAT_ARROW explicitly rather than silently
// downgrading to a string column.
func TestStorageReadSessionArrowRejectsUnsupportedColumnType(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_arrow_numeric", []map[string]any{
		{"name": "amount", "type": "NUMERIC"},
	}, "amount\n1.50\n")

	client := newTestStorageReadClient(t, s)
	_, err := client.CreateReadSession(context.Background(), &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table:      storageTableName("analytics", "storage_arrow_numeric"),
			DataFormat: storagepb.DataFormat_ARROW,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for an unsupported Arrow column type, got %v", err)
	}
}

func TestStorageReadRowsUnknownStreamReturnsNotFound(t *testing.T) {
	s := newTestServer()
	client := newTestStorageReadClient(t, s)

	rowsClient, err := client.ReadRows(context.Background(), &storagepb.ReadRowsRequest{ReadStream: "projects/p1/locations/local/sessions/nope/streams/nope"})
	if err != nil {
		t.Fatalf("ReadRows call itself should not fail: %v", err)
	}
	_, err = rowsClient.Recv()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for an unknown stream, got %v", err)
	}
}

func TestStorageSplitReadStreamIsUnimplemented(t *testing.T) {
	s := newTestServer()
	client := newTestStorageReadClient(t, s)

	_, err := client.SplitReadStream(context.Background(), &storagepb.SplitReadStreamRequest{Name: "projects/p1/locations/local/sessions/x/streams/y"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
}

func TestStorageReadSessionRejectsNestedSchema(t *testing.T) {
	s := newTestServer()
	loadNDJSONTable(t, s, "analytics", "storage_nested",
		[]map[string]any{
			{"name": "id", "type": "INT64"},
			{"name": "info", "type": "RECORD", "fields": []map[string]any{
				{"name": "city", "type": "STRING"},
			}},
		},
		`{"id":1,"info":{"city":"Bogota"}}`+"\n")

	client := newTestStorageReadClient(t, s)
	_, err := client.CreateReadSession(context.Background(), &storagepb.CreateReadSessionRequest{
		Parent:      "projects/p1",
		ReadSession: &storagepb.ReadSession{Table: storageTableName("analytics", "storage_nested")},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a nested schema (Avro-only scope), got %v", err)
	}
}
