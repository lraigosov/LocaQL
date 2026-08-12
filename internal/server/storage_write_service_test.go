package server

import (
	"context"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newTestStorageWriteClient(t *testing.T, s *Server) storagepb.BigQueryWriteClient {
	t.Helper()
	return storagepb.NewBigQueryWriteClient(newTestStorageGRPCConn(t, s))
}

// protoField builds one FieldDescriptorProto for a hand-assembled test
// message schema — the same shape a real Storage Write API client sends as
// ProtoSchema.proto_descriptor, just constructed directly instead of via a
// compiled .proto file.
func protoField(name string, number int32, typ descriptorpb.FieldDescriptorProto_Type, repeated bool) *descriptorpb.FieldDescriptorProto {
	label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	if repeated {
		label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	}
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Label:  label.Enum(),
		Type:   typ.Enum(),
	}
}

func idNameDescriptor() *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("IdName"),
		Field: []*descriptorpb.FieldDescriptorProto{
			protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
			protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
		},
	}
}

// encodeTestProtoRow serializes one row against desc, mirroring what a real
// Storage Write API client does client-side before sending ProtoRows.
func encodeTestProtoRow(t *testing.T, desc *descriptorpb.DescriptorProto, values map[string]any) []byte {
	t.Helper()
	md, err := buildDynamicMessageDescriptor(desc)
	if err != nil {
		t.Fatalf("build message descriptor: %v", err)
	}
	msg := dynamicpb.NewMessage(md)
	for name, v := range values {
		fd := md.Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			t.Fatalf("no such field %q in descriptor", name)
		}
		var pv protoreflect.Value
		switch val := v.(type) {
		case int64:
			pv = protoreflect.ValueOfInt64(val)
		case string:
			pv = protoreflect.ValueOfString(val)
		case bool:
			pv = protoreflect.ValueOfBool(val)
		case float64:
			pv = protoreflect.ValueOfFloat64(val)
		default:
			t.Fatalf("unsupported test value type %T for field %q", v, name)
		}
		msg.Set(fd, pv)
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal test proto row: %v", err)
	}
	return data
}

// encodeTestArrowBatch builds a real Arrow record from fields/rows (the same
// buildArrowRecord this project's Storage Read Arrow path uses) and
// serializes its schema and record batch into the two standalone IPC
// messages a real client would send as AppendRowsRequest.ArrowRows.
func encodeTestArrowBatch(t *testing.T, fields []tableField, rows [][]string) (schemaMsg, batchMsg []byte) {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema, err := buildArrowSchema(fields)
	if err != nil {
		t.Fatalf("buildArrowSchema: %v", err)
	}
	rec, err := buildArrowRecord(mem, schema, fields, rows)
	if err != nil {
		t.Fatalf("buildArrowRecord: %v", err)
	}
	defer rec.Release()

	schemaMsg, err = serializeArrowSchemaMessage(schema, mem)
	if err != nil {
		t.Fatalf("serializeArrowSchemaMessage: %v", err)
	}
	batchMsg, err = serializeArrowRecordBatchMessage(rec, mem)
	if err != nil {
		t.Fatalf("serializeArrowRecordBatchMessage: %v", err)
	}
	return schemaMsg, batchMsg
}

func writeStreamName(datasetID, tableID, streamID string) string {
	return "projects/p1/datasets/" + datasetID + "/tables/" + tableID + "/streams/" + streamID
}

func TestStorageWriteDefaultStreamAppendsRowsImmediately(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_default", idNameFields(), "id,name\n1,alpha\n")

	client := newTestStorageWriteClient(t, s)
	ctx := context.Background()
	appendClient, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}

	desc := idNameDescriptor()
	row := encodeTestProtoRow(t, desc, map[string]any{"id": int64(2), "name": "beta"})
	req := &storagepb.AppendRowsRequest{
		WriteStream: writeStreamName("analytics", "write_default", storageWriteDefaultStreamID),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: desc},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{row}},
		}},
	}
	if err := appendClient.Send(req); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := appendClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if resp.GetError() != nil {
		t.Fatalf("expected a successful append, got error: %v", resp.GetError())
	}
	_ = appendClient.CloseSend()

	_, rows, ok := s.tables.getData("p1", "analytics", "write_default")
	if !ok {
		t.Fatalf("expected table to exist")
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (1 loaded + 1 appended), got %d: %v", len(rows), rows)
	}
	found := false
	for _, r := range rows {
		if len(r) >= 2 && r[1] == "beta" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the appended row (name=beta) to be visible immediately, got %v", rows)
	}
}

func TestStorageWriteCommittedStreamOffsetTracking(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_committed_offset", idNameFields(), "id,name\n")

	readClient := newTestStorageReadClient(t, s) // unused directly, but confirms both services share one gRPC server without port conflicts
	_ = readClient

	writeClient := newTestStorageWriteClient(t, s)
	ctx := context.Background()

	created, err := writeClient.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent:      "projects/p1/datasets/analytics/tables/write_committed_offset",
		WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_COMMITTED},
	})
	if err != nil {
		t.Fatalf("CreateWriteStream: %v", err)
	}

	appendClient, err := writeClient.AppendRows(ctx)
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	desc := idNameDescriptor()
	row1 := encodeTestProtoRow(t, desc, map[string]any{"id": int64(1), "name": "alpha"})

	send := func(offset int64, rows ...[]byte) *storagepb.AppendRowsResponse {
		t.Helper()
		req := &storagepb.AppendRowsRequest{
			WriteStream: created.GetName(),
			Offset:      wrapperspb.Int64(offset),
			Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
				WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: desc},
				Rows:         &storagepb.ProtoRows{SerializedRows: rows},
			}},
		}
		if err := appendClient.Send(req); err != nil {
			t.Fatalf("Send: %v", err)
		}
		resp, err := appendClient.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		return resp
	}

	first := send(0, row1)
	if first.GetError() != nil {
		t.Fatalf("expected first append at offset 0 to succeed, got %v", first.GetError())
	}
	if first.GetAppendResult().GetOffset().GetValue() != 0 {
		t.Fatalf("expected AppendResult.offset=0, got %v", first.GetAppendResult().GetOffset())
	}

	retry := send(0, row1)
	if status.FromProto(retry.GetError()).Code() != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists retrying offset 0, got %v", retry.GetError())
	}

	tooFar := send(5, row1)
	if status.FromProto(tooFar.GetError()).Code() != codes.OutOfRange {
		t.Fatalf("expected OutOfRange for offset 5 (stream only has 1 row), got %v", tooFar.GetError())
	}

	row2 := encodeTestProtoRow(t, desc, map[string]any{"id": int64(2), "name": "beta"})
	second := send(1, row2)
	if second.GetError() != nil {
		t.Fatalf("expected append at the real next offset (1) to succeed, got %v", second.GetError())
	}
	_ = appendClient.CloseSend()

	_, rows, _ := s.tables.getData("p1", "analytics", "write_committed_offset")
	if len(rows) != 2 {
		t.Fatalf("expected 2 committed rows (alpha, beta), got %d: %v", len(rows), rows)
	}
}

func TestStorageWritePendingStreamBufferedUntilCommit(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_pending", idNameFields(), "id,name\n")

	client := newTestStorageWriteClient(t, s)
	ctx := context.Background()

	created, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent:      "projects/p1/datasets/analytics/tables/write_pending",
		WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_PENDING},
	})
	if err != nil {
		t.Fatalf("CreateWriteStream: %v", err)
	}

	appendClient, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	desc := idNameDescriptor()
	row := encodeTestProtoRow(t, desc, map[string]any{"id": int64(1), "name": "alpha"})
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		WriteStream: created.GetName(),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: desc},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{row}},
		}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := appendClient.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	_ = appendClient.CloseSend()

	_, rowsBeforeCommit, _ := s.tables.getData("p1", "analytics", "write_pending")
	if len(rowsBeforeCommit) != 0 {
		t.Fatalf("expected PENDING rows to stay invisible before commit, got %v", rowsBeforeCommit)
	}

	if _, err := client.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("FinalizeWriteStream: %v", err)
	}
	commitResp, err := client.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       "projects/p1/datasets/analytics/tables/write_pending",
		WriteStreams: []string{created.GetName()},
	})
	if err != nil {
		t.Fatalf("BatchCommitWriteStreams: %v", err)
	}
	if len(commitResp.GetStreamErrors()) > 0 {
		t.Fatalf("expected no stream errors, got %v", commitResp.GetStreamErrors())
	}
	if commitResp.GetCommitTime() == nil {
		t.Fatalf("expected a commit_time on success")
	}

	_, rowsAfterCommit, _ := s.tables.getData("p1", "analytics", "write_pending")
	if len(rowsAfterCommit) != 1 {
		t.Fatalf("expected 1 row visible after commit, got %v", rowsAfterCommit)
	}
}

func TestStorageWriteBatchCommitRejectsNonFinalizedStream(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_not_finalized", idNameFields(), "id,name\n")

	client := newTestStorageWriteClient(t, s)
	ctx := context.Background()
	created, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent:      "projects/p1/datasets/analytics/tables/write_not_finalized",
		WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_PENDING},
	})
	if err != nil {
		t.Fatalf("CreateWriteStream: %v", err)
	}

	commitResp, err := client.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       "projects/p1/datasets/analytics/tables/write_not_finalized",
		WriteStreams: []string{created.GetName()},
	})
	if err != nil {
		t.Fatalf("BatchCommitWriteStreams: %v", err)
	}
	if len(commitResp.GetStreamErrors()) == 0 {
		t.Fatalf("expected a stream error committing a non-finalized stream")
	}
}

func TestStorageWriteGetWriteStreamReturnsSchema(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_get_stream", idNameFields(), "id,name\n")

	client := newTestStorageWriteClient(t, s)
	ctx := context.Background()
	created, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent:      "projects/p1/datasets/analytics/tables/write_get_stream",
		WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_COMMITTED},
	})
	if err != nil {
		t.Fatalf("CreateWriteStream: %v", err)
	}

	got, err := client.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetWriteStream: %v", err)
	}
	if len(got.GetTableSchema().GetFields()) != 2 {
		t.Fatalf("expected 2 schema fields (id, name), got %v", got.GetTableSchema())
	}
}

// TestStorageWriteGetWriteStreamResolvesDefaultStream guards against a real
// regression found via the official cloud.google.com/go/bigquery/storage/
// managedwriter client: setting up a DefaultStream writer calls
// GetWriteStream on the well-known `_default` stream as part of its own
// connection-pool resolution, even though `_default` is never registered via
// CreateWriteStream (see AppendRows' own isDefault handling). Without this,
// GetWriteStream 404s and that client can never append to `_default` at all,
// despite AppendRows itself working for it when called directly.
func TestStorageWriteGetWriteStreamResolvesDefaultStream(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_get_default", idNameFields(), "id,name\n1,alpha\n")

	client := newTestStorageWriteClient(t, s)
	ctx := context.Background()

	name := writeStreamName("analytics", "write_get_default", storageWriteDefaultStreamID)
	got, err := client.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{Name: name})
	if err != nil {
		t.Fatalf("GetWriteStream(_default): %v", err)
	}
	if got.GetName() != name {
		t.Fatalf("expected name %q, got %q", name, got.GetName())
	}
	if got.GetType() != storagepb.WriteStream_COMMITTED {
		t.Fatalf("expected the _default stream to report COMMITTED semantics, got %v", got.GetType())
	}
	if len(got.GetTableSchema().GetFields()) != 2 {
		t.Fatalf("expected 2 schema fields (id, name), got %v", got.GetTableSchema())
	}
}

func TestStorageWriteGetWriteStreamDefaultStreamMissingTableFailsExplicitly(t *testing.T) {
	s := newTestServer()
	client := newTestStorageWriteClient(t, s)

	name := writeStreamName("analytics", "does_not_exist", storageWriteDefaultStreamID)
	_, err := client.GetWriteStream(context.Background(), &storagepb.GetWriteStreamRequest{Name: name})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for a missing table's _default stream, got %v", err)
	}
}

func TestStorageWriteFlushRowsIsUnimplemented(t *testing.T) {
	s := newTestServer()
	client := newTestStorageWriteClient(t, s)
	_, err := client.FlushRows(context.Background(), &storagepb.FlushRowsRequest{WriteStream: "projects/p1/datasets/analytics/tables/x/streams/y"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
}

func TestStorageWriteCreateWriteStreamRejectsBuffered(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_buffered", idNameFields(), "id,name\n")

	client := newTestStorageWriteClient(t, s)
	_, err := client.CreateWriteStream(context.Background(), &storagepb.CreateWriteStreamRequest{
		Parent:      "projects/p1/datasets/analytics/tables/write_buffered",
		WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_BUFFERED},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented for BUFFERED, got %v", err)
	}
}

func TestStorageWriteCreateWriteStreamRejectsNestedSchema(t *testing.T) {
	s := newTestServer()
	loadNDJSONTable(t, s, "analytics", "write_nested",
		[]map[string]any{
			{"name": "id", "type": "INT64"},
			{"name": "info", "type": "RECORD", "fields": []map[string]any{
				{"name": "city", "type": "STRING"},
			}},
		},
		`{"id":1,"info":{"city":"Bogota"}}`+"\n")

	client := newTestStorageWriteClient(t, s)
	_, err := client.CreateWriteStream(context.Background(), &storagepb.CreateWriteStreamRequest{
		Parent:      "projects/p1/datasets/analytics/tables/write_nested",
		WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_COMMITTED},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a nested schema, got %v", err)
	}
}

func TestStorageWriteAppendRowsRejectsRepeatedProtoField(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_repeated_field", idNameFields(), "id,name\n")

	client := newTestStorageWriteClient(t, s)
	ctx := context.Background()
	appendClient, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}

	desc := &descriptorpb.DescriptorProto{
		Name: proto.String("Repeated"),
		Field: []*descriptorpb.FieldDescriptorProto{
			protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
			protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, true),
		},
	}
	md, err := buildDynamicMessageDescriptor(desc)
	if err != nil {
		t.Fatalf("build message descriptor: %v", err)
	}
	msg := dynamicpb.NewMessage(md)
	fd := md.Fields().ByName("id")
	msg.Set(fd, protoreflect.ValueOfInt64(1))
	nameFd := md.Fields().ByName("name")
	list := msg.NewField(nameFd).List()
	list.Append(protoreflect.ValueOfString("alpha"))
	msg.Set(nameFd, protoreflect.ValueOfList(list))
	rowBytes, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		WriteStream: writeStreamName("analytics", "write_repeated_field", storageWriteDefaultStreamID),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: desc},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{rowBytes}},
		}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := appendClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if len(resp.GetRowErrors()) == 0 {
		t.Fatalf("expected a row error for the repeated proto field, got %v", resp)
	}
}

// TestStorageWriteDefaultStreamAppendsArrowRowsImmediately mirrors
// TestStorageWriteDefaultStreamAppendsRowsImmediately for
// AppendRowsRequest.ArrowRows: a real Arrow-encoded batch, appended to the
// `_default` stream, visible in the catalog immediately.
func TestStorageWriteDefaultStreamAppendsArrowRowsImmediately(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_arrow_default", idNameFields(), "id,name\n1,alpha\n")

	client := newTestStorageWriteClient(t, s)
	ctx := context.Background()
	appendClient, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}

	fields := []tableField{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "name", Type: "STRING", Mode: "NULLABLE"},
	}
	schemaMsg, batchMsg := encodeTestArrowBatch(t, fields, [][]string{{"2", "beta"}})

	req := &storagepb.AppendRowsRequest{
		WriteStream: writeStreamName("analytics", "write_arrow_default", storageWriteDefaultStreamID),
		Rows: &storagepb.AppendRowsRequest_ArrowRows{ArrowRows: &storagepb.AppendRowsRequest_ArrowData{
			WriterSchema: &storagepb.ArrowSchema{SerializedSchema: schemaMsg},
			Rows:         &storagepb.ArrowRecordBatch{SerializedRecordBatch: batchMsg},
		}},
	}
	if err := appendClient.Send(req); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := appendClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if resp.GetError() != nil {
		t.Fatalf("expected a successful append, got error: %v", resp.GetError())
	}
	_ = appendClient.CloseSend()

	_, rows, ok := s.tables.getData("p1", "analytics", "write_arrow_default")
	if !ok {
		t.Fatalf("expected table to exist")
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (1 loaded + 1 appended), got %d: %v", len(rows), rows)
	}
	found := false
	for _, r := range rows {
		if len(r) >= 2 && r[1] == "beta" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the appended Arrow row (name=beta) to be visible immediately, got %v", rows)
	}
}

// TestStorageWritePendingStreamBuffersArrowRowsUntilCommit mirrors the
// proto_rows PENDING-stream commit flow, using Arrow rows instead, proving
// the two encodings converge on the same buffered-until-commit behavior.
func TestStorageWritePendingStreamBuffersArrowRowsUntilCommit(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_arrow_pending", idNameFields(), "id,name\n1,alpha\n")

	writeClient := newTestStorageWriteClient(t, s)
	ctx := context.Background()

	created, err := writeClient.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent:      "projects/p1/datasets/analytics/tables/write_arrow_pending",
		WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_PENDING},
	})
	if err != nil {
		t.Fatalf("CreateWriteStream: %v", err)
	}

	appendClient, err := writeClient.AppendRows(ctx)
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	fields := []tableField{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "name", Type: "STRING", Mode: "NULLABLE"},
	}
	schemaMsg, batchMsg := encodeTestArrowBatch(t, fields, [][]string{{"2", "beta"}})
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		WriteStream: created.GetName(),
		Rows: &storagepb.AppendRowsRequest_ArrowRows{ArrowRows: &storagepb.AppendRowsRequest_ArrowData{
			WriterSchema: &storagepb.ArrowSchema{SerializedSchema: schemaMsg},
			Rows:         &storagepb.ArrowRecordBatch{SerializedRecordBatch: batchMsg},
		}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := appendClient.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	_ = appendClient.CloseSend()

	if _, rows, ok := s.tables.getData("p1", "analytics", "write_arrow_pending"); ok && len(rows) != 1 {
		t.Fatalf("expected the buffered Arrow row not yet visible before commit, got %d rows", len(rows))
	}

	if _, err := writeClient.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("FinalizeWriteStream: %v", err)
	}
	if _, err := writeClient.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       "projects/p1/datasets/analytics/tables/write_arrow_pending",
		WriteStreams: []string{created.GetName()},
	}); err != nil {
		t.Fatalf("BatchCommitWriteStreams: %v", err)
	}

	_, rows, ok := s.tables.getData("p1", "analytics", "write_arrow_pending")
	if !ok || len(rows) != 2 {
		t.Fatalf("expected 2 rows after commit, got %d: %v", len(rows), rows)
	}
}

// TestStorageWriteArrowRowsRequiresWriterSchema documents that arrow_rows
// without a writer_schema fails explicitly, mirroring proto_rows' equivalent
// requirement.
func TestStorageWriteArrowRowsRequiresWriterSchema(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "write_arrow_no_schema", idNameFields(), "id,name\n1,alpha\n")

	client := newTestStorageWriteClient(t, s)
	appendClient, err := client.AppendRows(context.Background())
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	_, batchMsg := encodeTestArrowBatch(t, []tableField{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "name", Type: "STRING", Mode: "NULLABLE"},
	}, [][]string{{"2", "beta"}})

	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		WriteStream: writeStreamName("analytics", "write_arrow_no_schema", storageWriteDefaultStreamID),
		Rows: &storagepb.AppendRowsRequest_ArrowRows{ArrowRows: &storagepb.AppendRowsRequest_ArrowData{
			Rows: &storagepb.ArrowRecordBatch{SerializedRecordBatch: batchMsg},
		}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := appendClient.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for missing writer_schema, got %v", err)
	}
}
