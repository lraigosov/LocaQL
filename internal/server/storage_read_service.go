package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// storageReadLocation is a fixed placeholder for the {location} path segment
// in session/stream resource names. Real BigQuery Storage API connections
// are made to a region-specific endpoint (the region is implied by which
// endpoint you dial, never passed in a request field) — LocaQL has no
// region concept anywhere else either, so this is a constant rather than
// pretending to model multi-region routing.
const storageReadLocation = "local"

// storageReadStream is one CreateReadSession stream's resolved data: rows
// materialized once (via the same real SQL engine every other query in this
// project uses — column projection and row_restriction become a real SELECT
// ... WHERE ... run through executeRealSQLQuery, not a hand-rolled filter)
// plus the Avro codec used to binary-encode them for ReadRows.
type storageReadStream struct {
	fields      []tableField
	rows        [][]string
	dataFormat  storagepb.DataFormat
	codec       *goavro.Codec // set when dataFormat == AVRO
	arrowSchema *arrow.Schema // set when dataFormat == ARROW
}

// storageReadService implements storagepb.BigQueryReadServer: the bounded
// first increment of BigQuery Storage API emulation (see KNOWN-DIVERGENCES.md
// and capabilities/registry.yaml `grpc.storage.read` for the exact scope).
// Only CreateReadSession + ReadRows, one stream per session, Avro framing —
// SplitReadStream and the Write API are explicitly unimplemented, not
// silently degraded.
type storageReadService struct {
	storagepb.UnimplementedBigQueryReadServer

	server *Server

	mu      sync.Mutex
	counter int64
	streams map[string]*storageReadStream // key: stream resource name
}

func newStorageReadService(s *Server) *storageReadService {
	return &storageReadService{server: s, streams: make(map[string]*storageReadStream)}
}

// NewStorageGRPCServer builds the gRPC server exposing the BigQuery Storage
// Read API against this Server's catalog — a separate listener/port from
// the REST server, matching the real API (a distinct gRPC service, not part
// of the JSON REST surface) and the local-emulator convention already used
// by Google's own Firestore/Pub/Sub/Bigtable emulators: plaintext, no TLS,
// no auth interceptor, consistent with this emulator's permanent
// anonymous-only design (see KNOWN-DIVERGENCES.md, master plan §24).
func (s *Server) NewStorageGRPCServer() *grpc.Server {
	gs := grpc.NewServer()
	storagepb.RegisterBigQueryReadServer(gs, newStorageReadService(s))
	storagepb.RegisterBigQueryWriteServer(gs, newStorageWriteService(s))
	return gs
}

// parseStorageTableName parses the BigQuery Storage API's table resource
// name shape: projects/{project_id}/datasets/{dataset_id}/tables/{table_id}.
func parseStorageTableName(name string) (projectID, datasetID, tableID string, err error) {
	parts := strings.Split(strings.Trim(strings.TrimSpace(name), "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "datasets" || parts[4] != "tables" {
		return "", "", "", fmt.Errorf("invalid table name %q: expected projects/{project}/datasets/{dataset}/tables/{table}", name)
	}
	return parts[1], parts[3], parts[5], nil
}

// buildStorageReadQuery renders a CreateReadSession's column projection
// (TableReadOptions.selected_fields) and push-down filter
// (TableReadOptions.row_restriction) as a real SELECT statement, executed
// through the same real GoogleSQL engine every other query in this project
// uses — genuine WHERE-clause semantics, not a hand-rolled filter
// interpreter, and free column projection since SELECT already does that.
// The FROM reference is deliberately bare (dataset.table, no backticks):
// referencedTables' regex (sql_engine.go) only recognizes a single pair of
// backticks wrapping the whole dotted reference or none at all, matching
// the bare form already used by every other hand-written query in this
// project — per-segment-quoted identifiers like `dataset`.`table` would
// silently fail to materialize and surface as a confusing "table not
// found" from the engine instead.
func buildStorageReadQuery(datasetID, tableID string, selectedFields []string, rowRestriction string) string {
	columnsExpr := "*"
	if len(selectedFields) > 0 {
		quoted := make([]string, len(selectedFields))
		for i, f := range selectedFields {
			quoted[i] = quoteIdent(f)
		}
		columnsExpr = strings.Join(quoted, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s.%s", columnsExpr, datasetID, tableID)
	if rr := strings.TrimSpace(rowRestriction); rr != "" {
		query += " WHERE " + rr
	}
	return query
}

// parseStorageReadOptions extracts CreateReadSessionRequest's column
// projection/push-down filter and rejects an Arrow buffer-compression
// request explicitly (not implemented — see configureArrowReadStream).
func parseStorageReadOptions(rs *storagepb.ReadSession) (selectedFields []string, rowRestriction string, err error) {
	opts := rs.GetReadOptions()
	if opts == nil {
		return nil, "", nil
	}
	if arrowOpts := opts.GetArrowSerializationOptions(); arrowOpts != nil &&
		arrowOpts.GetBufferCompression() != storagepb.ArrowSerializationOptions_COMPRESSION_UNSPECIFIED {
		return nil, "", status.Error(codes.Unimplemented, "arrow_serialization_options.buffer_compression is not supported yet; leave it unset (no compression)")
	}
	return opts.GetSelectedFields(), opts.GetRowRestriction(), nil
}

// configureAvroReadStream builds the Avro codec for stream's resolved rows
// and returns the ReadSession.schema oneof value for it.
func configureAvroReadStream(stream *storageReadStream, fields []tableField) (*storagepb.ReadSession_AvroSchema, error) {
	if err := rejectNestedFields("the Storage Read API (Avro)", fields); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	schemaJSON, err := buildAvroSchemaJSON(fields)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build avro schema: %v", err)
	}
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build avro codec: %v", err)
	}
	stream.codec = codec
	return &storagepb.ReadSession_AvroSchema{AvroSchema: &storagepb.AvroSchema{Schema: schemaJSON}}, nil
}

// configureArrowReadStream builds the real Arrow schema for stream's
// resolved rows and returns the ReadSession.schema oneof value for it. See
// arrowFieldType/buildArrowSchema for the exact bounded type mapping.
func configureArrowReadStream(stream *storageReadStream, fields []tableField) (*storagepb.ReadSession_ArrowSchema, error) {
	arrowSchema, err := buildArrowSchema(fields)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	schemaMsg, err := serializeArrowSchemaMessage(arrowSchema, memory.DefaultAllocator)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build arrow schema message: %v", err)
	}
	stream.arrowSchema = arrowSchema
	return &storagepb.ReadSession_ArrowSchema{ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: schemaMsg}}, nil
}

func (s *storageReadService) CreateReadSession(_ context.Context, req *storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
	rs := req.GetReadSession()
	if rs == nil {
		return nil, status.Error(codes.InvalidArgument, "read_session is required")
	}
	projectID, datasetID, tableID, err := parseStorageTableName(rs.GetTable())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if _, ok, _ := s.server.tables.get(projectID, datasetID, tableID); !ok {
		return nil, status.Errorf(codes.NotFound, "table not found: %s.%s", datasetID, tableID)
	}

	selectedFields, rowRestriction, err := parseStorageReadOptions(rs)
	if err != nil {
		return nil, err
	}

	queryText := buildStorageReadQuery(datasetID, tableID, selectedFields, rowRestriction)
	fields, rows, err := s.server.executeRealSQLQuery(projectID, queryText, nil)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve read session: %v", err)
	}

	dataFormat := rs.GetDataFormat()
	if dataFormat == storagepb.DataFormat_DATA_FORMAT_UNSPECIFIED {
		dataFormat = storagepb.DataFormat_AVRO // real BigQuery's own default
	}

	stream := &storageReadStream{fields: fields, rows: rows, dataFormat: dataFormat}
	var avroSchemaField *storagepb.ReadSession_AvroSchema
	var arrowSchemaField *storagepb.ReadSession_ArrowSchema
	switch dataFormat {
	case storagepb.DataFormat_AVRO:
		avroSchemaField, err = configureAvroReadStream(stream, fields)
	case storagepb.DataFormat_ARROW:
		arrowSchemaField, err = configureArrowReadStream(stream, fields)
	default:
		err = status.Errorf(codes.InvalidArgument, "unsupported data_format %s; use DATA_FORMAT_AVRO or DATA_FORMAT_ARROW", dataFormat)
	}
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.counter++
	sessionID := "session_" + strconv.FormatInt(s.counter, 10)
	s.counter++
	streamID := "stream_" + strconv.FormatInt(s.counter, 10)
	sessionName := fmt.Sprintf("projects/%s/locations/%s/sessions/%s", projectID, storageReadLocation, sessionID)
	streamName := fmt.Sprintf("%s/streams/%s", sessionName, streamID)
	s.streams[streamName] = stream
	s.mu.Unlock()

	resp := &storagepb.ReadSession{
		Name:              sessionName,
		DataFormat:        dataFormat,
		Table:             rs.GetTable(),
		Streams:           []*storagepb.ReadStream{{Name: streamName}},
		EstimatedRowCount: int64(len(rows)),
	}
	if avroSchemaField != nil {
		resp.Schema = avroSchemaField
	}
	if arrowSchemaField != nil {
		resp.Schema = arrowSchemaField
	}
	return resp, nil
}

func (s *storageReadService) ReadRows(req *storagepb.ReadRowsRequest, stream storagepb.BigQueryRead_ReadRowsServer) error {
	s.mu.Lock()
	st, ok := s.streams[req.GetReadStream()]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "read stream not found: %s", req.GetReadStream())
	}

	offset := req.GetOffset()
	if offset < 0 || offset > int64(len(st.rows)) {
		return status.Errorf(codes.InvalidArgument, "offset %d out of range for stream with %d rows", offset, len(st.rows))
	}

	remaining := st.rows[offset:]
	if st.dataFormat == storagepb.DataFormat_ARROW {
		return sendArrowReadRowsResponse(stream, st, remaining)
	}
	return sendAvroReadRowsResponse(stream, st, remaining)
}

// sendAvroReadRowsResponse encodes and sends all remaining rows as a single
// Avro response. All rows in a single response: this emulator is
// local-dev-sized, not BigQuery-scale (same convention already used for
// materializeNestedRows and Load/Extract), so the real 128 MiB-per-response
// limit is never exercised here.
func sendAvroReadRowsResponse(stream storagepb.BigQueryRead_ReadRowsServer, st *storageReadStream, rows [][]string) error {
	if len(rows) == 0 {
		return stream.Send(&storagepb.ReadRowsResponse{
			Rows:     &storagepb.ReadRowsResponse_AvroRows{AvroRows: &storagepb.AvroRows{}},
			RowCount: 0,
		})
	}

	var binary []byte
	for _, row := range rows {
		record := make(map[string]any, len(st.fields))
		for i, field := range st.fields {
			if i >= len(row) {
				record[field.Name] = nil
				continue
			}
			record[field.Name] = stringToAvroValue(row[i], field)
		}
		var err error
		binary, err = st.codec.BinaryFromNative(binary, record)
		if err != nil {
			return status.Errorf(codes.Internal, "encode avro row: %v", err)
		}
	}

	return stream.Send(&storagepb.ReadRowsResponse{
		Rows: &storagepb.ReadRowsResponse_AvroRows{AvroRows: &storagepb.AvroRows{
			SerializedBinaryRows: binary,
			RowCount:             int64(len(rows)),
		}},
		RowCount: int64(len(rows)),
		Schema:   &storagepb.ReadRowsResponse_AvroSchema{AvroSchema: &storagepb.AvroSchema{Schema: st.codec.Schema()}},
	})
}

// sendArrowReadRowsResponse mirrors sendAvroReadRowsResponse for
// DATA_FORMAT_ARROW: all remaining rows as one Arrow record batch message,
// with the schema echoed on every response the same way the Avro path
// already does.
func sendArrowReadRowsResponse(stream storagepb.BigQueryRead_ReadRowsServer, st *storageReadStream, rows [][]string) error {
	if len(rows) == 0 {
		return stream.Send(&storagepb.ReadRowsResponse{
			Rows:     &storagepb.ReadRowsResponse_ArrowRecordBatch{ArrowRecordBatch: &storagepb.ArrowRecordBatch{}},
			RowCount: 0,
		})
	}

	rec, err := buildArrowRecord(memory.DefaultAllocator, st.arrowSchema, st.fields, rows)
	if err != nil {
		return status.Errorf(codes.Internal, "encode arrow record batch: %v", err)
	}
	defer rec.Release()

	batchMsg, err := serializeArrowRecordBatchMessage(rec, memory.DefaultAllocator)
	if err != nil {
		return status.Errorf(codes.Internal, "serialize arrow record batch: %v", err)
	}
	schemaMsg, err := serializeArrowSchemaMessage(st.arrowSchema, memory.DefaultAllocator)
	if err != nil {
		return status.Errorf(codes.Internal, "serialize arrow schema: %v", err)
	}

	return stream.Send(&storagepb.ReadRowsResponse{
		Rows: &storagepb.ReadRowsResponse_ArrowRecordBatch{ArrowRecordBatch: &storagepb.ArrowRecordBatch{
			SerializedRecordBatch: batchMsg,
			RowCount:              int64(len(rows)),
		}},
		RowCount: int64(len(rows)),
		Schema:   &storagepb.ReadRowsResponse_ArrowSchema{ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: schemaMsg}},
	})
}

// SplitReadStream is explicitly unimplemented for this bounded first
// increment (see capabilities/registry.yaml `grpc.storage.read`) — a real,
// declared gap rather than a silent no-op: every session has exactly one
// stream, so there is nothing to split yet.
func (s *storageReadService) SplitReadStream(context.Context, *storagepb.SplitReadStreamRequest) (*storagepb.SplitReadStreamResponse, error) {
	return nil, status.Error(codes.Unimplemented, "SplitReadStream is not implemented; every CreateReadSession returns exactly one stream in this emulator")
}
