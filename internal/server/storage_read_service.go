package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
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
	fields []tableField
	rows   [][]string
	codec  *goavro.Codec
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

	var selectedFields []string
	var rowRestriction string
	if opts := rs.GetReadOptions(); opts != nil {
		selectedFields = opts.GetSelectedFields()
		rowRestriction = opts.GetRowRestriction()
		if opts.GetArrowSerializationOptions() != nil {
			return nil, status.Error(codes.Unimplemented, "Arrow framing is not supported yet; this emulator only supports Avro (DATA_FORMAT_AVRO) — leave output_format_serialization_options unset or set avro_serialization_options")
		}
	}

	queryText := buildStorageReadQuery(datasetID, tableID, selectedFields, rowRestriction)
	fields, rows, err := s.server.executeRealSQLQuery(projectID, queryText, nil)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve read session: %v", err)
	}
	if err := rejectNestedFields("the Storage Read API", fields); err != nil {
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

	s.mu.Lock()
	s.counter++
	sessionID := "session_" + strconv.FormatInt(s.counter, 10)
	s.counter++
	streamID := "stream_" + strconv.FormatInt(s.counter, 10)
	sessionName := fmt.Sprintf("projects/%s/locations/%s/sessions/%s", projectID, storageReadLocation, sessionID)
	streamName := fmt.Sprintf("%s/streams/%s", sessionName, streamID)
	s.streams[streamName] = &storageReadStream{fields: fields, rows: rows, codec: codec}
	s.mu.Unlock()

	return &storagepb.ReadSession{
		Name:              sessionName,
		DataFormat:        storagepb.DataFormat_AVRO,
		Schema:            &storagepb.ReadSession_AvroSchema{AvroSchema: &storagepb.AvroSchema{Schema: schemaJSON}},
		Table:             rs.GetTable(),
		Streams:           []*storagepb.ReadStream{{Name: streamName}},
		EstimatedRowCount: int64(len(rows)),
	}, nil
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
	if len(remaining) == 0 {
		return stream.Send(&storagepb.ReadRowsResponse{
			Rows:     &storagepb.ReadRowsResponse_AvroRows{AvroRows: &storagepb.AvroRows{}},
			RowCount: 0,
		})
	}

	// All rows in a single response: this emulator is local-dev-sized, not
	// BigQuery-scale (same convention already used for materializeNestedRows
	// and Load/Extract), so the real 128 MiB-per-response limit is never
	// exercised here.
	var binary []byte
	for _, row := range remaining {
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
			RowCount:             int64(len(remaining)),
		}},
		RowCount: int64(len(remaining)),
		Schema:   &storagepb.ReadRowsResponse_AvroSchema{AvroSchema: &storagepb.AvroSchema{Schema: st.codec.Schema()}},
	})
}

// SplitReadStream is explicitly unimplemented for this bounded first
// increment (see capabilities/registry.yaml `grpc.storage.read`) — a real,
// declared gap rather than a silent no-op: every session has exactly one
// stream, so there is nothing to split yet.
func (s *storageReadService) SplitReadStream(context.Context, *storagepb.SplitReadStreamRequest) (*storagepb.SplitReadStreamResponse, error) {
	return nil, status.Error(codes.Unimplemented, "SplitReadStream is not implemented; every CreateReadSession returns exactly one stream in this emulator")
}
