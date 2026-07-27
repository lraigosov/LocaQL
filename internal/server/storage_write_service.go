package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// storageWriteDefaultStreamID is the well-known stream id every table has
// implicitly (real BigQuery: "This stream doesn't need to be created using
// CreateWriteStream... can be used simultaneously by any number of
// clients"). Appends to it are always COMMITTED and never carry an offset.
const storageWriteDefaultStreamID = "_default"

type writeStreamKind int

const (
	writeStreamCommitted writeStreamKind = iota
	writeStreamPending
)

// storageWriteStream is one CreateWriteStream's server-side state. Rows for
// a COMMITTED stream are appended straight into the real catalog (via the
// same upsertCopyDestination helper copy jobs already use, WRITE_APPEND +
// CREATE_NEVER — the destination table must already exist, matching real
// BigQuery's contract that Storage Write never creates a table). Rows for a
// PENDING stream are buffered here until BatchCommitWriteStreams applies
// every finalized stream's buffer to the catalog atomically in one call.
type storageWriteStream struct {
	mu         sync.Mutex
	Name       string
	ProjectID  string
	DatasetID  string
	TableID    string
	Kind       writeStreamKind
	Fields     []tableField
	CreatedAt  time.Time
	CommitTime time.Time
	Finalized  bool
	Committed  bool
	Buffered   [][]string
	NextOffset int64 // rows accepted so far — committed count for COMMITTED, buffered count for PENDING; drives real offset/exactly-once checks
}

func (st *storageWriteStream) toProto() *storagepb.WriteStream {
	st.mu.Lock()
	defer st.mu.Unlock()
	typ := storagepb.WriteStream_COMMITTED
	if st.Kind == writeStreamPending {
		typ = storagepb.WriteStream_PENDING
	}
	ws := &storagepb.WriteStream{
		Name:        st.Name,
		Type:        typ,
		CreateTime:  timestamppb.New(st.CreatedAt),
		TableSchema: tableFieldsToStorageSchema(st.Fields),
	}
	if !st.CommitTime.IsZero() {
		ws.CommitTime = timestamppb.New(st.CommitTime)
	}
	return ws
}

// storageWriteService implements storagepb.BigQueryWriteServer: the second
// (larger) increment of BigQuery Storage API emulation, covering the
// `_default` stream plus explicit COMMITTED/PENDING streams with real
// offset/exactly-once checks and an atomic BatchCommitWriteStreams. BUFFERED
// streams and FlushRows are explicitly unimplemented (see
// capabilities/registry.yaml `grpc.storage.write`).
type storageWriteService struct {
	storagepb.UnimplementedBigQueryWriteServer
	server *Server

	mu      sync.Mutex
	counter int64
	streams map[string]*storageWriteStream // key: stream resource name
}

func newStorageWriteService(s *Server) *storageWriteService {
	return &storageWriteService{server: s, streams: make(map[string]*storageWriteStream)}
}

// parseStorageWriteStreamName parses
// projects/{p}/datasets/{d}/tables/{t}/streams/{stream_id} and reports
// whether stream_id is the well-known "_default" stream.
func parseStorageWriteStreamName(name string) (projectID, datasetID, tableID, streamID string, isDefault bool, err error) {
	parts := strings.Split(strings.Trim(strings.TrimSpace(name), "/"), "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "datasets" || parts[4] != "tables" || parts[6] != "streams" {
		return "", "", "", "", false, fmt.Errorf("invalid write stream name %q: expected projects/{project}/datasets/{dataset}/tables/{table}/streams/{stream}", name)
	}
	streamID = parts[7]
	return parts[1], parts[3], parts[5], streamID, streamID == storageWriteDefaultStreamID, nil
}

func tableFieldsToStorageSchema(fields []tableField) *storagepb.TableSchema {
	out := make([]*storagepb.TableFieldSchema, len(fields))
	for i, f := range fields {
		out[i] = &storagepb.TableFieldSchema{
			Name: f.Name,
			Type: storageFieldTypeFor(f.Type),
			Mode: storageFieldModeFor(f.Mode),
		}
	}
	return &storagepb.TableSchema{Fields: out}
}

func storageFieldTypeFor(t string) storagepb.TableFieldSchema_Type {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "INT64", "INTEGER":
		return storagepb.TableFieldSchema_INT64
	case "FLOAT64", "FLOAT":
		return storagepb.TableFieldSchema_DOUBLE
	case "BOOL", "BOOLEAN":
		return storagepb.TableFieldSchema_BOOL
	case "BYTES":
		return storagepb.TableFieldSchema_BYTES
	case "DATE":
		return storagepb.TableFieldSchema_DATE
	case "DATETIME":
		return storagepb.TableFieldSchema_DATETIME
	case "TIME":
		return storagepb.TableFieldSchema_TIME
	case "TIMESTAMP":
		return storagepb.TableFieldSchema_TIMESTAMP
	case "NUMERIC":
		return storagepb.TableFieldSchema_NUMERIC
	case "BIGNUMERIC":
		return storagepb.TableFieldSchema_BIGNUMERIC
	case "RECORD", "STRUCT":
		return storagepb.TableFieldSchema_STRUCT
	default:
		return storagepb.TableFieldSchema_STRING
	}
}

func storageFieldModeFor(mode string) storagepb.TableFieldSchema_Mode {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "REQUIRED":
		return storagepb.TableFieldSchema_REQUIRED
	case "REPEATED":
		return storagepb.TableFieldSchema_REPEATED
	default:
		return storagepb.TableFieldSchema_NULLABLE
	}
}

func (s *storageWriteService) CreateWriteStream(_ context.Context, req *storagepb.CreateWriteStreamRequest) (*storagepb.WriteStream, error) {
	projectID, datasetID, tableID, err := parseStorageTableName(req.GetParent())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rec, ok, _ := s.server.tables.get(projectID, datasetID, tableID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table not found: %s.%s", datasetID, tableID)
	}
	if err := rejectNestedFields("the Storage Write API", rec.Schema); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var kind writeStreamKind
	switch req.GetWriteStream().GetType() {
	case storagepb.WriteStream_COMMITTED:
		kind = writeStreamCommitted
	case storagepb.WriteStream_PENDING:
		kind = writeStreamPending
	case storagepb.WriteStream_BUFFERED:
		return nil, status.Error(codes.Unimplemented, "BUFFERED write streams are not supported; use COMMITTED or PENDING")
	default:
		return nil, status.Error(codes.InvalidArgument, "write_stream.type is required (COMMITTED or PENDING)")
	}

	s.mu.Lock()
	s.counter++
	streamID := "stream_" + strconv.FormatInt(s.counter, 10)
	name := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/%s", projectID, datasetID, tableID, streamID)
	st := &storageWriteStream{
		Name: name, ProjectID: projectID, DatasetID: datasetID, TableID: tableID,
		Kind: kind, Fields: cloneTableFields(rec.Schema), CreatedAt: time.Now().UTC(),
	}
	s.streams[name] = st
	s.mu.Unlock()

	return st.toProto(), nil
}

func (s *storageWriteService) GetWriteStream(_ context.Context, req *storagepb.GetWriteStreamRequest) (*storagepb.WriteStream, error) {
	s.mu.Lock()
	st, ok := s.streams[req.GetName()]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "write stream not found: %s", req.GetName())
	}
	return st.toProto(), nil
}

func (s *storageWriteService) FinalizeWriteStream(_ context.Context, req *storagepb.FinalizeWriteStreamRequest) (*storagepb.FinalizeWriteStreamResponse, error) {
	s.mu.Lock()
	st, ok := s.streams[req.GetName()]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "write stream not found: %s", req.GetName())
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.Finalized = true
	return &storagepb.FinalizeWriteStreamResponse{RowCount: st.NextOffset}, nil
}

// BatchCommitWriteStreams applies every named PENDING stream's buffered rows
// to the real catalog in a single upsertCopyDestination call — genuinely
// atomic (all rows from all streams land together under one table lock, or
// none do if any stream fails validation first).
func (s *storageWriteService) BatchCommitWriteStreams(_ context.Context, req *storagepb.BatchCommitWriteStreamsRequest) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	projectID, datasetID, tableID, err := parseStorageTableName(req.GetParent())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	s.mu.Lock()
	streams := make([]*storageWriteStream, 0, len(req.GetWriteStreams()))
	for _, name := range req.GetWriteStreams() {
		st, ok := s.streams[name]
		if !ok {
			s.mu.Unlock()
			return nil, status.Errorf(codes.NotFound, "write stream not found: %s", name)
		}
		streams = append(streams, st)
	}
	s.mu.Unlock()

	var streamErrors []*storagepb.StorageError
	for _, st := range streams {
		st.mu.Lock()
		switch {
		case st.Kind != writeStreamPending:
			streamErrors = append(streamErrors, &storagepb.StorageError{Entity: st.Name, ErrorMessage: "only PENDING streams can be committed"})
		case !st.Finalized:
			streamErrors = append(streamErrors, &storagepb.StorageError{Entity: st.Name, ErrorMessage: "stream must be finalized before commit"})
		case st.Committed:
			streamErrors = append(streamErrors, &storagepb.StorageError{Entity: st.Name, ErrorMessage: "stream already committed"})
		case st.ProjectID != projectID || st.DatasetID != datasetID || st.TableID != tableID:
			streamErrors = append(streamErrors, &storagepb.StorageError{Entity: st.Name, ErrorMessage: "stream does not belong to parent table"})
		}
		st.mu.Unlock()
	}
	if len(streamErrors) > 0 {
		return &storagepb.BatchCommitWriteStreamsResponse{StreamErrors: streamErrors}, nil
	}

	dest := tableReference{ProjectID: projectID, DatasetID: datasetID, TableID: tableID}
	var allRows [][]string
	var fields []tableField
	for _, st := range streams {
		st.mu.Lock()
		allRows = append(allRows, st.Buffered...)
		if fields == nil {
			fields = st.Fields
		}
		st.mu.Unlock()
	}
	if len(allRows) > 0 {
		if _, err := s.server.tables.upsertCopyDestination(dest, fields, allRows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	now := time.Now().UTC()
	for _, st := range streams {
		st.mu.Lock()
		st.Committed = true
		st.CommitTime = now
		st.Buffered = nil
		st.mu.Unlock()
	}
	return &storagepb.BatchCommitWriteStreamsResponse{CommitTime: timestamppb.New(now)}, nil
}

// FlushRows is explicitly unimplemented: it only applies to BUFFERED
// streams, and CreateWriteStream already rejects BUFFERED explicitly, so
// there is never a valid target for it in this emulator.
func (s *storageWriteService) FlushRows(context.Context, *storagepb.FlushRowsRequest) (*storagepb.FlushRowsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "FlushRows is not implemented; BUFFERED write streams are not supported (use COMMITTED or PENDING)")
}

// buildDynamicMessageDescriptor wraps a bare DescriptorProto (what
// ProtoSchema.proto_descriptor actually carries) in a synthetic,
// self-contained FileDescriptorProto so it can be resolved into a real
// protoreflect.MessageDescriptor — the real API's own contract requires the
// descriptor to be self-contained (no external imports), so an empty
// resolver (protoregistry.GlobalFiles) is sufficient here.
func buildDynamicMessageDescriptor(desc *descriptorpb.DescriptorProto) (protoreflect.MessageDescriptor, error) {
	if desc == nil {
		return nil, fmt.Errorf("writer_schema.proto_descriptor is required")
	}
	fdProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("locaql_storage_write_dynamic.proto"),
		Package:     proto.String("locaql.storagewrite"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{desc},
	}
	fd, err := protodesc.NewFile(fdProto, protoregistry.GlobalFiles)
	if err != nil {
		return nil, fmt.Errorf("build dynamic proto descriptor: %w", err)
	}
	return fd.Messages().Get(0), nil
}

// protoMessageToRow converts one decoded dynamic protobuf row into this
// project's stored string-cell row, matched against the destination table's
// columns by field name (not position — the wire schema a client sends and
// this project's column order need not match). A proto field that is
// repeated or a nested message is rejected explicitly rather than silently
// dropped or corrupted, matching the RECORD/REPEATED scope boundary already
// used by rejectNestedFields elsewhere.
func protoMessageToRow(msg *dynamicpb.Message, fields []tableField) ([]string, error) {
	md := msg.Descriptor()
	row := make([]string, len(fields))
	for i, f := range fields {
		fd := md.Fields().ByName(protoreflect.Name(f.Name))
		if fd == nil || !msg.Has(fd) {
			row[i] = ""
			continue
		}
		if fd.Cardinality() == protoreflect.Repeated {
			return nil, fmt.Errorf("column %s: repeated proto fields are not supported yet", f.Name)
		}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			return nil, fmt.Errorf("column %s: nested message proto fields are not supported yet", f.Name)
		}
		row[i] = protoScalarValueToString(fd.Kind(), msg.Get(fd))
	}
	return row, nil
}

func protoScalarValueToString(kind protoreflect.Kind, v protoreflect.Value) string {
	switch kind {
	case protoreflect.BoolKind:
		return strconv.FormatBool(v.Bool())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10)
	case protoreflect.FloatKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32)
	case protoreflect.DoubleKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case protoreflect.EnumKind:
		return strconv.FormatInt(int64(v.Enum()), 10)
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}

// AppendRows is the bidi-streaming heart of the Write API: each request may
// switch destination stream, (re)declare its writer_schema, and carry a
// batch of protobuf-serialized rows. Rows for the `_default` stream are
// applied to the real catalog immediately (COMMITTED semantics); rows for an
// explicit PENDING stream are buffered until BatchCommitWriteStreams; rows
// for an explicit COMMITTED stream are also applied immediately, but unlike
// `_default` they support real offset-based exactly-once semantics.
func (s *storageWriteService) AppendRows(stream storagepb.BigQueryWrite_AppendRowsServer) error {
	var (
		currentStreamName string
		isDefault         bool
		explicitStream    *storageWriteStream
		fields            []tableField
		msgDesc           protoreflect.MessageDescriptor
	)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if ws := req.GetWriteStream(); ws != "" && ws != currentStreamName {
			projectID, datasetID, tableID, _, isDef, parseErr := parseStorageWriteStreamName(ws)
			if parseErr != nil {
				return status.Error(codes.InvalidArgument, parseErr.Error())
			}
			isDefault = isDef
			if isDefault {
				rec, ok, _ := s.server.tables.get(projectID, datasetID, tableID)
				if !ok {
					return status.Errorf(codes.NotFound, "table not found: %s.%s", datasetID, tableID)
				}
				if err := rejectNestedFields("the Storage Write API", rec.Schema); err != nil {
					return status.Error(codes.InvalidArgument, err.Error())
				}
				fields = cloneTableFields(rec.Schema)
				explicitStream = nil
			} else {
				s.mu.Lock()
				st, ok := s.streams[ws]
				s.mu.Unlock()
				if !ok {
					return status.Errorf(codes.NotFound, "write stream not found: %s", ws)
				}
				explicitStream = st
				fields = st.Fields
			}
			currentStreamName = ws
			msgDesc = nil // a destination switch requires a fresh writer_schema, per the real API's own contract
		}
		if currentStreamName == "" {
			return status.Error(codes.InvalidArgument, "write_stream is required on the first AppendRows request")
		}

		protoData := req.GetProtoRows()
		if protoData == nil {
			if req.GetArrowRows() != nil {
				return status.Error(codes.Unimplemented, "arrow_rows is not supported for AppendRows; only proto_rows")
			}
			return status.Error(codes.InvalidArgument, "proto_rows is required")
		}
		if schema := protoData.GetWriterSchema(); schema != nil {
			md, buildErr := buildDynamicMessageDescriptor(schema.GetProtoDescriptor())
			if buildErr != nil {
				return status.Errorf(codes.InvalidArgument, "invalid writer_schema: %v", buildErr)
			}
			msgDesc = md
		}
		if msgDesc == nil {
			return status.Error(codes.InvalidArgument, "writer_schema must be specified before the first row is sent for a destination")
		}

		serializedRows := protoData.GetRows().GetSerializedRows()
		rows := make([][]string, 0, len(serializedRows))
		var rowErrors []*storagepb.RowError
		for i, raw := range serializedRows {
			msg := dynamicpb.NewMessage(msgDesc)
			if unmarshalErr := proto.Unmarshal(raw, msg); unmarshalErr != nil {
				rowErrors = append(rowErrors, &storagepb.RowError{Index: int64(i), Message: unmarshalErr.Error()})
				continue
			}
			row, convErr := protoMessageToRow(msg, fields)
			if convErr != nil {
				rowErrors = append(rowErrors, &storagepb.RowError{Index: int64(i), Message: convErr.Error()})
				continue
			}
			rows = append(rows, row)
		}
		if len(rowErrors) > 0 {
			if sendErr := stream.Send(&storagepb.AppendRowsResponse{RowErrors: rowErrors, WriteStream: currentStreamName}); sendErr != nil {
				return sendErr
			}
			continue
		}

		if isDefault {
			if req.GetOffset() != nil {
				return status.Error(codes.InvalidArgument, "offset is not allowed when appending to the _default stream")
			}
			projectID, datasetID, tableID, _, _, _ := parseStorageWriteStreamName(currentStreamName)
			dest := tableReference{ProjectID: projectID, DatasetID: datasetID, TableID: tableID}
			if _, upsertErr := s.server.tables.upsertCopyDestination(dest, fields, rows, "CREATE_NEVER", "WRITE_APPEND"); upsertErr != nil {
				return status.Error(codes.InvalidArgument, upsertErr.Error())
			}
			if sendErr := stream.Send(&storagepb.AppendRowsResponse{
				Response:    &storagepb.AppendRowsResponse_AppendResult_{AppendResult: &storagepb.AppendRowsResponse_AppendResult{}},
				WriteStream: currentStreamName,
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		explicitStream.mu.Lock()
		if explicitStream.Finalized {
			explicitStream.mu.Unlock()
			return status.Error(codes.FailedPrecondition, "cannot append to a finalized write stream")
		}
		if off := req.GetOffset(); off != nil {
			want := off.GetValue()
			switch {
			case want < explicitStream.NextOffset:
				explicitStream.mu.Unlock()
				errStatus := status.New(codes.AlreadyExists, fmt.Sprintf("offset %d already written; current end of stream is %d", want, explicitStream.NextOffset))
				if sendErr := stream.Send(&storagepb.AppendRowsResponse{Response: &storagepb.AppendRowsResponse_Error{Error: errStatus.Proto()}, WriteStream: currentStreamName}); sendErr != nil {
					return sendErr
				}
				continue
			case want > explicitStream.NextOffset:
				explicitStream.mu.Unlock()
				errStatus := status.New(codes.OutOfRange, fmt.Sprintf("offset %d is beyond the current end of stream %d", want, explicitStream.NextOffset))
				if sendErr := stream.Send(&storagepb.AppendRowsResponse{Response: &storagepb.AppendRowsResponse_Error{Error: errStatus.Proto()}, WriteStream: currentStreamName}); sendErr != nil {
					return sendErr
				}
				continue
			}
		}

		startOffset := explicitStream.NextOffset
		if explicitStream.Kind == writeStreamCommitted {
			dest := tableReference{ProjectID: explicitStream.ProjectID, DatasetID: explicitStream.DatasetID, TableID: explicitStream.TableID}
			if _, upsertErr := s.server.tables.upsertCopyDestination(dest, fields, rows, "CREATE_NEVER", "WRITE_APPEND"); upsertErr != nil {
				explicitStream.mu.Unlock()
				return status.Error(codes.Internal, upsertErr.Error())
			}
		} else {
			explicitStream.Buffered = append(explicitStream.Buffered, rows...)
		}
		explicitStream.NextOffset += int64(len(rows))
		explicitStream.mu.Unlock()

		if sendErr := stream.Send(&storagepb.AppendRowsResponse{
			Response: &storagepb.AppendRowsResponse_AppendResult_{AppendResult: &storagepb.AppendRowsResponse_AppendResult{
				Offset: wrapperspb.Int64(startOffset),
			}},
			WriteStream: currentStreamName,
		}); sendErr != nil {
			return sendErr
		}
	}
}
