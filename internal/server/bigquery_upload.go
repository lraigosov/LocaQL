package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxUploadMetadataBytes = 10 << 20
	uploadSessionTTL       = time.Hour
)

var (
	uploadChunkRangePattern = regexp.MustCompile(`^bytes (\d+)-(\d+)/(\d+|\*)$`)
	uploadProbeRangePattern = regexp.MustCompile(`^bytes \*/(\d+|\*)$`)
)

type uploadedLoadMedia struct {
	Data []byte
	Name string
}

type uploadedLoadMediaContextKey struct{}

func uploadedLoadMediaFromRequest(r *http.Request) (uploadedLoadMedia, bool) {
	media, ok := r.Context().Value(uploadedLoadMediaContextKey{}).(uploadedLoadMedia)
	return media, ok
}

type bigQueryUploadSession struct {
	ProjectID string
	Metadata  []byte
	Data      []byte
	UpdatedAt time.Time

	Finalizing bool
	Completed  bool
	Status     int
	Header     http.Header
	Body       []byte
}

type bigQueryUploadService struct {
	mu       sync.Mutex
	sessions map[string]*bigQueryUploadSession
}

func newBigQueryUploadService() *bigQueryUploadService {
	return &bigQueryUploadService{sessions: map[string]*bigQueryUploadSession{}}
}

func (u *bigQueryUploadService) begin(projectID string, metadata []byte) (string, error) {
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", fmt.Errorf("create upload session id: %w", err)
	}
	id := hex.EncodeToString(idBytes)
	now := time.Now().UTC()

	u.mu.Lock()
	defer u.mu.Unlock()
	for existingID, session := range u.sessions {
		if now.Sub(session.UpdatedAt) > uploadSessionTTL {
			delete(u.sessions, existingID)
		}
	}
	u.sessions[id] = &bigQueryUploadSession{
		ProjectID: projectID,
		Metadata:  cloneBytes(metadata),
		UpdatedAt: now,
	}
	return id, nil
}

func (s *Server) bigQueryJobUpload(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/upload/bigquery/v2/projects/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "jobs" {
		writeError(w, http.StatusNotFound, "Not found", "notFound")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
		return
	}

	projectID := parts[0]
	switch r.URL.Query().Get("uploadType") {
	case "multipart":
		s.handleBigQueryMultipartUpload(w, r, projectID)
	case "resumable":
		s.initiateBigQueryResumableUpload(w, r, projectID)
	default:
		writeError(w, http.StatusBadRequest, "uploadType must be multipart or resumable", "invalid")
	}
}

func (s *Server) handleBigQueryMultipartUpload(w http.ResponseWriter, r *http.Request, projectID string) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "multipart/related") || params["boundary"] == "" {
		writeError(w, http.StatusBadRequest, "Content-Type must be multipart/related with a boundary", "invalid")
		return
	}

	reader := multipart.NewReader(r.Body, params["boundary"])
	metadataPart, err := reader.NextPart()
	if err != nil {
		writeError(w, http.StatusBadRequest, "multipart upload requires a JSON metadata part", "invalid")
		return
	}
	metadata, err := readUploadMetadata(metadataPart)
	_ = metadataPart.Close()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	if err := validateUploadedJobMetadata(metadata); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}

	mediaPart, err := reader.NextPart()
	if err != nil {
		writeError(w, http.StatusBadRequest, "multipart upload requires a media part", "invalid")
		return
	}
	data, err := io.ReadAll(mediaPart)
	_ = mediaPart.Close()
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read uploaded media", "invalid")
		return
	}
	if extraPart, extraErr := reader.NextPart(); extraErr != io.EOF {
		if extraPart != nil {
			_ = extraPart.Close()
		}
		writeError(w, http.StatusBadRequest, "multipart upload must contain exactly two parts", "invalid")
		return
	}

	captured := s.submitUploadedJob(r, projectID, metadata, data)
	writeUploadResponse(w, captured, true)
}

func (s *Server) initiateBigQueryResumableUpload(w http.ResponseWriter, r *http.Request, projectID string) {
	metadata, err := readUploadMetadata(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	if err := validateUploadedJobMetadata(metadata); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	id, err := s.uploads.begin(projectID, metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "internalError")
		return
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]); forwarded == "http" || forwarded == "https" {
		scheme = forwarded
	}
	location := (&url.URL{
		Scheme: scheme,
		Host:   r.Host,
		Path:   "/_emulator/bigquery/uploads/" + id,
	}).String()
	w.Header().Set("Location", location)
	w.Header().Set("X-GUploader-UploadID", id)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) bigQueryResumableUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
		return
	}
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/_emulator/bigquery/uploads/"), "/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "Upload session not found", "notFound")
		return
	}

	rangeSpec, err := parseUploadContentRange(r.Header.Get("Content-Range"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read upload chunk", "invalid")
		return
	}

	metadata, data, projectID, shouldFinalize, handled := s.acceptUploadChunk(w, id, rangeSpec, payload)
	if handled {
		return
	}
	if !shouldFinalize {
		writeError(w, http.StatusInternalServerError, "upload session entered an invalid state", "internalError")
		return
	}

	captured := s.submitUploadedJob(r, projectID, metadata, data)
	s.uploads.mu.Lock()
	if session := s.uploads.sessions[id]; session != nil {
		session.Finalizing = false
		session.Completed = true
		session.Status = captured.statusCode()
		session.Header = captured.header.Clone()
		session.Body = cloneBytes(captured.body.Bytes())
		// Completed-session retries only need the cached HTTP response; release
		// the metadata and media buffers immediately instead of retaining the
		// uploaded file until the session TTL expires.
		session.Metadata = nil
		session.Data = nil
		session.UpdatedAt = time.Now().UTC()
	}
	s.uploads.mu.Unlock()
	writeUploadResponse(w, captured, true)
}

type uploadContentRange struct {
	Probe      bool
	Start      int64
	End        int64
	Total      int64
	TotalKnown bool
}

func parseUploadContentRange(value string) (uploadContentRange, error) {
	value = strings.TrimSpace(value)
	if match := uploadChunkRangePattern.FindStringSubmatch(value); match != nil {
		start, _ := strconv.ParseInt(match[1], 10, 64)
		end, _ := strconv.ParseInt(match[2], 10, 64)
		if end < start {
			return uploadContentRange{}, fmt.Errorf("invalid Content-Range %q", value)
		}
		out := uploadContentRange{Start: start, End: end}
		if match[3] != "*" {
			out.Total, _ = strconv.ParseInt(match[3], 10, 64)
			out.TotalKnown = true
			if out.Total <= end {
				return uploadContentRange{}, fmt.Errorf("Content-Range total must be greater than its end offset")
			}
		}
		return out, nil
	}
	if match := uploadProbeRangePattern.FindStringSubmatch(value); match != nil {
		out := uploadContentRange{Probe: true}
		if match[1] != "*" {
			out.Total, _ = strconv.ParseInt(match[1], 10, 64)
			out.TotalKnown = true
		}
		return out, nil
	}
	return uploadContentRange{}, fmt.Errorf("Content-Range must use 'bytes start-end/total', 'bytes start-end/*', or 'bytes */*'")
}

func (s *Server) acceptUploadChunk(w http.ResponseWriter, id string, spec uploadContentRange, payload []byte) ([]byte, []byte, string, bool, bool) {
	s.uploads.mu.Lock()
	defer s.uploads.mu.Unlock()
	session := s.uploads.sessions[id]
	if session == nil || time.Since(session.UpdatedAt) > uploadSessionTTL {
		delete(s.uploads.sessions, id)
		writeError(w, http.StatusNotFound, "Upload session not found or expired", "notFound")
		return nil, nil, "", false, true
	}
	if session.Completed {
		writeStoredUploadResponse(w, session)
		return nil, nil, "", false, true
	}
	if session.Finalizing {
		writeError(w, http.StatusConflict, "Upload session is being finalized", "conflict")
		return nil, nil, "", false, true
	}
	session.UpdatedAt = time.Now().UTC()

	if spec.Probe {
		if spec.TotalKnown && spec.Total == 0 && len(session.Data) == 0 && len(payload) == 0 {
			session.Finalizing = true
			return cloneBytes(session.Metadata), []byte{}, session.ProjectID, true, false
		}
		writeResumeIncomplete(w, int64(len(session.Data)))
		return nil, nil, "", false, true
	}
	if int64(len(payload)) != spec.End-spec.Start+1 {
		writeError(w, http.StatusBadRequest, "upload chunk length does not match Content-Range", "invalid")
		return nil, nil, "", false, true
	}

	current := int64(len(session.Data))
	if spec.Start < current {
		if spec.End < current && bytes.Equal(session.Data[spec.Start:spec.End+1], payload) {
			writeResumeIncomplete(w, current)
			return nil, nil, "", false, true
		}
		writeError(w, http.StatusConflict, fmt.Sprintf("upload offset %d conflicts with committed offset %d", spec.Start, current), "conflict")
		return nil, nil, "", false, true
	}
	if spec.Start > current {
		writeError(w, http.StatusConflict, fmt.Sprintf("upload offset %d does not match committed offset %d", spec.Start, current), "conflict")
		return nil, nil, "", false, true
	}
	session.Data = append(session.Data, payload...)
	current = int64(len(session.Data))
	if !spec.TotalKnown || current < spec.Total {
		writeResumeIncomplete(w, current)
		return nil, nil, "", false, true
	}
	if current != spec.Total {
		writeError(w, http.StatusBadRequest, "uploaded bytes exceed Content-Range total", "invalid")
		return nil, nil, "", false, true
	}
	session.Finalizing = true
	return cloneBytes(session.Metadata), cloneBytes(session.Data), session.ProjectID, true, false
}

func writeResumeIncomplete(w http.ResponseWriter, received int64) {
	if received > 0 {
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", received-1))
	}
	w.WriteHeader(http.StatusPermanentRedirect)
}

func readUploadMetadata(reader io.Reader) ([]byte, error) {
	limited := io.LimitReader(reader, maxUploadMetadataBytes+1)
	metadata, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("failed to read upload metadata")
	}
	if len(metadata) > maxUploadMetadataBytes {
		return nil, fmt.Errorf("upload metadata exceeds %d bytes", maxUploadMetadataBytes)
	}
	return metadata, nil
}

func validateUploadedJobMetadata(metadata []byte) error {
	var payload map[string]any
	if err := json.Unmarshal(metadata, &payload); err != nil {
		return fmt.Errorf("upload metadata must be valid JSON: %w", err)
	}
	configuration, ok := payload["configuration"].(map[string]any)
	if !ok {
		return fmt.Errorf("upload metadata requires configuration.load")
	}
	if _, ok := configuration["load"].(map[string]any); !ok {
		return fmt.Errorf("upload metadata requires configuration.load")
	}
	return nil
}

type uploadResponseCapture struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newUploadResponseCapture() *uploadResponseCapture {
	return &uploadResponseCapture{header: make(http.Header)}
}

func (c *uploadResponseCapture) Header() http.Header { return c.header }

func (c *uploadResponseCapture) WriteHeader(status int) {
	if c.status == 0 {
		c.status = status
	}
}

func (c *uploadResponseCapture) Write(data []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	return c.body.Write(data)
}

func (c *uploadResponseCapture) statusCode() int {
	if c.status == 0 {
		return http.StatusOK
	}
	return c.status
}

func (s *Server) submitUploadedJob(r *http.Request, projectID string, metadata, data []byte) *uploadResponseCapture {
	ctx := context.WithValue(r.Context(), uploadedLoadMediaContextKey{}, uploadedLoadMedia{
		Data: cloneBytes(data),
		Name: "uploaded-file",
	})
	request := r.Clone(ctx)
	request.Body = io.NopCloser(bytes.NewReader(metadata))
	request.ContentLength = int64(len(metadata))
	request.Header = r.Header.Clone()
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

	captured := newUploadResponseCapture()
	s.insertJob(captured, request, projectID)
	return captured
}

func writeUploadResponse(w http.ResponseWriter, captured *uploadResponseCapture, forceOK bool) {
	for key, values := range captured.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status := captured.statusCode()
	if forceOK && status >= http.StatusOK && status < http.StatusMultipleChoices {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(captured.body.Bytes())
}

func writeStoredUploadResponse(w http.ResponseWriter, session *bigQueryUploadSession) {
	for key, values := range session.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status := session.Status
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(session.Body)
}
