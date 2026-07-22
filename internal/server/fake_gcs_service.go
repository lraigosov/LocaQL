package server

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file implements a minimal, real-contract-compatible subset of the
// Google Cloud Storage JSON API: buckets.insert/list, objects.insert (the
// "simple"/media upload variant only), objects.get, objects.list,
// objects.delete, and media (content) download. It exists so a user's own
// code that already uses the official Cloud Storage client library (Go,
// Python, Java, ...) can point STORAGE_EMULATOR_HOST at this same emulator
// process instead of real GCS, matching the project's core "same code,
// different endpoint" contract — the same reason the BigQuery REST surface
// exists at all.
//
// Explicitly NOT implemented, matching real fields/paths rather than
// inventing partial ones: resumable and multipart uploads (only
// uploadType=media), IAM/ACLs, object versioning/generations beyond a
// placeholder, lifecycle rules, notifications, signed URLs. A request for
// any of those either 404s (unregistered path) or is silently ignored where
// the field is accepted but not enforced (documented per-field below).
//
// Storage backend: local disk under LOCAQL_FAKE_GCS_ROOT, using the exact
// same root and bucket/object path-join convention as resolveFakeGCSPath (in
// bigquery.go), so a file reachable via a gs:// load/extract URI is also
// reachable through this HTTP API, and vice versa.

// fakeGCSRoot resolves LOCAQL_FAKE_GCS_ROOT, matching resolveFakeGCSPath's
// error wording style for the load/extract path (kept as a separate,
// intentionally duplicated implementation rather than refactoring
// resolveFakeGCSPath, since that function's existing tests assert on its
// exact error text referencing the gs:// URI).
const gcsMethodNotAllowed = "Method not allowed"

func fakeGCSRoot() (string, error) {
	root := strings.TrimSpace(os.Getenv("LOCAQL_FAKE_GCS_ROOT"))
	if root == "" {
		return "", fmt.Errorf("LOCAQL_FAKE_GCS_ROOT is not set; the fake GCS JSON API has no local directory to store bucket/object data in")
	}
	return filepath.Clean(root), nil
}

// fakeGCSObjectPath resolves bucket/object onto local disk under
// LOCAQL_FAKE_GCS_ROOT, rejecting any path that would escape the root.
func fakeGCSObjectPath(root, bucket, object string) (string, error) {
	joined := filepath.Join(root, bucket, object)
	if joined != root && !strings.HasPrefix(joined, root+string(filepath.Separator)) {
		return "", fmt.Errorf("object %q in bucket %q resolves outside LOCAQL_FAKE_GCS_ROOT", object, bucket)
	}
	return joined, nil
}

func writeGCSError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
			"errors":  []map[string]any{{"message": message, "domain": "global", "reason": strings.ToLower(http.StatusText(status))}},
		},
	})
}

func renderGCSBucketResource(name string) map[string]any {
	return map[string]any{
		"kind":     "storage#bucket",
		"id":       name,
		"name":     name,
		"selfLink": "/storage/v1/b/" + name,
	}
}

func renderGCSObjectResource(bucket, object string, info os.FileInfo, md5sum string) map[string]any {
	modTime := info.ModTime().UTC().Format(time.RFC3339)
	return map[string]any{
		"kind":        "storage#object",
		"id":          fmt.Sprintf("%s/%s", bucket, object),
		"name":        object,
		"bucket":      bucket,
		"size":        strconv.FormatInt(info.Size(), 10),
		"contentType": "application/octet-stream",
		"md5Hash":     md5sum,
		"etag":        fmt.Sprintf("%q", md5sum),
		"timeCreated": modTime,
		"updated":     modTime,
		"selfLink":    fmt.Sprintf("/storage/v1/b/%s/o/%s", bucket, object),
		"mediaLink":   fmt.Sprintf("/storage/v1/b/%s/o/%s?alt=media", bucket, object),
	}
}

func fileMD5Base64(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

// gcsBucketsCollection handles GET (buckets.list) and POST (buckets.insert)
// at exactly /storage/v1/b. Buckets are just top-level directories under
// LOCAQL_FAKE_GCS_ROOT — there is no separate bucket metadata store.
func (s *Server) gcsBucketsCollection(w http.ResponseWriter, r *http.Request) {
	root, err := fakeGCSRoot()
	if err != nil {
		writeGCSError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		entries, err := os.ReadDir(root)
		if err != nil && !os.IsNotExist(err) {
			writeGCSError(w, http.StatusInternalServerError, err.Error())
			return
		}
		items := make([]map[string]any, 0, len(entries))
		for _, e := range entries {
			if e.IsDir() {
				items = append(items, renderGCSBucketResource(e.Name()))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"kind": "storage#buckets", "items": items})
	case http.MethodPost:
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeGCSError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			writeGCSError(w, http.StatusBadRequest, "name is required")
			return
		}
		path, err := fakeGCSObjectPath(root, name, "")
		if err != nil {
			writeGCSError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := os.MkdirAll(path, 0o755); err != nil {
			writeGCSError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, renderGCSBucketResource(name))
	default:
		writeGCSError(w, http.StatusMethodNotAllowed, gcsMethodNotAllowed)
	}
}

// gcsBucketScope dispatches everything under /storage/v1/b/{bucket}... :
// bucket metadata (GET /storage/v1/b/{bucket}), object list
// (GET /storage/v1/b/{bucket}/o), and object get/download/delete
// (GET|DELETE /storage/v1/b/{bucket}/o/{object}, object may itself contain
// '/'). Object names are taken verbatim from the remainder of the path
// after "/o/" rather than re-splitting on every slash, since a real GCS
// object name commonly contains '/' as a pseudo-directory separator.
func (s *Server) gcsBucketScope(w http.ResponseWriter, r *http.Request) {
	root, err := fakeGCSRoot()
	if err != nil {
		writeGCSError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/storage/v1/b/")
	bucket, tail, hasTail := strings.Cut(rest, "/")
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		writeGCSError(w, http.StatusBadRequest, "bucket is required")
		return
	}

	if !hasTail {
		s.gcsGetBucket(w, r, root, bucket)
		return
	}

	if tail == "o" {
		s.gcsListObjects(w, r, root, bucket)
		return
	}

	object := strings.TrimPrefix(tail, "o/")
	if object == tail || object == "" {
		writeGCSError(w, http.StatusNotFound, "unrecognized storage path")
		return
	}

	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("alt") == "media" {
			s.gcsDownloadObject(w, root, bucket, object)
			return
		}
		s.gcsGetObjectMetadata(w, root, bucket, object)
	case http.MethodDelete:
		s.gcsDeleteObject(w, root, bucket, object)
	default:
		writeGCSError(w, http.StatusMethodNotAllowed, gcsMethodNotAllowed)
	}
}

func (s *Server) gcsGetBucket(w http.ResponseWriter, _ *http.Request, root, bucket string) {
	path, err := fakeGCSObjectPath(root, bucket, "")
	if err != nil {
		writeGCSError(w, http.StatusBadRequest, err.Error())
		return
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		writeGCSError(w, http.StatusNotFound, fmt.Sprintf("bucket %q not found", bucket))
		return
	}
	writeJSON(w, http.StatusOK, renderGCSBucketResource(bucket))
}

func (s *Server) gcsListObjects(w http.ResponseWriter, r *http.Request, root, bucket string) {
	bucketPath, err := fakeGCSObjectPath(root, bucket, "")
	if err != nil {
		writeGCSError(w, http.StatusBadRequest, err.Error())
		return
	}
	prefix := r.URL.Query().Get("prefix")

	var names []string
	walkErr := filepath.WalkDir(bucketPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if prefix == "" || strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
		return nil
	})
	if walkErr != nil {
		writeGCSError(w, http.StatusInternalServerError, walkErr.Error())
		return
	}
	sort.Strings(names)

	items := make([]map[string]any, 0, len(names))
	for _, name := range names {
		info, err := os.Stat(filepath.Join(bucketPath, filepath.FromSlash(name)))
		if err != nil {
			continue
		}
		md5sum, err := fileMD5Base64(filepath.Join(bucketPath, filepath.FromSlash(name)))
		if err != nil {
			continue
		}
		items = append(items, renderGCSObjectResource(bucket, name, info, md5sum))
	}
	writeJSON(w, http.StatusOK, map[string]any{"kind": "storage#objects", "items": items})
}

func (s *Server) gcsGetObjectMetadata(w http.ResponseWriter, root, bucket, object string) {
	path, err := fakeGCSObjectPath(root, bucket, object)
	if err != nil {
		writeGCSError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		writeGCSError(w, http.StatusNotFound, fmt.Sprintf("object %q not found in bucket %q", object, bucket))
		return
	}
	md5sum, err := fileMD5Base64(path)
	if err != nil {
		writeGCSError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, renderGCSObjectResource(bucket, object, info, md5sum))
}

func (s *Server) gcsDownloadObject(w http.ResponseWriter, root, bucket, object string) {
	path, err := fakeGCSObjectPath(root, bucket, object)
	if err != nil {
		writeGCSError(w, http.StatusBadRequest, err.Error())
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		writeGCSError(w, http.StatusNotFound, fmt.Sprintf("object %q not found in bucket %q", object, bucket))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) gcsDeleteObject(w http.ResponseWriter, root, bucket, object string) {
	path, err := fakeGCSObjectPath(root, bucket, object)
	if err != nil {
		writeGCSError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := os.Remove(path); err != nil {
		writeGCSError(w, http.StatusNotFound, fmt.Sprintf("object %q not found in bucket %q", object, bucket))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// gcsObjectUpload implements the two objects.insert upload variants that
// don't require a stateful session: "media" (data-only body, object name
// from the ?name= query parameter) and "multipart" (a multipart/related
// body: a JSON metadata part carrying the object name, then a data part).
// Multipart matters more than it first appears: the official Go client
// library's ObjectHandle.NewWriter does NOT default to media — verified by
// running the real cloud.google.com/go/storage client against this server
// with STORAGE_EMULATOR_HOST, which sent a multipart request and failed
// until this was implemented. Resumable (uploadType=resumable) is not
// implemented and fails explicitly rather than silently mishandling the
// request.
func (s *Server) gcsObjectUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeGCSError(w, http.StatusMethodNotAllowed, gcsMethodNotAllowed)
		return
	}
	root, err := fakeGCSRoot()
	if err != nil {
		writeGCSError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	bucket := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/upload/storage/v1/b/"), "/o")
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		writeGCSError(w, http.StatusBadRequest, "bucket is required")
		return
	}
	defer func() { _ = r.Body.Close() }()

	uploadType := r.URL.Query().Get("uploadType")
	var object string
	var data []byte

	switch uploadType {
	case "", "media":
		object = strings.TrimSpace(r.URL.Query().Get("name"))
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeGCSError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		data = body
	case "multipart":
		object, data, err = parseGCSMultipartUpload(r)
		if err != nil {
			writeGCSError(w, http.StatusBadRequest, err.Error())
			return
		}
		if object == "" {
			object = strings.TrimSpace(r.URL.Query().Get("name"))
		}
	default:
		writeGCSError(w, http.StatusNotImplemented, fmt.Sprintf("uploadType %q is not supported; only media and multipart are implemented", uploadType))
		return
	}

	if object == "" {
		writeGCSError(w, http.StatusBadRequest, "object name is required (via ?name= or multipart metadata)")
		return
	}

	info, md5sum, err := writeGCSObject(root, bucket, object, data)
	if err != nil {
		writeGCSError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, renderGCSObjectResource(bucket, object, info, md5sum))
}

// parseGCSMultipartUpload reads a multipart/related upload body: the first
// part is a JSON metadata object (only "name" is used), the second part is
// the raw object data. Any additional parts are ignored.
func parseGCSMultipartUpload(r *http.Request) (object string, data []byte, err error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return "", nil, fmt.Errorf("multipart upload requires a multipart/related Content-Type with a boundary")
	}
	boundary, ok := params["boundary"]
	if !ok {
		return "", nil, fmt.Errorf("multipart upload Content-Type is missing a boundary parameter")
	}

	reader := multipart.NewReader(r.Body, boundary)
	var metadata struct {
		Name string `json:"name"`
	}
	for partIndex := 0; ; partIndex++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", nil, fmt.Errorf("failed to read multipart upload: %w", err)
		}
		content, err := io.ReadAll(part)
		if err != nil {
			return "", nil, fmt.Errorf("failed to read multipart part: %w", err)
		}
		if partIndex == 0 {
			_ = json.Unmarshal(content, &metadata)
		} else {
			data = content
		}
	}
	return strings.TrimSpace(metadata.Name), data, nil
}

// writeGCSObject writes data to bucket/object under root, creating parent
// directories as needed (GCS has no real directory concept, so a nested
// object name shouldn't require the caller to pre-create anything), and
// returns the resulting file's info and base64 MD5 for rendering.
func writeGCSObject(root, bucket, object string, data []byte) (os.FileInfo, string, error) {
	path, err := fakeGCSObjectPath(root, bucket, object)
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, "", err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return nil, "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, "", err
	}
	md5sum, err := fileMD5Base64(path)
	if err != nil {
		return nil, "", err
	}
	return info, md5sum, nil
}
