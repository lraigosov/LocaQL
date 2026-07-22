package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type tableField struct {
	Name string
	Type string
}

type tableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
}

type tableRecord struct {
	ProjectID    string
	DatasetID    string
	TableID      string
	FriendlyName string
	Description  string
	Labels       map[string]string
	Schema       []tableField
	Rows         [][]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Version      int
}

type tableInsert struct {
	ProjectID    string
	DatasetID    string
	TableID      string
	FriendlyName string
	Description  string
	Labels       map[string]string
	Schema       []tableField
	Rows         [][]string
}

type tablePatch struct {
	ProjectID       string
	DatasetID       string
	TableID         string
	FriendlyName    string
	Description     string
	Labels          map[string]string
	HasFriendlyName bool
	HasDescription  bool
	HasLabels       bool
}

type tableUpdate struct {
	ProjectID    string
	DatasetID    string
	TableID      string
	FriendlyName string
	Description  string
	Labels       map[string]string
}

type tableService struct {
	mu              sync.RWMutex
	defaults        []string
	projects        map[string]map[string]map[string]*tableRecord
	datasetVersions map[string]int
}

func newTableService() *tableService {
	return &tableService{
		defaults:        []string{"events", "daily_metrics", "users", "raw_import"},
		projects:        make(map[string]map[string]map[string]*tableRecord),
		datasetVersions: make(map[string]int),
	}
}

func (s *tableService) list(projectID, datasetID string, start, size int) ([]*tableRecord, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	version := s.datasetVersions[s.datasetKey(projectID, datasetID)]
	ids := make([]string, 0, len(tables))
	for id := range tables {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	if start > len(ids) {
		start = len(ids)
	}
	end := start + size
	if end > len(ids) {
		end = len(ids)
	}

	out := make([]*tableRecord, 0, end-start)
	for _, id := range ids[start:end] {
		cp := *tables[id]
		out = append(out, &cp)
	}

	next := -1
	if end < len(ids) {
		next = end
	}

	return out, next, version
}

func (s *tableService) get(projectID, datasetID, tableID string) (*tableRecord, bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, false, 0
	}
	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true, t.Version
}

func (s *tableService) insert(input tableInsert) (*tableRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	tableID := strings.TrimSpace(input.TableID)
	if projectID == "" || datasetID == "" || tableID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := tables[tableID]; exists {
		return nil, false
	}
	now := time.Now().UTC()
	t := &tableRecord{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		TableID:      tableID,
		FriendlyName: strings.TrimSpace(input.FriendlyName),
		Description:  strings.TrimSpace(input.Description),
		Labels:       cloneLabels(input.Labels),
		Schema:       cloneTableFields(input.Schema),
		Rows:         cloneTableRows(input.Rows),
		CreatedAt:    now,
		UpdatedAt:    now,
		Version:      1,
	}
	if len(t.Schema) == 0 && len(t.Rows) > 0 {
		t.Schema = inferSchemaFromRows(t.Rows)
	}
	tables[tableID] = t
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
	cp.Schema = cloneTableFields(cp.Schema)
	cp.Rows = cloneTableRows(cp.Rows)
	return &cp, true
}

func (s *tableService) delete(projectID, datasetID, tableID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := tables[tableID]; !exists {
		return false
	}
	delete(tables, tableID)
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return true
}

func (s *tableService) patch(input tablePatch) (*tableRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	tableID := strings.TrimSpace(input.TableID)
	if projectID == "" || datasetID == "" || tableID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, false
	}

	if input.HasFriendlyName {
		t.FriendlyName = strings.TrimSpace(input.FriendlyName)
	}
	if input.HasDescription {
		t.Description = strings.TrimSpace(input.Description)
	}
	if input.HasLabels {
		t.Labels = cloneLabels(input.Labels)
	}

	t.UpdatedAt = time.Now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++

	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
	cp.Schema = cloneTableFields(cp.Schema)
	cp.Rows = cloneTableRows(cp.Rows)
	return &cp, true
}

func (s *tableService) update(input tableUpdate) (*tableRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	tableID := strings.TrimSpace(input.TableID)
	if projectID == "" || datasetID == "" || tableID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, false
	}

	t.FriendlyName = strings.TrimSpace(input.FriendlyName)
	t.Description = strings.TrimSpace(input.Description)
	t.Labels = cloneLabels(input.Labels)
	t.UpdatedAt = time.Now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++

	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
	cp.Schema = cloneTableFields(cp.Schema)
	cp.Rows = cloneTableRows(cp.Rows)
	return &cp, true
}

func (s *tableService) getData(projectID, datasetID, tableID string) ([]tableField, [][]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, nil, false
	}
	return cloneTableFields(t.Schema), cloneTableRows(t.Rows), true
}

func (s *tableService) upsertCopyDestination(dest tableReference, schema []tableField, rows [][]string, createDisposition, writeDisposition string) (int, error) {
	projectID := strings.TrimSpace(dest.ProjectID)
	datasetID := strings.TrimSpace(dest.DatasetID)
	tableID := strings.TrimSpace(dest.TableID)
	if projectID == "" || datasetID == "" || tableID == "" {
		return 0, fmt.Errorf("destinationTable is required")
	}

	createDisposition = normalizeCreateDisposition(createDisposition)
	writeDisposition = normalizeWriteDisposition(writeDisposition)

	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	existing := tables[tableID]
	if existing == nil {
		if createDisposition == "CREATE_NEVER" {
			return 0, fmt.Errorf("destination table not found with CREATE_NEVER")
		}
		now := time.Now().UTC()
		existing = &tableRecord{
			ProjectID: projectID,
			DatasetID: datasetID,
			TableID:   tableID,
			CreatedAt: now,
			UpdatedAt: now,
			Version:   1,
		}
		tables[tableID] = existing
	} else {
		if writeDisposition == "WRITE_EMPTY" && len(existing.Rows) > 0 {
			return 0, fmt.Errorf("destination table is not empty")
		}
	}

	if writeDisposition == "WRITE_APPEND" && len(existing.Schema) > 0 && len(schema) > 0 && !sameSchema(existing.Schema, schema) {
		return 0, fmt.Errorf("source and destination schemas do not match for WRITE_APPEND")
	}

	if writeDisposition == "WRITE_APPEND" {
		existing.Rows = append(cloneTableRows(existing.Rows), cloneTableRows(rows)...)
		if len(existing.Schema) == 0 {
			existing.Schema = cloneTableFields(schema)
		}
	} else {
		existing.Schema = cloneTableFields(schema)
		existing.Rows = cloneTableRows(rows)
	}
	if len(existing.Schema) == 0 && len(existing.Rows) > 0 {
		existing.Schema = inferSchemaFromRows(existing.Rows)
	}
	existing.UpdatedAt = time.Now().UTC()
	existing.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return len(rows), nil
}

// datasetTableCount reports how many tables exist for projectID/datasetID
// WITHOUT triggering ensureDatasetLocked's lazy demo-table seeding. A dataset
// that was never touched via the tables service reports 0, even though a
// later read would auto-seed default demo tables.
func (s *tableService) datasetTableCount(projectID, datasetID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	proj := s.projects[projectID]
	if proj == nil {
		return 0
	}
	return len(proj[datasetID])
}

// deleteAllForDataset removes every table tracked for projectID/datasetID
// without seeding. It is a no-op if the dataset was never materialized.
func (s *tableService) deleteAllForDataset(projectID, datasetID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.projects[projectID]
	if proj == nil {
		return
	}
	if _, ok := proj[datasetID]; !ok {
		return
	}
	delete(proj, datasetID)
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
}

func (s *tableService) ensureDatasetLocked(projectID, datasetID string) map[string]*tableRecord {
	proj := s.projects[projectID]
	if proj == nil {
		proj = make(map[string]map[string]*tableRecord)
		s.projects[projectID] = proj
	}

	tables := proj[datasetID]
	if tables != nil {
		return tables
	}

	tables = make(map[string]*tableRecord)
	now := time.Now().UTC()
	for _, id := range s.defaults {
		schema, rows := defaultTableData(id)
		tables[id] = &tableRecord{
			ProjectID: projectID,
			DatasetID: datasetID,
			TableID:   id,
			Schema:    schema,
			Rows:      rows,
			CreatedAt: now,
			UpdatedAt: now,
			Version:   1,
		}
	}
	proj[datasetID] = tables
	key := s.datasetKey(projectID, datasetID)
	if _, ok := s.datasetVersions[key]; !ok {
		s.datasetVersions[key] = 1
	}
	return tables
}

func (s *tableService) datasetKey(projectID, datasetID string) string {
	return projectID + ":" + datasetID
}

func cloneTableFields(fields []tableField) []tableField {
	if len(fields) == 0 {
		return nil
	}
	out := make([]tableField, len(fields))
	copy(out, fields)
	return out
}

func cloneTableRows(rows [][]string) [][]string {
	if len(rows) == 0 {
		return nil
	}
	out := make([][]string, len(rows))
	for i, row := range rows {
		out[i] = append([]string(nil), row...)
	}
	return out
}

func inferSchemaFromRows(rows [][]string) []tableField {
	if len(rows) == 0 {
		return nil
	}
	width := len(rows[0])
	fields := make([]tableField, 0, width)
	for i := 0; i < width; i++ {
		fields = append(fields, tableField{Name: fmt.Sprintf("col_%d", i+1), Type: "STRING"})
	}
	return fields
}

func sameSchema(left, right []tableField) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].Type != right[i].Type {
			return false
		}
	}
	return true
}

func normalizeCreateDisposition(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	if v == "CREATE_NEVER" {
		return "CREATE_NEVER"
	}
	return "CREATE_IF_NEEDED"
}

func normalizeWriteDisposition(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	switch v {
	case "WRITE_TRUNCATE", "WRITE_APPEND":
		return v
	default:
		return "WRITE_EMPTY"
	}
}

func defaultTableData(tableID string) ([]tableField, [][]string) {
	switch tableID {
	case "daily_metrics":
		return []tableField{{Name: "metric_date", Type: "DATE"}, {Name: "metric_name", Type: "STRING"}, {Name: "metric_value", Type: "INT64"}}, [][]string{{"2026-07-18", "signups", "12"}, {"2026-07-19", "signups", "15"}, {"2026-07-20", "signups", "11"}, {"2026-07-21", "signups", "19"}}
	case "events":
		return []tableField{{Name: "event_id", Type: "INT64"}, {Name: "event_name", Type: "STRING"}}, [][]string{{"1", "page_view"}, {"2", "checkout"}, {"3", "purchase"}, {"4", "refund"}}
	case "users":
		return []tableField{{Name: "user_id", Type: "INT64"}, {Name: "user_name", Type: "STRING"}}, [][]string{{"1", "alice"}, {"2", "bob"}, {"3", "carol"}, {"4", "dave"}}
	default:
		return []tableField{{Name: "col_1", Type: "STRING"}, {Name: "col_2", Type: "STRING"}}, [][]string{{"1", "alpha"}, {"2", "beta"}, {"3", "gamma"}, {"4", "delta"}}
	}
}
