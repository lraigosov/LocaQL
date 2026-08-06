package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// tableField describes one schema column. Mode defaults to NULLABLE
// (BigQuery's own default) when empty. Fields is populated only when Type is
// RECORD/STRUCT, describing the nested column's own fields positionally:
// BigQuery's REST tabledata.list/query result shape encodes a RECORD value
// as {"v": {"f": [{"v": ...}, ...]}} in that same field order, and a
// REPEATED value (any Mode == "REPEATED" field, scalar or RECORD) as
// {"v": [...]} of "single" values for the field's base type.
type tableField struct {
	Name   string
	Type   string
	Mode   string
	Fields []tableField
}

// tableRowValue documents the logical cell domain. Rows remain serialized as
// strings for backward-compatible catalog/job/session snapshots, but
// cell_storage.go adds an escaped, lossless SQL NULL tag; nested/repeated
// values remain canonical JSON and preserve their own recursive nulls.
type tableRowValue = any

type tableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
}

type tableRecord struct {
	ProjectID              string
	DatasetID              string
	TableID                string
	FriendlyName           string
	Description            string
	Labels                 map[string]string
	Schema                 []tableField
	Rows                   [][]string
	IngestionPartitions    []string
	External               *externalTableConfig
	View                   *viewConfig
	TimePartitioning       *timePartitioningConfig
	RangePartitioning      *rangePartitioningConfig
	Clustering             []string
	RequirePartitionFilter bool
	CreatedAt              time.Time
	UpdatedAt              time.Time
	Version                int
	ExpirationTime         time.Time // zero value means "never expires"
}

// viewConfig marks a table as a VIEW or MATERIALIZED_VIEW: its data is never
// stored in tableRecord.Rows, only its GoogleSQL definition. Both kinds are
// resolved by re-executing Query through the real query engine on every
// access (see resolveTableRowsVisiting in bigquery.go) rather than caching a
// snapshot — real BigQuery's periodic materialized-view refresh is not
// modeled, a simplification declared explicitly rather than faked.
type viewConfig struct {
	Query        string
	Materialized bool
}

func cloneViewConfig(v *viewConfig) *viewConfig {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

// externalTableConfig marks a table as backed by local files (or fake-GCS via
// LOCAQL_FAKE_GCS_ROOT) instead of in-memory rows. Data is never materialized
// into tableRecord.Rows; it is read fresh from sourceUris on every access, so
// external tables reflect the current file contents at query time, matching
// real BigQuery external table semantics.
type externalTableConfig struct {
	SourceURIs      []string
	SourceFormat    string
	FieldDelimiter  string
	SkipLeadingRows int
}

func cloneExternalConfig(c *externalTableConfig) *externalTableConfig {
	if c == nil {
		return nil
	}
	cp := *c
	cp.SourceURIs = append([]string(nil), c.SourceURIs...)
	return &cp
}

type tableInsert struct {
	ProjectID              string
	DatasetID              string
	TableID                string
	FriendlyName           string
	Description            string
	Labels                 map[string]string
	Schema                 []tableField
	Rows                   [][]string
	External               *externalTableConfig
	View                   *viewConfig
	TimePartitioning       *timePartitioningConfig
	RangePartitioning      *rangePartitioningConfig
	Clustering             []string
	RequirePartitionFilter bool
	ExpirationTime         time.Time
}

type tablePatch struct {
	ProjectID                 string
	DatasetID                 string
	TableID                   string
	FriendlyName              string
	Description               string
	Labels                    map[string]string
	ExpirationTime            time.Time
	Schema                    []tableField
	TimePartitioning          *timePartitioningConfig
	RangePartitioning         *rangePartitioningConfig
	Clustering                []string
	RequirePartitionFilter    bool
	HasFriendlyName           bool
	HasDescription            bool
	HasLabels                 bool
	HasExpirationTime         bool
	HasSchema                 bool
	HasTimePartitioning       bool
	HasRangePartitioning      bool
	HasClustering             bool
	HasRequirePartitionFilter bool
}

// tableUpdate mirrors real BigQuery PUT semantics: every field is fully
// replaced by whatever the request carries, including clearing a field left
// out of the body (no partial-patch Has* flags here, unlike tablePatch).
type tableUpdate struct {
	ProjectID              string
	DatasetID              string
	TableID                string
	FriendlyName           string
	Description            string
	Labels                 map[string]string
	ExpirationTime         time.Time
	TimePartitioning       *timePartitioningConfig
	RangePartitioning      *rangePartitioningConfig
	Clustering             []string
	RequirePartitionFilter bool
}

type tableService struct {
	mu              sync.RWMutex
	defaults        []string
	projects        map[string]map[string]map[string]*tableRecord
	datasetVersions map[string]int
	// now is swappable so expiration enforcement can be tested with a fake
	// clock instead of waiting on wall-clock time; production code never
	// overrides it.
	now func() time.Time
}

func newTableService() *tableService {
	return &tableService{
		defaults:        []string{"events", "daily_metrics", "users", "raw_import"},
		projects:        make(map[string]map[string]map[string]*tableRecord),
		datasetVersions: make(map[string]int),
		now:             time.Now,
	}
}

// purgeExpiredLocked deletes any table in tables whose ExpirationTime has
// passed according to s.now(), matching real BigQuery's behavior of a table
// simply ceasing to exist once expired (not a soft/reversible state — unlike
// dataset undelete, there is no tombstone for an expired table). Callers
// already hold s.mu. Returns whether anything was purged, so callers can
// decide whether to bump the dataset's version/ETag.
func (s *tableService) purgeExpiredLocked(tables map[string]*tableRecord) bool {
	if len(tables) == 0 {
		return false
	}
	now := s.now()
	purged := false
	for id, t := range tables {
		if !t.ExpirationTime.IsZero() && !t.ExpirationTime.After(now) {
			delete(tables, id)
			purged = true
			continue
		}
		if purgeExpiredPartitions(t, now) {
			purged = true
		}
	}
	return purged
}

func (s *tableService) list(projectID, datasetID string, start, size int) ([]*tableRecord, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	if s.purgeExpiredLocked(tables) {
		s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	}
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
		out = append(out, cloneTableRecord(tables[id]))
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
	if s.purgeExpiredLocked(tables) {
		s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	}
	t := tables[tableID]
	if t == nil {
		return nil, false, 0
	}
	return cloneTableRecord(t), true, t.Version
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
	now := s.now().UTC()
	t := &tableRecord{
		ProjectID:              projectID,
		DatasetID:              datasetID,
		TableID:                tableID,
		FriendlyName:           strings.TrimSpace(input.FriendlyName),
		Description:            strings.TrimSpace(input.Description),
		Labels:                 cloneLabels(input.Labels),
		Schema:                 cloneTableFields(input.Schema),
		Rows:                   cloneTableRows(input.Rows),
		External:               cloneExternalConfig(input.External),
		View:                   cloneViewConfig(input.View),
		TimePartitioning:       cloneTimePartitioning(input.TimePartitioning),
		RangePartitioning:      cloneRangePartitioning(input.RangePartitioning),
		Clustering:             cloneStrings(input.Clustering),
		RequirePartitionFilter: input.RequirePartitionFilter,
		CreatedAt:              now,
		UpdatedAt:              now,
		Version:                1,
		ExpirationTime:         input.ExpirationTime,
	}
	if len(t.Schema) == 0 && len(t.Rows) > 0 {
		t.Schema = inferSchemaFromRows(t.Rows)
	}
	if err := validateRowsForPartitioning(t.Schema, t.Rows, t.TimePartitioning, t.RangePartitioning); err != nil {
		return nil, false
	}
	t.IngestionPartitions = newIngestionPartitions(t.TimePartitioning, len(t.Rows), now)
	tables[tableID] = t
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return cloneTableRecord(t), true
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

// deleteIfVersion removes a table only if it is still the exact catalog
// version a SQL statement analyzed. Query jobs also serialize on their target
// table, but direct REST writes do not pass through that queue; the version
// check prevents a concurrent REST mutation from being silently discarded by
// a DROP TABLE that started from an older snapshot.
func (s *tableService) deleteIfVersion(projectID, datasetID, tableID string, expectedVersion int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return fmt.Errorf("table not found: %s.%s", datasetID, tableID)
	}
	if t.Version != expectedVersion {
		return fmt.Errorf("table %s.%s changed concurrently; retry the statement", datasetID, tableID)
	}
	delete(tables, tableID)
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return nil
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
	if input.HasExpirationTime {
		t.ExpirationTime = input.ExpirationTime
	}
	if input.HasSchema {
		t.Schema = cloneTableFields(input.Schema)
	}
	if input.HasTimePartitioning {
		t.TimePartitioning = cloneTimePartitioning(input.TimePartitioning)
	}
	if input.HasRangePartitioning {
		t.RangePartitioning = cloneRangePartitioning(input.RangePartitioning)
	}
	if input.HasClustering {
		t.Clustering = cloneStrings(input.Clustering)
	}
	if input.HasRequirePartitionFilter {
		t.RequirePartitionFilter = input.RequirePartitionFilter
	}

	t.UpdatedAt = s.now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++

	return cloneTableRecord(t), true
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
	t.ExpirationTime = input.ExpirationTime
	t.TimePartitioning = cloneTimePartitioning(input.TimePartitioning)
	t.RangePartitioning = cloneRangePartitioning(input.RangePartitioning)
	t.Clustering = cloneStrings(input.Clustering)
	t.RequirePartitionFilter = input.RequirePartitionFilter
	t.UpdatedAt = s.now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++

	return cloneTableRecord(t), true
}

func (s *tableService) getData(projectID, datasetID, tableID string) ([]tableField, [][]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	if s.purgeExpiredLocked(tables) {
		s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	}
	t := tables[tableID]
	if t == nil {
		return nil, nil, false
	}
	return cloneTableFields(t.Schema), cloneTableRows(t.Rows), true
}

// replaceRowsIfVersion atomically commits the final row image produced by a
// DML statement while preserving the table's declared schema and metadata.
// The compare-and-swap closes the race with direct REST writes that are not
// scheduled as jobs. Views and external tables are deliberately immutable via
// DML: materializing either into an ordinary table would destroy its identity.
func (s *tableService) replaceRowsIfVersion(projectID, datasetID, tableID string, expectedVersion int, rows [][]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return fmt.Errorf("table not found: %s.%s", datasetID, tableID)
	}
	if t.Version != expectedVersion {
		return fmt.Errorf("table %s.%s changed concurrently; retry the statement", datasetID, tableID)
	}
	if t.View != nil {
		return fmt.Errorf("DML target %s.%s is a view", datasetID, tableID)
	}
	if t.External != nil {
		return fmt.Errorf("DML target %s.%s is an external table", datasetID, tableID)
	}
	if err := validateStoredRows(t.Schema, rows); err != nil {
		return err
	}
	if err := validateRowsForPartitioning(t.Schema, rows, t.TimePartitioning, t.RangePartitioning); err != nil {
		return err
	}
	currentPartition := ingestionPartitionID(t.TimePartitioning, s.now().UTC())
	t.IngestionPartitions = reconcileIngestionPartitions(t.Rows, rows, t.IngestionPartitions, currentPartition)
	t.Rows = cloneTableRows(rows)
	t.UpdatedAt = s.now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return nil
}

// replaceTableIfVersion commits CREATE OR REPLACE TABLE over an existing
// resource. Unlike DML, DDL replaces the table definition itself, so schema,
// rows, view and external-table identity all change together under one lock.
func (s *tableService) replaceTableIfVersion(projectID, datasetID, tableID string, expectedVersion int, schema []tableField, rows [][]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return fmt.Errorf("table not found: %s.%s", datasetID, tableID)
	}
	if t.Version != expectedVersion {
		return fmt.Errorf("table %s.%s changed concurrently; retry the statement", datasetID, tableID)
	}
	if err := validateStoredRows(schema, rows); err != nil {
		return err
	}
	t.Schema = cloneTableFields(schema)
	t.Rows = cloneTableRows(rows)
	t.View = nil
	t.External = nil
	t.TimePartitioning = nil
	t.RangePartitioning = nil
	t.Clustering = nil
	t.RequirePartitionFilter = false
	t.IngestionPartitions = nil
	t.UpdatedAt = s.now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return nil
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
	created := existing == nil
	if created {
		if createDisposition == "CREATE_NEVER" {
			return 0, fmt.Errorf("destination table not found with CREATE_NEVER")
		}
		now := s.now().UTC()
		existing = &tableRecord{
			ProjectID: projectID,
			DatasetID: datasetID,
			TableID:   tableID,
			CreatedAt: now,
			UpdatedAt: now,
			Version:   1,
		}
	} else {
		if writeDisposition == "WRITE_EMPTY" && len(existing.Rows) > 0 {
			return 0, fmt.Errorf("destination table is not empty")
		}
	}

	if writeDisposition == "WRITE_APPEND" && len(existing.Schema) > 0 && len(schema) > 0 && !sameSchema(existing.Schema, schema) {
		return 0, fmt.Errorf("source and destination schemas do not match for WRITE_APPEND")
	}

	newSchema := cloneTableFields(schema)
	newRows := cloneTableRows(rows)
	newPartitions := newIngestionPartitions(existing.TimePartitioning, len(newRows), s.now().UTC())
	if writeDisposition == "WRITE_APPEND" {
		newRows = append(cloneTableRows(existing.Rows), newRows...)
		if len(existing.Schema) > 0 {
			newSchema = cloneTableFields(existing.Schema)
		}
		newPartitions = append(cloneStrings(existing.IngestionPartitions), newPartitions...)
	} else {
		newPartitions = newIngestionPartitions(existing.TimePartitioning, len(newRows), s.now().UTC())
	}
	if len(newSchema) == 0 && len(newRows) > 0 {
		newSchema = inferSchemaFromRows(newRows)
	}
	if err := validateStoredRows(newSchema, newRows); err != nil {
		return 0, err
	}
	if err := validateRowsForPartitioning(newSchema, newRows, existing.TimePartitioning, existing.RangePartitioning); err != nil {
		return 0, err
	}
	existing.Schema = newSchema
	existing.Rows = newRows
	existing.IngestionPartitions = newPartitions
	if created {
		tables[tableID] = existing
	}
	existing.UpdatedAt = s.now().UTC()
	existing.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return len(rows), nil
}

// datasetTableCount reports how many tables exist for projectID/datasetID
// WITHOUT triggering ensureDatasetLocked's lazy demo-table seeding. A dataset
// that was never touched via the tables service reports 0, even though a
// later read would auto-seed default demo tables.
func (s *tableService) datasetTableCount(projectID, datasetID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.projects[projectID]
	if proj == nil {
		return 0
	}
	tables := proj[datasetID]
	if s.purgeExpiredLocked(tables) {
		s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	}
	return len(tables)
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
	now := s.now().UTC()
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

// cloneTableFields deep-clones a schema, recursively cloning a RECORD
// field's nested Fields rather than sharing the same backing slice across
// clones (a plain copy() would only copy the tableField struct itself,
// leaving every clone's Fields slice aliased to the same array).
func cloneTableFields(fields []tableField) []tableField {
	if len(fields) == 0 {
		return nil
	}
	out := make([]tableField, len(fields))
	for i, f := range fields {
		out[i] = f
		out[i].Fields = cloneTableFields(f.Fields)
	}
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
