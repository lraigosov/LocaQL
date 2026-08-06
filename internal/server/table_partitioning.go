package server

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type timePartitioningConfig struct {
	Type         string
	Field        string
	ExpirationMs int64
}

type rangePartitioningConfig struct {
	Field    string
	Start    int64
	End      int64
	Interval int64
}

func cloneTimePartitioning(c *timePartitioningConfig) *timePartitioningConfig {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func cloneRangePartitioning(c *rangePartitioningConfig) *rangePartitioningConfig {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string(nil), values...)
}

func cloneTableRecord(t *tableRecord) *tableRecord {
	if t == nil {
		return nil
	}
	cp := *t
	cp.Labels = cloneLabels(t.Labels)
	cp.Schema = cloneTableFields(t.Schema)
	cp.Rows = cloneTableRows(t.Rows)
	cp.IngestionPartitions = cloneStrings(t.IngestionPartitions)
	cp.External = cloneExternalConfig(t.External)
	cp.View = cloneViewConfig(t.View)
	cp.TimePartitioning = cloneTimePartitioning(t.TimePartitioning)
	cp.RangePartitioning = cloneRangePartitioning(t.RangePartitioning)
	cp.Clustering = cloneStrings(t.Clustering)
	return &cp
}

func findTopLevelField(schema []tableField, name string) (tableField, int, bool) {
	for i, field := range schema {
		if strings.EqualFold(field.Name, strings.TrimSpace(name)) {
			return field, i, true
		}
	}
	return tableField{}, -1, false
}

func parseTimePartitioning(v any, schema []tableField) (*timePartitioningConfig, error) {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("timePartitioning must be an object")
	}

	partitionType := "DAY"
	if rawType, exists := obj["type"]; exists {
		typeString, ok := rawType.(string)
		if !ok {
			return nil, fmt.Errorf("timePartitioning.type must be a string")
		}
		partitionType = strings.ToUpper(strings.TrimSpace(typeString))
	}
	switch partitionType {
	case "DAY", "HOUR", "MONTH", "YEAR":
	default:
		return nil, fmt.Errorf("timePartitioning.type must be DAY, HOUR, MONTH, or YEAR")
	}

	config := &timePartitioningConfig{Type: partitionType}
	if rawExpiration, exists := obj["expirationMs"]; exists {
		if rawExpiration != nil {
			expiration, ok := parseExactInt64(rawExpiration)
			if !ok || expiration <= 0 || expiration > math.MaxInt64/int64(time.Millisecond) {
				return nil, fmt.Errorf("timePartitioning.expirationMs must be a positive integer string or number")
			}
			config.ExpirationMs = expiration
		}
	}

	if rawField, exists := obj["field"]; exists {
		fieldName, ok := rawField.(string)
		if !ok || strings.TrimSpace(fieldName) == "" {
			return nil, fmt.Errorf("timePartitioning.field must be a non-empty string")
		}
		field, _, found := findTopLevelField(schema, fieldName)
		if !found {
			return nil, fmt.Errorf("timePartitioning.field %q is not a top-level schema field", strings.TrimSpace(fieldName))
		}
		fieldType := strings.ToUpper(strings.TrimSpace(field.Type))
		if fieldType != "DATE" && fieldType != "TIMESTAMP" {
			return nil, fmt.Errorf("timePartitioning.field %q must have type DATE or TIMESTAMP", field.Name)
		}
		if normalizeMode(field.Mode) == "REPEATED" {
			return nil, fmt.Errorf("timePartitioning.field %q cannot be REPEATED", field.Name)
		}
		if fieldType == "DATE" && partitionType == "HOUR" {
			return nil, fmt.Errorf("HOUR partitioning requires a TIMESTAMP field")
		}
		config.Field = field.Name
	}
	return config, nil
}

func parseRangePartitioning(v any, schema []tableField) (*rangePartitioningConfig, error) {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("rangePartitioning must be an object")
	}
	rawField, ok := obj["field"].(string)
	if !ok || strings.TrimSpace(rawField) == "" {
		return nil, fmt.Errorf("rangePartitioning.field is required")
	}
	field, _, found := findTopLevelField(schema, rawField)
	if !found {
		return nil, fmt.Errorf("rangePartitioning.field %q is not a top-level schema field", strings.TrimSpace(rawField))
	}
	if !strings.EqualFold(field.Type, "INT64") || normalizeMode(field.Mode) == "REPEATED" {
		return nil, fmt.Errorf("rangePartitioning.field %q must be a non-repeated INT64 field", field.Name)
	}
	rangeObject, ok := obj["range"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("rangePartitioning.range is required")
	}
	start, startOK := parseExactInt64(rangeObject["start"])
	end, endOK := parseExactInt64(rangeObject["end"])
	interval, intervalOK := parseExactInt64(rangeObject["interval"])
	if !startOK || !endOK || !intervalOK {
		return nil, fmt.Errorf("rangePartitioning.range.start, end, and interval must be integer strings or numbers")
	}
	if start >= end {
		return nil, fmt.Errorf("rangePartitioning.range.start must be less than end")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("rangePartitioning.range.interval must be positive")
	}
	return &rangePartitioningConfig{Field: field.Name, Start: start, End: end, Interval: interval}, nil
}

func parseExactInt64(v any) (int64, bool) {
	switch value := v.(type) {
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed, err == nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) || value < -9223372036854775808.0 || value >= 9223372036854775808.0 {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func parseClustering(v any, schema []tableField) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("clustering must be an object")
	}
	rawFields, ok := obj["fields"].([]any)
	if !ok {
		return nil, fmt.Errorf("clustering.fields must be an array")
	}
	if len(rawFields) > 4 {
		return nil, fmt.Errorf("clustering.fields supports at most 4 columns")
	}
	fields := make([]string, 0, len(rawFields))
	seen := map[string]bool{}
	for _, raw := range rawFields {
		name, ok := raw.(string)
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("clustering.fields values must be non-empty strings")
		}
		field, _, found := findTopLevelField(schema, name)
		if !found {
			return nil, fmt.Errorf("clustering field %q is not a top-level schema field", strings.TrimSpace(name))
		}
		if normalizeMode(field.Mode) == "REPEATED" || isRecordType(field.Type) {
			return nil, fmt.Errorf("clustering field %q must be a non-repeated scalar field", field.Name)
		}
		key := strings.ToLower(field.Name)
		if seen[key] {
			return nil, fmt.Errorf("clustering field %q is duplicated", field.Name)
		}
		seen[key] = true
		fields = append(fields, field.Name)
	}
	return fields, nil
}

func sameTimePartitioningDefinition(a, b *timePartitioningConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return strings.EqualFold(a.Type, b.Type) && strings.EqualFold(a.Field, b.Field)
}

func sameRangePartitioning(a, b *rangePartitioningConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return strings.EqualFold(a.Field, b.Field) && a.Start == b.Start && a.End == b.End && a.Interval == b.Interval
}

func validatePartitioningCombination(timeConfig *timePartitioningConfig, rangeConfig *rangePartitioningConfig, requireFilter bool) error {
	if timeConfig != nil && rangeConfig != nil {
		return fmt.Errorf("timePartitioning and rangePartitioning are mutually exclusive")
	}
	if requireFilter && timeConfig == nil && rangeConfig == nil {
		return fmt.Errorf("requirePartitionFilter can only be true for a partitioned table")
	}
	if requireFilter && timeConfig != nil && timeConfig.Field == "" {
		return fmt.Errorf("requirePartitionFilter for ingestion-time partitioning is not supported until _PARTITIONTIME/_PARTITIONDATE query pseudocolumns are implemented")
	}
	return nil
}

func ingestionPartitionID(config *timePartitioningConfig, at time.Time) string {
	if config == nil || config.Field != "" {
		return ""
	}
	return formatTimePartitionID(at.UTC(), config.Type)
}

func newIngestionPartitions(config *timePartitioningConfig, count int, at time.Time) []string {
	id := ingestionPartitionID(config, at)
	if id == "" || count == 0 {
		return nil
	}
	partitions := make([]string, count)
	for i := range partitions {
		partitions[i] = id
	}
	return partitions
}

func reconcileIngestionPartitions(oldRows, newRows [][]string, oldPartitions []string, currentPartition string) []string {
	if currentPartition == "" || len(newRows) == 0 {
		return nil
	}
	available := make(map[string][]int, len(oldRows))
	oldPartition := make([]string, len(oldRows))
	for i, row := range oldRows {
		partition := currentPartition
		if i < len(oldPartitions) && oldPartitions[i] != "" {
			partition = oldPartitions[i]
		}
		oldPartition[i] = partition
		keyBytes, _ := json.Marshal(row)
		key := string(keyBytes)
		available[key] = append(available[key], i)
	}
	result := make([]string, len(newRows))
	usedOld := make([]bool, len(oldRows))
	for i, row := range newRows {
		keyBytes, _ := json.Marshal(row)
		key := string(keyBytes)
		choices := available[key]
		if len(choices) == 0 {
			continue
		}
		oldIndex := choices[0]
		result[i] = oldPartition[oldIndex]
		usedOld[oldIndex] = true
		available[key] = choices[1:]
	}
	// An UPDATE changes a row's values but not its ingestion-time partition.
	// googlesqlite retains target-table row order for UPDATE, so unmatched
	// before/after rows at the same index are the safest observable mapping.
	// Exact matches were consumed first, which keeps DELETE and INSERT cases
	// stable; genuinely new unmatched rows receive the current partition.
	for i := range result {
		if result[i] != "" {
			continue
		}
		if i < len(oldRows) && !usedOld[i] {
			result[i] = oldPartition[i]
			usedOld[i] = true
			continue
		}
		result[i] = currentPartition
	}
	return result
}

func validateRowsForPartitioning(schema []tableField, rows [][]string, timeConfig *timePartitioningConfig, rangeConfig *rangePartitioningConfig) error {
	if timeConfig != nil && timeConfig.Field != "" {
		field, index, _ := findTopLevelField(schema, timeConfig.Field)
		for rowIndex, row := range rows {
			cell := storedNullCell
			if index < len(row) {
				cell = row[index]
			}
			if _, isNull := loadStoredCell(cell); isNull {
				continue
			}
			if _, ok := timePartitionIDFromCell(field.Type, cell, timeConfig.Type); !ok {
				return fmt.Errorf("row %d partition field %q is not a valid %s value", rowIndex, field.Name, field.Type)
			}
		}
	}
	if rangeConfig != nil {
		field, index, _ := findTopLevelField(schema, rangeConfig.Field)
		for rowIndex, row := range rows {
			cell := storedNullCell
			if index < len(row) {
				cell = row[index]
			}
			decoded, isNull := loadStoredCell(cell)
			if isNull {
				continue
			}
			if _, err := strconv.ParseInt(decoded, 10, 64); err != nil {
				return fmt.Errorf("row %d partition field %q is not a valid INT64 value", rowIndex, field.Name)
			}
		}
	}
	return nil
}

func partitionCounts(table *tableRecord) map[string]int {
	counts := map[string]int{}
	switch {
	case table.TimePartitioning != nil && table.TimePartitioning.Field == "":
		fallback := ingestionPartitionID(table.TimePartitioning, table.CreatedAt)
		for i := range table.Rows {
			partition := fallback
			if i < len(table.IngestionPartitions) && table.IngestionPartitions[i] != "" {
				partition = table.IngestionPartitions[i]
			}
			counts[partition]++
		}
	case table.TimePartitioning != nil:
		field, index, found := findTopLevelField(table.Schema, table.TimePartitioning.Field)
		if !found {
			return counts
		}
		for _, row := range table.Rows {
			cell := storedNullCell
			if index < len(row) {
				cell = row[index]
			}
			if _, isNull := loadStoredCell(cell); isNull {
				counts["__NULL__"]++
				continue
			}
			partition, ok := timePartitionIDFromCell(field.Type, cell, table.TimePartitioning.Type)
			if !ok {
				partition = "__UNPARTITIONED__"
			}
			counts[partition]++
		}
	case table.RangePartitioning != nil:
		_, index, found := findTopLevelField(table.Schema, table.RangePartitioning.Field)
		if !found {
			return counts
		}
		for _, row := range table.Rows {
			cell := storedNullCell
			if index < len(row) {
				cell = row[index]
			}
			decoded, isNull := loadStoredCell(cell)
			if isNull {
				counts["__NULL__"]++
				continue
			}
			value, err := strconv.ParseInt(decoded, 10, 64)
			if err != nil || value < table.RangePartitioning.Start || value >= table.RangePartitioning.End {
				counts["__UNPARTITIONED__"]++
				continue
			}
			bucket := table.RangePartitioning.Start + ((value-table.RangePartitioning.Start)/table.RangePartitioning.Interval)*table.RangePartitioning.Interval
			counts[strconv.FormatInt(bucket, 10)]++
		}
	default:
		counts[storedNullCell] = len(table.Rows)
	}
	return counts
}

func sortedPartitionIDs(counts map[string]int) []string {
	ids := make([]string, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func purgeExpiredPartitions(table *tableRecord, now time.Time) bool {
	config := table.TimePartitioning
	if config == nil || config.ExpirationMs <= 0 || len(table.Rows) == 0 || table.External != nil || table.View != nil {
		return false
	}
	field := tableField{}
	fieldIndex := -1
	if config.Field != "" {
		var found bool
		field, fieldIndex, found = findTopLevelField(table.Schema, config.Field)
		if !found {
			return false
		}
	}
	keptRows := make([][]string, 0, len(table.Rows))
	keptPartitions := make([]string, 0, len(table.IngestionPartitions))
	removed := false
	for rowIndex, row := range table.Rows {
		partitionID := ""
		if config.Field == "" {
			partitionID = ingestionPartitionID(config, table.CreatedAt)
			if rowIndex < len(table.IngestionPartitions) && table.IngestionPartitions[rowIndex] != "" {
				partitionID = table.IngestionPartitions[rowIndex]
			}
		} else {
			cell := storedNullCell
			if fieldIndex < len(row) {
				cell = row[fieldIndex]
			}
			if _, isNull := loadStoredCell(cell); !isNull {
				partitionID, _ = timePartitionIDFromCell(field.Type, cell, config.Type)
			}
		}
		partitionStart, ok := parseTimePartitionID(partitionID, config.Type)
		if ok && !partitionStart.Add(time.Duration(config.ExpirationMs)*time.Millisecond).After(now.UTC()) {
			removed = true
			continue
		}
		keptRows = append(keptRows, append([]string(nil), row...))
		if config.Field == "" {
			keptPartitions = append(keptPartitions, partitionID)
		}
	}
	if !removed {
		return false
	}
	table.Rows = keptRows
	if config.Field == "" {
		table.IngestionPartitions = keptPartitions
	}
	table.UpdatedAt = now.UTC()
	table.Version++
	return true
}

func parseTimePartitionID(partitionID, partitionType string) (time.Time, bool) {
	if partitionID == "" || strings.HasPrefix(partitionID, "__") {
		return time.Time{}, false
	}
	layout := "20060102"
	switch strings.ToUpper(partitionType) {
	case "HOUR":
		layout = "2006010215"
	case "MONTH":
		layout = "200601"
	case "YEAR":
		layout = "2006"
	}
	parsed, err := time.ParseInLocation(layout, partitionID, time.UTC)
	return parsed, err == nil
}

func timePartitionIDFromCell(fieldType, cell, partitionType string) (string, bool) {
	decoded, isNull := loadStoredCell(cell)
	if isNull {
		return "", false
	}
	var parsed time.Time
	var err error
	if strings.EqualFold(fieldType, "DATE") {
		parsed, err = time.Parse("2006-01-02", decoded)
	} else {
		parsed, err = parsePartitionTimestamp(decoded)
	}
	if err != nil {
		return "", false
	}
	return formatTimePartitionID(parsed.UTC(), partitionType), true
}

func parsePartitionTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 MST",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		whole, fraction := math.Modf(seconds)
		return time.Unix(int64(whole), int64(fraction*float64(time.Second))).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
}

func formatTimePartitionID(value time.Time, partitionType string) string {
	switch strings.ToUpper(partitionType) {
	case "HOUR":
		return value.Format("2006010215")
	case "MONTH":
		return value.Format("200601")
	case "YEAR":
		return value.Format("2006")
	default:
		return value.Format("20060102")
	}
}

var whereClausePattern = regexp.MustCompile(`(?is)\bWHERE\b(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|\bQUALIFY\b|\bLIMIT\b|$)`)

func queryHasPartitionFilter(queryText string, table *tableRecord) bool {
	columns := []string{}
	if table.TimePartitioning != nil {
		if table.TimePartitioning.Field != "" {
			columns = append(columns, table.TimePartitioning.Field)
		} else {
			columns = append(columns, "_PARTITIONTIME", "_PARTITIONDATE")
		}
	}
	if table.RangePartitioning != nil {
		columns = append(columns, table.RangePartitioning.Field)
	}
	for _, match := range whereClausePattern.FindAllStringSubmatch(queryText, -1) {
		if len(match) < 2 {
			continue
		}
		for _, column := range columns {
			pattern := regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9_])` + regexp.QuoteMeta(column) + `(?:[^A-Za-z0-9_]|$)`)
			if pattern.MatchString(match[1]) {
				return true
			}
		}
	}
	return false
}
