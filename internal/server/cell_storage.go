package server

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Stored rows predate nullable cells and are serialized as [][]string across
// the catalog, jobs, sessions and Storage APIs. These two NUL-prefixed tags
// make that representation lossless without invalidating existing snapshots:
// an untagged string remains an ordinary value, storedNullCell is SQL NULL,
// and a user value which could collide with either internal tag is escaped.
// NUL is valid in JSON strings but cannot occur accidentally in CSV text and
// is escaped by every JSON encoder, making the on-disk representation safe.
const (
	storedNullCell          = "\x00locaql:null"
	storedEscapedCellPrefix = "\x00locaql:value:"
)

func storeStringCell(value string) string {
	if value == storedNullCell || strings.HasPrefix(value, storedEscapedCellPrefix) {
		return storedEscapedCellPrefix + value
	}
	return value
}

func loadStoredCell(cell string) (value string, isNull bool) {
	if cell == storedNullCell {
		return "", true
	}
	if strings.HasPrefix(cell, storedEscapedCellPrefix) {
		return strings.TrimPrefix(cell, storedEscapedCellPrefix), false
	}
	return cell, false
}

func storedCellIsNull(cell string) bool {
	return cell == storedNullCell
}

// validateStoredRows enforces REQUIRED recursively before rows enter the
// catalog. Missing and explicit-null fields share BigQuery's SQL NULL
// semantics; empty strings and numeric/boolean zero values remain present.
func validateStoredRows(schema []tableField, rows [][]string) error {
	for rowIndex, row := range rows {
		for columnIndex, field := range schema {
			cell := storedNullCell
			if columnIndex < len(row) {
				cell = row[columnIndex]
			}
			if err := validateStoredField(field, cell); err != nil {
				return fmt.Errorf("row %d column %q: %w", rowIndex+1, field.Name, err)
			}
		}
	}
	return nil
}

func validateStoredField(field tableField, cell string) error {
	raw, isNull := loadStoredCell(cell)
	if isNull {
		if normalizeMode(field.Mode) == "REQUIRED" {
			return fmt.Errorf("REQUIRED field is NULL or absent")
		}
		return nil
	}
	if field.Mode != "REPEATED" && !isRecordType(field.Type) {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return fmt.Errorf("invalid stored nested JSON: %w", err)
	}
	return validateDecodedField(field, decoded)
}

func validateDecodedField(field tableField, value any) error {
	if value == nil {
		if normalizeMode(field.Mode) == "REQUIRED" {
			return fmt.Errorf("REQUIRED field is NULL or absent")
		}
		return nil
	}
	if field.Mode == "REPEATED" {
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("REPEATED field is not an array")
		}
		base := field
		base.Mode = "NULLABLE"
		for i, item := range items {
			if item == nil {
				return fmt.Errorf("REPEATED element %d is NULL", i)
			}
			if err := validateDecodedField(base, item); err != nil {
				return fmt.Errorf("REPEATED element %d: %w", i, err)
			}
		}
		return nil
	}
	if isRecordType(field.Type) {
		obj, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("RECORD field is not an object")
		}
		for _, sub := range field.Fields {
			if err := validateDecodedField(sub, obj[sub.Name]); err != nil {
				return fmt.Errorf("nested field %q: %w", sub.Name, err)
			}
		}
	}
	return nil
}
