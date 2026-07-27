package server

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// storedQueryParameter is a BigQuery queryParameters entry (jobs.query /
// jobs.insert configuration.query), reduced to plain strings for the same
// reason every table cell in this project is: it needs to round-trip
// through jobRecord's JSON persistence (see jobs_service.go persistLocked)
// without a Go `any` field silently changing shape (e.g. []byte becoming a
// base64 string) across a marshal/unmarshal cycle. The real typed Go value
// is only materialized right before binding, in buildQueryArgs.
type storedQueryParameter struct {
	Name   string `json:"name,omitempty"`
	Type   string `json:"type"`
	Value  string `json:"value"`
	IsNull bool   `json:"isNull"`
}

// scalarQueryParameterTypes are the parameterType.type values this project
// binds to a real Go value today. NUMERIC/BIGNUMERIC are deliberately
// excluded: the driver types an untyped bind value from its Go type alone
// (see queryParameterValueForType), and a Go string parameter always infers
// as STRING rather than NUMERIC/BIGNUMERIC (verified empirically — CAST(@a
// AS NUMERIC) would fix it, but rewriting arbitrary client SQL text to
// inject a CAST around every parameter reference is exactly the kind of SQL
// parsing this project avoids). ARRAY/STRUCT/GEOGRAPHY/RANGE/JSON/INTERVAL
// are out of scope for the same reason nested/nested-adjacent types are
// elsewhere in this codebase — rejected explicitly, not silently mishandled.
var scalarQueryParameterTypes = map[string]bool{
	"STRING": true, "INT64": true, "FLOAT64": true, "BOOL": true, "BOOLEAN": true,
	"BYTES": true, "DATE": true, "DATETIME": true, "TIME": true, "TIMESTAMP": true,
}

// parseQueryParametersFromRaw converts the raw JSON shape of a BigQuery
// queryParameters array (each entry: {"name"?, "parameterType": {"type":
// ...}, "parameterValue": {"value"?: ...}}) into storedQueryParameter. mode
// must be "NAMED" or "POSITIONAL" (case-insensitive); if empty, it's
// inferred from whether the first parameter carries a name.
func parseQueryParametersFromRaw(mode string, rawParams []any) ([]storedQueryParameter, string, error) {
	if len(rawParams) == 0 {
		return nil, mode, nil
	}
	out := make([]storedQueryParameter, 0, len(rawParams))
	for i, rawParam := range rawParams {
		paramMap, ok := rawParam.(map[string]any)
		if !ok {
			return nil, mode, fmt.Errorf("queryParameters[%d]: expected an object", i)
		}
		name, _ := paramMap["name"].(string)
		typeMap, _ := paramMap["parameterType"].(map[string]any)
		typeName, _ := typeMap["type"].(string)
		typeName = strings.ToUpper(strings.TrimSpace(typeName))
		if typeName == "" {
			return nil, mode, fmt.Errorf("queryParameters[%d]: missing parameterType.type", i)
		}
		if !scalarQueryParameterTypes[typeName] {
			return nil, mode, fmt.Errorf("queryParameters[%d]: parameterType %q is not supported (only scalar types are: STRING, INT64, FLOAT64, BOOL, BYTES, DATE, DATETIME, TIME, TIMESTAMP)", i, typeName)
		}
		valueMap, _ := paramMap["parameterValue"].(map[string]any)
		rawValue, hasValue := valueMap["value"]
		sp := storedQueryParameter{Name: name, Type: typeName}
		if !hasValue || rawValue == nil {
			sp.IsNull = true
		} else if s, ok := rawValue.(string); ok {
			sp.Value = s
		} else {
			// BigQuery's own wire format always sends parameterValue.value
			// as a string, even for numeric-looking types; tolerate a raw
			// JSON number/bool too rather than rejecting a client that sent
			// one unquoted.
			sp.Value = fmt.Sprintf("%v", rawValue)
		}
		out = append(out, sp)
	}
	if mode == "" {
		if out[0].Name != "" {
			mode = "NAMED"
		} else {
			mode = "POSITIONAL"
		}
	}
	return out, strings.ToUpper(strings.TrimSpace(mode)), nil
}

// buildQueryArgs converts parsed query parameters into args for db.Query,
// binding NULL as a plain untyped nil (never a typed nil pointer — verified
// empirically that this driver panics on e.g. a nil *int64) and every other
// scalar as the Go type that lets the analyzer infer the matching GoogleSQL
// type from the bound value itself (int64/float64/bool/[]byte/string).
func buildQueryArgs(mode string, params []storedQueryParameter) ([]any, error) {
	if len(params) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(params))
	for i, p := range params {
		v, err := queryParameterValueForType(p)
		if err != nil {
			return nil, fmt.Errorf("queryParameters[%d]: %w", i, err)
		}
		if mode == "POSITIONAL" {
			args = append(args, v)
			continue
		}
		if p.Name == "" {
			return nil, fmt.Errorf("queryParameters[%d]: NAMED parameter mode requires a name", i)
		}
		args = append(args, sql.Named(p.Name, v))
	}
	return args, nil
}

// queryParameterValueForType converts one stored parameter's raw string
// value into the Go value that binds it as the matching GoogleSQL scalar
// type. DATE/DATETIME/TIME/TIMESTAMP pass through as plain strings,
// consistent with how those columns are already stored/rendered everywhere
// else in this codebase.
func queryParameterValueForType(p storedQueryParameter) (any, error) {
	if p.IsNull {
		return nil, nil
	}
	switch p.Type {
	case "INT64":
		n, err := strconv.ParseInt(p.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid INT64 value %q: %w", p.Value, err)
		}
		return n, nil
	case "FLOAT64":
		f, err := strconv.ParseFloat(p.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLOAT64 value %q: %w", p.Value, err)
		}
		return f, nil
	case "BOOL", "BOOLEAN":
		b, err := strconv.ParseBool(p.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid BOOL value %q: %w", p.Value, err)
		}
		return b, nil
	case "BYTES":
		decoded, err := base64.StdEncoding.DecodeString(p.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid BYTES value %q: expected base64: %w", p.Value, err)
		}
		return decoded, nil
	default: // STRING, DATE, DATETIME, TIME, TIMESTAMP
		return p.Value, nil
	}
}
