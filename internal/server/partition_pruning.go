package server

import (
	"strconv"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
)

// prunePartitionedRows performs conservative logical partition pruning for a
// read-only query. It only acts when the table occurs exactly once and a WHERE
// expression proves partition_column = typed_literal, optionally as one term
// of an AND. Unsupported/ambiguous forms deliberately return the full table.
func prunePartitionedRows(queryText, projectID string, ref datasetTableRef, table *tableRecord, rows [][]string) ([][]string, []string, bool) {
	if table == nil || table.View != nil || table.External != nil || (table.TimePartitioning == nil && table.RangePartitioning == nil) {
		return rows, nil, false
	}
	statement, err := parseGoogleSQLStatement(queryText)
	if err != nil {
		return rows, nil, false
	}

	occurrences := 0
	var whereExpressions []googlesql.ASTExpressionNode
	if err := walkGoogleSQLAST(statement, func(node googlesql.ASTNode) error {
		switch typed := node.(type) {
		case *googlesql.ASTTablePathExpression:
			path, err := typed.PathExpr()
			if err != nil || path == nil {
				return err
			}
			parts, err := path.ToIdentifierVector()
			if err != nil {
				return err
			}
			seen := map[datasetTableRef]bool{}
			var found []datasetTableRef
			appendDatasetTableRef(parts, projectID, seen, &found)
			if len(found) == 1 && found[0] == ref {
				occurrences++
			}
		case *googlesql.ASTWhereClause:
			expression, err := typed.Expression()
			if err != nil {
				return err
			}
			if expression != nil {
				whereExpressions = append(whereExpressions, expression)
			}
		}
		return nil
	}); err != nil || occurrences != 1 {
		return rows, nil, false
	}

	var targetIDs map[string]bool
	foundConstraint := false
	for _, expression := range whereExpressions {
		ids, found := partitionEqualityTargets(expression, table)
		if !found {
			continue
		}
		if !foundConstraint {
			targetIDs = ids
			foundConstraint = true
			continue
		}
		for id := range targetIDs {
			if !ids[id] {
				delete(targetIDs, id)
			}
		}
	}
	if !foundConstraint {
		return rows, nil, false
	}

	keptRows := make([][]string, 0, len(rows))
	keptPartitions := make([]string, 0, len(rows))
	for rowIndex, row := range rows {
		partitionID, ok := rowPartitionID(table, rowIndex, row)
		if !ok || !targetIDs[partitionID] {
			continue
		}
		keptRows = append(keptRows, row)
		if table.TimePartitioning != nil && table.TimePartitioning.Field == "" {
			keptPartitions = append(keptPartitions, partitionID)
		}
	}
	return keptRows, keptPartitions, true
}

// partitionEqualityTargets returns the partition IDs proven by equality
// predicates in an AND tree. OR is never pruned: retaining a single branch
// would be incorrect when another branch admits rows from other partitions.
func partitionEqualityTargets(expression googlesql.ASTExpressionNode, table *tableRecord) (map[string]bool, bool) {
	switch typed := expression.(type) {
	case *googlesql.ASTOrExpr:
		return nil, false
	case *googlesql.ASTAndExpr:
		children, err := typed.NumChildren()
		if err != nil {
			return nil, false
		}
		var intersection map[string]bool
		found := false
		for i := int32(0); i < children; i++ {
			child, err := typed.Child(i)
			if err != nil {
				return nil, false
			}
			childExpression, ok := child.(googlesql.ASTExpressionNode)
			if !ok {
				continue
			}
			ids, childFound := partitionEqualityTargets(childExpression, table)
			if !childFound {
				continue
			}
			if !found {
				intersection = ids
				found = true
				continue
			}
			for id := range intersection {
				if !ids[id] {
					delete(intersection, id)
				}
			}
		}
		return intersection, found
	case *googlesql.ASTBinaryExpression:
		op, err := typed.Op()
		if err != nil || op != googlesql.ASTBinaryExpressionEnums_OpEq {
			return nil, false
		}
		lhs, err := typed.Lhs()
		if err != nil {
			return nil, false
		}
		rhs, err := typed.Rhs()
		if err != nil {
			return nil, false
		}
		if id, ok := partitionTargetFromComparison(lhs, rhs, table); ok {
			return map[string]bool{id: true}, true
		}
		if id, ok := partitionTargetFromComparison(rhs, lhs, table); ok {
			return map[string]bool{id: true}, true
		}
	}
	return nil, false
}

func partitionTargetFromComparison(columnNode, literalNode googlesql.ASTExpressionNode, table *tableRecord) (string, bool) {
	path, ok := columnNode.(*googlesql.ASTPathExpression)
	if !ok {
		return "", false
	}
	parts, err := path.ToIdentifierVector()
	if err != nil || len(parts) == 0 {
		return "", false
	}
	column := parts[len(parts)-1]

	if table.RangePartitioning != nil && strings.EqualFold(column, table.RangePartitioning.Field) {
		literal, ok := literalNode.(*googlesql.ASTIntLiteral)
		if !ok {
			return "", false
		}
		image, err := literal.Image()
		if err != nil {
			return "", false
		}
		value, err := strconv.ParseInt(image, 10, 64)
		if err != nil {
			return "", false
		}
		config := table.RangePartitioning
		if value < config.Start || value >= config.End {
			return "__UNPARTITIONED__", true
		}
		bucket := config.Start + ((value-config.Start)/config.Interval)*config.Interval
		return strconv.FormatInt(bucket, 10), true
	}

	if table.TimePartitioning == nil {
		return "", false
	}
	fieldType := ""
	switch {
	case table.TimePartitioning.Field == "" && strings.EqualFold(column, "_PARTITIONDATE"):
		fieldType = "DATE"
	case table.TimePartitioning.Field == "" && strings.EqualFold(column, "_PARTITIONTIME"):
		fieldType = "TIMESTAMP"
	case table.TimePartitioning.Field != "" && strings.EqualFold(column, table.TimePartitioning.Field):
		field, _, found := findTopLevelField(table.Schema, table.TimePartitioning.Field)
		if !found {
			return "", false
		}
		fieldType = field.Type
	default:
		return "", false
	}
	literal, ok := literalNode.(*googlesql.ASTDateOrTimeLiteral)
	if !ok {
		return "", false
	}
	stringLiteral, err := literal.StringLiteral()
	if err != nil || stringLiteral == nil {
		return "", false
	}
	value, err := stringLiteral.StringValue()
	if err != nil {
		return "", false
	}
	return timePartitionIDFromCell(fieldType, storeStringCell(value), table.TimePartitioning.Type)
}

func rowPartitionID(table *tableRecord, rowIndex int, row []string) (string, bool) {
	switch {
	case table.TimePartitioning != nil && table.TimePartitioning.Field == "":
		partitionID := ingestionPartitionID(table.TimePartitioning, table.CreatedAt)
		if rowIndex < len(table.IngestionPartitions) && table.IngestionPartitions[rowIndex] != "" {
			partitionID = table.IngestionPartitions[rowIndex]
		}
		return partitionID, partitionID != ""
	case table.TimePartitioning != nil:
		field, index, found := findTopLevelField(table.Schema, table.TimePartitioning.Field)
		if !found {
			return "", false
		}
		cell := storedNullCell
		if index < len(row) {
			cell = row[index]
		}
		if _, isNull := loadStoredCell(cell); isNull {
			return "__NULL__", true
		}
		partitionID, ok := timePartitionIDFromCell(field.Type, cell, table.TimePartitioning.Type)
		if !ok {
			return "__UNPARTITIONED__", true
		}
		return partitionID, true
	case table.RangePartitioning != nil:
		_, index, found := findTopLevelField(table.Schema, table.RangePartitioning.Field)
		if !found {
			return "", false
		}
		cell := storedNullCell
		if index < len(row) {
			cell = row[index]
		}
		decoded, isNull := loadStoredCell(cell)
		if isNull {
			return "__NULL__", true
		}
		value, err := strconv.ParseInt(decoded, 10, 64)
		config := table.RangePartitioning
		if err != nil || value < config.Start || value >= config.End {
			return "__UNPARTITIONED__", true
		}
		bucket := config.Start + ((value-config.Start)/config.Interval)*config.Interval
		return strconv.FormatInt(bucket, 10), true
	}
	return "", false
}

func queryHasPartitionFilterAST(queryText string, table *tableRecord) (bool, error) {
	statement, err := parseGoogleSQLStatement(queryText)
	if err != nil {
		return false, err
	}
	columns := partitionFilterColumns(table)
	found := false
	err = walkGoogleSQLAST(statement, func(node googlesql.ASTNode) error {
		if found {
			return nil
		}
		var expression googlesql.ASTExpressionNode
		var err error
		switch typed := node.(type) {
		case *googlesql.ASTWhereClause:
			expression, err = typed.Expression()
		case *googlesql.ASTDeleteStatement:
			expression, err = typed.Where()
		case *googlesql.ASTUpdateStatement:
			expression, err = typed.Where()
		default:
			return nil
		}
		if err != nil || expression == nil {
			return err
		}
		return walkGoogleSQLAST(expression, func(child googlesql.ASTNode) error {
			path, ok := child.(*googlesql.ASTPathExpression)
			if !ok {
				return nil
			}
			parts, err := path.ToIdentifierVector()
			if err != nil || len(parts) == 0 {
				return err
			}
			for _, column := range columns {
				if strings.EqualFold(parts[len(parts)-1], column) {
					found = true
					break
				}
			}
			return nil
		})
	})
	return found, err
}

func partitionFilterColumns(table *tableRecord) []string {
	var columns []string
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
	return columns
}
