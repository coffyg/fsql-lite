// scanner.go - pgx.Rows to struct scanning
package fsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
)

// StructScan scans a single row from pgx.Rows into a struct
// Note: This only works with Rows, not Row (from QueryRow)
// For QueryRow, manually call .Scan() with the fields you need
func StructScan(rows pgx.Rows, dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer to a struct")
	}

	destValue = destValue.Elem()
	if destValue.Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to a struct")
	}

	// Get field descriptions from rows
	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) == 0 {
		return fmt.Errorf("no field descriptions available")
	}

	// Build column name to field mapping
	columnMap := make(map[string]reflect.Value, len(fieldDescriptions))
	destType := destValue.Type()

	for i := 0; i < destValue.NumField(); i++ {
		field := destType.Field(i)
		dbTag := field.Tag.Get("db")

		if dbTag != "" && dbTag != "-" {
			// Map column name to field
			columnMap[dbTag] = destValue.Field(i)
		}
	}

	// Create scan targets for each column in order
	scanTargets := make([]interface{}, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columnName := string(fd.Name)

		if fieldValue, exists := columnMap[columnName]; exists && fieldValue.CanSet() {
			// Point to the actual field
			scanTargets[i] = fieldValue.Addr().Interface()
		} else {
			// Discard unknown columns
			var discard interface{}
			scanTargets[i] = &discard
		}
	}

	// Advance to first row and scan
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return fmt.Errorf("no rows to scan")
	}

	return rows.Scan(scanTargets...)
}

// StructsScan scans multiple pgx.Rows into a slice of structs
func StructsScan(rows pgx.Rows, dest interface{}) error {
	defer rows.Close()

	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer to a slice")
	}

	destValue = destValue.Elem()
	if destValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to a slice")
	}

	// Get the element type of the slice
	elemType := destValue.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to a slice of structs")
	}

	// Get field descriptions once
	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) == 0 {
		return nil // No fields, nothing to scan
	}

	// Build column name to field index mapping using the element type
	columnToFieldIndex := make(map[string]int, len(fieldDescriptions))

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")

		if dbTag != "" && dbTag != "-" {
			columnToFieldIndex[dbTag] = i
		}
	}

	// Process each row
	for rows.Next() {
		// Create new struct instance
		elemValue := reflect.New(elemType).Elem()

		// Create scan targets for this row
		scanTargets := make([]interface{}, len(fieldDescriptions))
		for i, fd := range fieldDescriptions {
			columnName := string(fd.Name)

			if fieldIndex, exists := columnToFieldIndex[columnName]; exists {
				fieldValue := elemValue.Field(fieldIndex)
				if fieldValue.CanSet() {
					scanTargets[i] = fieldValue.Addr().Interface()
				} else {
					var discard interface{}
					scanTargets[i] = &discard
				}
			} else {
				// Discard unknown columns
				var discard interface{}
				scanTargets[i] = &discard
			}
		}

		// Scan the row
		if err := rows.Scan(scanTargets...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Append to slice
		if isPtr {
			destValue.Set(reflect.Append(destValue, elemValue.Addr()))
		} else {
			destValue.Set(reflect.Append(destValue, elemValue))
		}
	}

	return rows.Err()
}

// Get scans a single row using a raw SQL query
func Get(dest interface{}, query string, args ...interface{}) error {
	rows, err := DB.Query(context.Background(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return StructScan(rows, dest)
}

// Select scans multiple rows using a raw SQL query
func Select(dest interface{}, query string, args ...interface{}) error {
	rows, err := DB.Query(context.Background(), query, args...)
	if err != nil {
		return err
	}
	return StructsScan(rows, dest)
}

// NullableString is a helper for scanning nullable strings
type NullableString struct {
	sql.NullString
}

// Value implements driver.Valuer
func (ns NullableString) Value() (interface{}, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}

// Scan implements sql.Scanner
func (ns *NullableString) Scan(value interface{}) error {
	return ns.NullString.Scan(value)
}
