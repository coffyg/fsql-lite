// scanner.go - pgx.Rows to struct scanning
package fsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

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

	// Build column name to field mapping (including nested structs for linked fields)
	columnMap := make(map[string]reflect.Value, len(fieldDescriptions))
	destType := destValue.Type()

	for i := 0; i < destValue.NumField(); i++ {
		field := destType.Field(i)
		dbTag := field.Tag.Get("db")
		dbMode := field.Tag.Get("dbMode")

		if dbTag == "" || dbTag == "-" {
			continue
		}

		// Check if this is a linked field
		if strings.Contains(dbMode, "l") {
			// This is a pointer to a nested struct
			fieldValue := destValue.Field(i)

			// Initialize if nil
			if fieldValue.IsNil() {
				fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
			}

			// Map nested struct fields with prefix
			nestedValue := fieldValue.Elem()
			nestedType := nestedValue.Type()

			for j := 0; j < nestedValue.NumField(); j++ {
				nestedField := nestedType.Field(j)
				nestedDbTag := nestedField.Tag.Get("db")

				if nestedDbTag != "" && nestedDbTag != "-" {
					// Column name is "prefix.fieldname"
					columnName := dbTag + "." + nestedDbTag
					columnMap[columnName] = nestedValue.Field(j)
				}
			}
		} else {
			// Regular field
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

	// Build column name to field index mapping (including nested linked fields)
	type fieldMapping struct {
		indices      []int // Field indices path
		isLinked     bool
		linkedPrefix string
	}
	columnToFieldMap := make(map[string]fieldMapping, len(fieldDescriptions))

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		dbMode := field.Tag.Get("dbMode")

		if dbTag == "" || dbTag == "-" {
			continue
		}

		// Check if this is a linked field
		if strings.Contains(dbMode, "l") {
			// Map nested fields
			nestedType := field.Type.Elem() // Get struct type from pointer
			for j := 0; j < nestedType.NumField(); j++ {
				nestedField := nestedType.Field(j)
				nestedDbTag := nestedField.Tag.Get("db")

				if nestedDbTag != "" && nestedDbTag != "-" {
					columnName := dbTag + "." + nestedDbTag
					columnToFieldMap[columnName] = fieldMapping{
						indices:      []int{i, j},
						isLinked:     true,
						linkedPrefix: dbTag,
					}
				}
			}
		} else {
			// Regular field
			columnToFieldMap[dbTag] = fieldMapping{
				indices:  []int{i},
				isLinked: false,
			}
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

			if mapping, exists := columnToFieldMap[columnName]; exists {
				if mapping.isLinked {
					// Initialize linked struct if needed
					linkedField := elemValue.Field(mapping.indices[0])
					if linkedField.IsNil() {
						linkedField.Set(reflect.New(linkedField.Type().Elem()))
					}
					// Get the nested field
					nestedField := linkedField.Elem().Field(mapping.indices[1])
					if nestedField.CanSet() {
						scanTargets[i] = nestedField.Addr().Interface()
					} else {
						var discard interface{}
						scanTargets[i] = &discard
					}
				} else {
					// Regular field
					fieldValue := elemValue.Field(mapping.indices[0])
					if fieldValue.CanSet() {
						scanTargets[i] = fieldValue.Addr().Interface()
					} else {
						var discard interface{}
						scanTargets[i] = &discard
					}
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
