// scanner.go - Adapted from original fsql scanner for pgx
package fsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/jackc/pgx/v5"
)

// ColumnCache for pre-computed column mapping info
type ColumnCache struct {
	columnIndexes map[string]int
	fieldIndices  [][]int
	fieldPointers []interface{}
	initialized   bool
}

// Pool for column caches based on query patterns
var columnCachePool = sync.Pool{
	New: func() interface{} {
		return &ColumnCache{
			columnIndexes: make(map[string]int, 20),
			fieldIndices:  make([][]int, 0, 20),
			fieldPointers: make([]interface{}, 0, 20),
		}
	},
}

// Column cache map for faster lookups
var columnCacheMap sync.Map

// getColumns extracts column names from pgx FieldDescriptions
func getColumns(rows pgx.Rows) []string {
	fds := rows.FieldDescriptions()
	columns := make([]string, len(fds))
	for i, fd := range fds {
		columns[i] = string(fd.Name)
	}
	return columns
}

// ScanRows efficiently scans rows into a destination struct or slice
// This is adapted from the original fsql scanner
func ScanRows(rows pgx.Rows, dest interface{}) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer")
	}

	// Get the pointed-to value
	direct := reflect.Indirect(value)

	// Get column names
	columns := getColumns(rows)

	// Create a fingerprint of the columns to use as cache key
	columnFingerprint := getColumnFingerprint(columns)

	// Check if we have a cached scanner for this column set
	var cache *ColumnCache
	cacheEntry, found := columnCacheMap.Load(columnFingerprint)
	if found {
		cache = cacheEntry.(*ColumnCache)
	} else {
		// Create a new cache entry
		cache = columnCachePool.Get().(*ColumnCache)
		cache.columnIndexes = make(map[string]int, len(columns))
		cache.fieldIndices = make([][]int, len(columns))
		cache.fieldPointers = make([]interface{}, len(columns))
		cache.initialized = false

		// Store in cache for future use
		columnCacheMap.Store(columnFingerprint, cache)
	}

	// For slice destinations, handle differently
	if direct.Kind() == reflect.Slice {
		sliceType := direct.Type().Elem()
		isPtr := sliceType.Kind() == reflect.Ptr

		// Get the base element type
		baseType := sliceType
		if isPtr {
			baseType = sliceType.Elem()
		}

		// Check if this is a slice of primitives (not structs)
		if baseType.Kind() != reflect.Struct {
			// Simple scan for primitive types (string, int, etc)
			for rows.Next() {
				rowDest := reflect.New(baseType)
				if err := rows.Scan(rowDest.Interface()); err != nil {
					return err
				}
				if isPtr {
					direct.Set(reflect.Append(direct, rowDest))
				} else {
					direct.Set(reflect.Append(direct, rowDest.Elem()))
				}
			}
			return rows.Err()
		}

		for rows.Next() {
			// Create a new item
			var rowDest reflect.Value
			if isPtr {
				rowDest = reflect.New(sliceType.Elem())
			} else {
				rowDest = reflect.New(sliceType)
			}

			// Initialize cache if needed
			if !cache.initialized {
				err := initializeCache(cache, columns, rowDest)
				if err != nil {
					return err
				}
			}

			// Scan into the row destination using cached mapping
			err := scanWithCache(cache, rows)
			if err != nil {
				return err
			}

			// Append to the result slice
			if isPtr {
				direct.Set(reflect.Append(direct, rowDest))
			} else {
				direct.Set(reflect.Append(direct, reflect.Indirect(rowDest)))
			}
		}

		return rows.Err()
	} else {
		// For a single destination
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return sql.ErrNoRows
		}

		// Check if this is a primitive type (not a struct)
		if direct.Kind() != reflect.Struct {
			// Simple scan for primitive types (string, int, *string, etc)
			if err := rows.Scan(dest); err != nil {
				return err
			}
			return rows.Err()
		}

		// Initialize cache if needed
		if !cache.initialized {
			err := initializeCache(cache, columns, value)
			if err != nil {
				return err
			}
		}

		// Scan into destination using cached mapping
		err := scanWithCache(cache, rows)
		if err != nil {
			return err
		}

		// Check if there are more rows (which would be unexpected)
		if rows.Next() {
			return errors.New("query returned multiple rows for a single destination")
		}

		return rows.Err()
	}
}

// Initialize the column mapping cache
func initializeCache(cache *ColumnCache, columns []string, value reflect.Value) error {
	// Get the pointed-to value
	elem := reflect.Indirect(value)
	elemType := elem.Type()

	// Get field mapping
	fieldMap := getFieldMap(elemType)

	// Create column-to-field mapping
	for i, colName := range columns {
		cache.columnIndexes[colName] = i

		// Find field indices for this column
		fieldPath, ok := fieldMap[colName]
		if !ok {
			// If column doesn't map to a field, use a placeholder
			var placeholder interface{}
			cache.fieldPointers[i] = &placeholder
			continue
		}

		// Store field indices for this column
		cache.fieldIndices[i] = fieldPath

		// Create scanner pointer for this field
		field := elem
		for _, idx := range fieldPath {
			field = field.Field(idx)
			if field.Kind() == reflect.Ptr && field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			if field.Kind() == reflect.Ptr {
				field = field.Elem()
			}
		}

		// Create a suitable destination pointer based on field type
		cache.fieldPointers[i] = getDestPtr(field)
	}

	cache.initialized = true
	return nil
}

// Scan a row using the cached mapping
func scanWithCache(cache *ColumnCache, rows pgx.Rows) error {
	return rows.Scan(cache.fieldPointers...)
}

// Get a field map for a given type
var fieldMapCache sync.Map

func getFieldMap(t reflect.Type) map[string][]int {
	// Check if we already have this type in cache
	if cached, ok := fieldMapCache.Load(t); ok {
		return cached.(map[string][]int)
	}

	fieldMap := make(map[string][]int)

	// Create field map by recursively analyzing the type
	buildFieldMap(t, fieldMap, nil)

	// Store in cache for future use
	fieldMapCache.Store(t, fieldMap)

	return fieldMap
}

// Build a field map by recursively analyzing the type
func buildFieldMap(t reflect.Type, fieldMap map[string][]int, path []int) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Get DB tag
		tag := field.Tag.Get("db")
		if tag == "" || tag == "-" {
			// If this field is a struct, recurse into it
			if field.Type.Kind() == reflect.Struct {
				// Add this field index to the path
				newPath := make([]int, len(path)+1)
				copy(newPath, path)
				newPath[len(path)] = i

				// Recurse
				buildFieldMap(field.Type, fieldMap, newPath)
			}
			continue
		}

		// Create the field path
		fieldPath := make([]int, len(path)+1)
		copy(fieldPath, path)
		fieldPath[len(path)] = i

		// Store the field path
		fieldMap[tag] = fieldPath
	}
}

// Create a destination pointer for a field
func getDestPtr(field reflect.Value) interface{} {
	// For pgx, we can just return the address of the field directly
	// pgx handles type conversion internally
	return field.Addr().Interface()
}

// Generate a fingerprint for a set of columns
func getColumnFingerprint(columns []string) string {
	if len(columns) < 5 {
		fingerprint := ""
		for i, col := range columns {
			if i > 0 {
				fingerprint += "|"
			}
			fingerprint += col
		}
		return fingerprint
	}
	// For larger sets, use the first column plus length as a quick fingerprint
	return columns[0] + "|" + columns[len(columns)-1] + "|" + fmt.Sprintf("%d", len(columns))
}

// StructScan scans a single row from pgx.Rows into a struct
func StructScan(rows pgx.Rows, dest interface{}) error {
	return ScanRows(rows, dest)
}

// StructsScan scans multiple pgx.Rows into a slice of structs
func StructsScan(rows pgx.Rows, dest interface{}) error {
	defer rows.Close()
	return ScanRows(rows, dest)
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

// ScanSingle scans a single row into the destination struct
func ScanSingle(rows pgx.Rows, dest interface{}) error {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	err := ScanRows(rows, dest)
	if err != nil {
		return err
	}

	if rows.Next() {
		return errors.New("query returned multiple rows for a single destination")
	}

	return rows.Err()
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

// ResetScannerCache clears all cached field mappings
func ResetScannerCache() {
	columnCacheMap = sync.Map{}
	fieldMapCache = sync.Map{}
}
