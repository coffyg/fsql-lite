// scanner.go - Uses sqlx's reflectx for efficient struct scanning with pgx
package fsql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx/reflectx"
)

// pgxScannerWrapper wraps sql.Scanner to handle pgx's string returns for JSONB
// pgx returns string for JSONB in text mode, but sqlx-compatible Scanners expect []byte
type pgxScannerWrapper struct {
	target sql.Scanner
}

func (p *pgxScannerWrapper) Scan(value interface{}) error {
	if value == nil {
		return p.target.Scan(nil)
	}
	// pgx returns string for JSONB/text types, convert to []byte for compatibility
	if str, ok := value.(string); ok {
		return p.target.Scan([]byte(str))
	}
	return p.target.Scan(value)
}

// traversalCache caches traversals per (type, columns) combination
type traversalCache struct {
	traversals [][]int
	hasScanner []bool // true if field implements sql.Scanner
}

var (
	// traversalCacheMap maps "typeName:col1,col2,col3" → cached traversals
	traversalCacheMap  = make(map[string]*traversalCache)
	traversalCacheLock sync.RWMutex
)

// Global mapper using "db" tag (same as sqlx)
// Uses strings.ToLower so field "UUID" matches column "uuid"
var mapper = reflectx.NewMapperFunc("db", strings.ToLower)

// getColumns extracts column names from pgx FieldDescriptions
func getColumns(rows pgx.Rows) []string {
	fds := rows.FieldDescriptions()
	columns := make([]string, len(fds))
	for i, fd := range fds {
		columns[i] = string(fd.Name)
	}
	return columns
}

// ScanRows scans pgx rows into a destination (struct or slice)
func ScanRows(rows pgx.Rows, dest interface{}) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer")
	}

	direct := reflect.Indirect(value)
	columns := getColumns(rows)

	// Handle slice vs single struct
	if direct.Kind() == reflect.Slice {
		return scanSlice(rows, direct, columns)
	}
	return scanSingle(rows, direct, columns)
}

// scanSlice scans rows into a slice destination
func scanSlice(rows pgx.Rows, slice reflect.Value, columns []string) error {
	sliceType := slice.Type().Elem()
	isPtr := sliceType.Kind() == reflect.Ptr

	baseType := sliceType
	if isPtr {
		baseType = sliceType.Elem()
	}

	// Handle primitive slices
	if baseType.Kind() != reflect.Struct {
		for rows.Next() {
			vp := reflect.New(baseType)
			if err := rows.Scan(vp.Interface()); err != nil {
				return err
			}
			if isPtr {
				slice.Set(reflect.Append(slice, vp))
			} else {
				slice.Set(reflect.Append(slice, vp.Elem()))
			}
		}
		return rows.Err()
	}

	// Get field traversals and scanner flags ONCE for the type
	tm := mapper.TypeMap(baseType)
	traversals, hasScanner := getTraversalsAndScanners(tm, baseType, columns)

	// Reusable values slice for scanning
	values := make([]interface{}, len(columns))

	for rows.Next() {
		vp := reflect.New(baseType)
		v := vp.Elem()

		// Set up scan destinations using reflectx field traversals
		if err := setupScanDests(v, columns, traversals, hasScanner, values); err != nil {
			return err
		}

		if err := rows.Scan(values...); err != nil {
			return err
		}

		if isPtr {
			slice.Set(reflect.Append(slice, vp))
		} else {
			slice.Set(reflect.Append(slice, v))
		}
	}
	return rows.Err()
}

// scanSingle scans a single row into a struct
func scanSingle(rows pgx.Rows, dest reflect.Value, columns []string) error {
	// Handle primitives (non-structs)
	if dest.Kind() != reflect.Struct {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return sql.ErrNoRows
		}
		return rows.Scan(dest.Addr().Interface())
	}

	// Handle sql.Scanner types (like sql.NullInt64, sql.NullString, etc.)
	// These are structs but should be scanned directly, not via field mapping
	if scanner, ok := dest.Addr().Interface().(sql.Scanner); ok {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return sql.ErrNoRows
		}
		// Use pgx's scan which will call the Scanner interface
		return rows.Scan(scanner)
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	tm := mapper.TypeMap(dest.Type())
	traversals, hasScanner := getTraversalsAndScanners(tm, dest.Type(), columns)
	values := make([]interface{}, len(columns))

	if err := setupScanDests(dest, columns, traversals, hasScanner, values); err != nil {
		return err
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	if rows.Next() {
		return errors.New("query returned multiple rows for a single destination")
	}
	return rows.Err()
}

// getTraversalsAndScanners gets field traversals and scanner flags for columns (cached)
func getTraversalsAndScanners(tm *reflectx.StructMap, baseType reflect.Type, columns []string) ([][]int, []bool) {
	// Build cache key: "typeName:col1,col2,col3..."
	// Using a simple string concatenation for the key
	keyLen := len(baseType.String()) + 1
	for _, col := range columns {
		keyLen += len(col) + 1
	}

	var keyBuilder strings.Builder
	keyBuilder.Grow(keyLen)
	keyBuilder.WriteString(baseType.String())
	keyBuilder.WriteByte(':')
	for i, col := range columns {
		if i > 0 {
			keyBuilder.WriteByte(',')
		}
		keyBuilder.WriteString(col)
	}
	cacheKey := keyBuilder.String()

	// Check cache with read lock
	traversalCacheLock.RLock()
	if cached, ok := traversalCacheMap[cacheKey]; ok {
		traversalCacheLock.RUnlock()
		return cached.traversals, cached.hasScanner
	}
	traversalCacheLock.RUnlock()

	// Not in cache, compute and store
	traversals := make([][]int, len(columns))
	hasScanner := make([]bool, len(columns))

	for i, col := range columns {
		fi := tm.GetByPath(col)
		if fi == nil {
			continue
		}
		traversals[i] = fi.Index

		// Check if field type implements sql.Scanner
		fieldType := fi.Field.Type
		if fieldType.Kind() == reflect.Ptr {
			// *T implements Scanner if *T has Scan method
			if fieldType.Implements(scannerType) {
				hasScanner[i] = true
			}
		} else {
			// T implements Scanner if *T has Scan method (pointer receiver)
			if reflect.PtrTo(fieldType).Implements(scannerType) {
				hasScanner[i] = true
			}
		}
	}

	// Store in cache with write lock
	traversalCacheLock.Lock()
	traversalCacheMap[cacheKey] = &traversalCache{
		traversals: traversals,
		hasScanner: hasScanner,
	}
	traversalCacheLock.Unlock()

	return traversals, hasScanner
}

// sql.Scanner type for interface check
var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// setupScanDests sets up scan destinations using field traversals
func setupScanDests(v reflect.Value, columns []string, traversals [][]int, hasScanner []bool, values []interface{}) error {
	for i, traversal := range traversals {
		if traversal == nil {
			// Column doesn't map to a field - use placeholder
			var placeholder interface{}
			values[i] = &placeholder
			continue
		}

		// Navigate through the traversal, initializing nil pointers along the way
		f := v
		for _, idx := range traversal {
			if f.Kind() == reflect.Ptr {
				if f.IsNil() {
					f.Set(reflect.New(f.Type().Elem()))
				}
				f = f.Elem()
			}
			f = f.Field(idx)
		}

		// Wrap sql.Scanner types to handle pgx's string→[]byte conversion for JSONB
		if hasScanner[i] {
			// For pointer fields (*JSONSettings), the Scanner interface is on the pointer type
			// Initialize if nil, then use the pointer value directly as the scanner
			if f.Kind() == reflect.Ptr {
				if f.IsNil() {
					f.Set(reflect.New(f.Type().Elem()))
				}
				if scanner, ok := f.Interface().(sql.Scanner); ok {
					values[i] = &pgxScannerWrapper{target: scanner}
					continue
				}
			} else {
				// For non-pointer fields, get address and check for Scanner
				ptr := f.Addr().Interface()
				if scanner, ok := ptr.(sql.Scanner); ok {
					values[i] = &pgxScannerWrapper{target: scanner}
					continue
				}
			}
		}

		values[i] = f.Addr().Interface()
	}
	return nil
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

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer")
	}

	direct := reflect.Indirect(v)
	columns := getColumns(rows)

	tm := mapper.TypeMap(direct.Type())
	traversals, hasScanner := getTraversalsAndScanners(tm, direct.Type(), columns)
	values := make([]interface{}, len(columns))

	if err := setupScanDests(direct, columns, traversals, hasScanner, values); err != nil {
		return err
	}

	if err := rows.Scan(values...); err != nil {
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

func (ns NullableString) Value() (interface{}, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}

func (ns *NullableString) Scan(value interface{}) error {
	return ns.NullString.Scan(value)
}

// ResetScannerCache clears mapper cache (if needed)
func ResetScannerCache() {
	mapper = reflectx.NewMapperFunc("db", func(s string) string { return s })
}
