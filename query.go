// query.go - Query execution helpers
package fsql

import (
	"context"
	"fmt"
)

// Insert executes an INSERT query and scans the RETURNING value
func Insert(ctx context.Context, tableName string, values map[string]interface{}, returning string) error {
	query, args := GetInsertQuery(tableName, values, returning)

	if returning != "" {
		// Scan RETURNING value back into the values map
		var returnValue interface{}
		err := DB.QueryRow(ctx, query, args...).Scan(&returnValue)
		if err != nil {
			return fmt.Errorf("insert failed: %w", err)
		}

		// Store returned value in values map
		values[returning] = returnValue
		return nil
	}

	// No RETURNING clause - just exec
	_, err := DB.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	return nil
}

// Update executes an UPDATE query and scans the RETURNING value
func Update(ctx context.Context, tableName string, values map[string]interface{}, returning string) error {
	query, args := GetUpdateQuery(tableName, values, returning)

	if returning != "" {
		// Scan RETURNING value back into the values map
		var returnValue interface{}
		err := DB.QueryRow(ctx, query, args...).Scan(&returnValue)
		if err != nil {
			return fmt.Errorf("update failed: %w", err)
		}

		// Store returned value in values map
		values[returning] = returnValue
		return nil
	}

	// No RETURNING clause - just exec
	_, err := DB.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

// SelectOne executes a query and scans a single row into dest
func SelectOne(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := DB.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	return StructScan(rows, dest)
}

// SelectMany executes a query and scans multiple rows into dest (must be pointer to slice)
func SelectMany(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := DB.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	return StructsScan(rows, dest)
}

// Exec executes a query without returning rows
func Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := DB.Exec(ctx, query, args...)
	return err
}

// QueryRow executes a query that returns a single row
// Returns pgx.Row for custom scanning
func QueryRow(ctx context.Context, query string, args ...interface{}) interface{} {
	return DB.QueryRow(ctx, query, args...)
}
