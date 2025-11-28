// tx_orm.go - Transaction ORM operations for fsql-lite
// Provides object-based transaction methods compatible with original fsql API
package fsql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Custom query step types for transaction query builder
type orderByStep struct {
	Clause string
}

type limitStep struct {
	Limit int64
}

type offsetStep struct {
	Offset int64
}

// InsertObjectWithTx inserts a struct object within a transaction (original fsql signature)
// This uses reflection to extract values from the struct based on dbMode tags
func InsertObjectWithTx(tx *Tx, object interface{}, tableName string) error {
	return InsertObjectWithTxContext(context.Background(), tx, object, tableName)
}

// InsertObjectWithTxContext inserts a struct object within a transaction with context
func InsertObjectWithTxContext(ctx context.Context, tx *Tx, object interface{}, tableName string) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	// Get model tag cache for the object
	objectType := reflect.TypeOf(object)
	if objectType.Kind() == reflect.Ptr {
		objectType = objectType.Elem()
	}

	tagCache, err := getModelTagCache(objectType)
	if err != nil {
		return fmt.Errorf("failed to get model tag cache: %w", err)
	}

	// Build query for insertion
	var columns []string
	var placeholders []string
	var values []interface{}

	// Process insert fields
	for i, field := range tagCache.Fields {
		// Only include insert fields
		if strings.Contains(field.Mode, "i") {
			columns = append(columns, field.DbName)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(values)+1))

			// Get field value
			val := reflect.ValueOf(object)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			fieldVal := val.Field(i).Interface()

			// Apply any value transformation
			if field.InsertValue != "" {
				fieldVal = field.InsertValue
			}

			values = append(values, fieldVal)
		}
	}

	if len(columns) == 0 {
		return fmt.Errorf("no fields marked for insertion")
	}

	// Build and execute query
	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`,
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err = tx.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	return nil
}

// UpdateObjectWithTx updates a struct object within a transaction (original fsql signature)
func UpdateObjectWithTx(tx *Tx, object interface{}, tableName, whereClause string, whereArgs ...interface{}) error {
	return UpdateObjectWithTxContext(context.Background(), tx, object, tableName, whereClause, whereArgs...)
}

// UpdateObjectWithTxContext updates a struct object within a transaction with context
func UpdateObjectWithTxContext(ctx context.Context, tx *Tx, object interface{}, tableName, whereClause string, whereArgs ...interface{}) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	// Get model tag cache for the object
	objectType := reflect.TypeOf(object)
	if objectType.Kind() == reflect.Ptr {
		objectType = objectType.Elem()
	}

	tagCache, err := getModelTagCache(objectType)
	if err != nil {
		return fmt.Errorf("failed to get model tag cache: %w", err)
	}

	// Build query for update
	var setClause []string
	var values []interface{}

	// Process update fields
	for i, field := range tagCache.Fields {
		// Only include update fields
		if strings.Contains(field.Mode, "u") {
			setClause = append(setClause, fmt.Sprintf("%s = $%d", field.DbName, len(values)+1))

			// Get field value
			val := reflect.ValueOf(object)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			fieldVal := val.Field(i).Interface()

			values = append(values, fieldVal)
		}
	}

	if len(setClause) == 0 {
		return fmt.Errorf("no fields marked for update")
	}

	// Add where args to values
	paramOffset := len(values)
	for i, arg := range whereArgs {
		// Replace placeholders in where clause
		whereClause = strings.Replace(whereClause,
			fmt.Sprintf("$%d", i+1),
			fmt.Sprintf("$%d", paramOffset+i+1), 1)
		values = append(values, arg)
	}

	// Build and execute query
	query := fmt.Sprintf(`UPDATE "%s" SET %s WHERE %s`,
		tableName,
		strings.Join(setClause, ", "),
		whereClause)

	_, err = tx.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

// DeleteWithTxCompat deletes records within a transaction (original fsql signature without ctx)
func DeleteWithTxCompat(tx *Tx, tableName, whereClause string, whereArgs ...interface{}) error {
	return DeleteWithTx(context.Background(), tx, tableName, whereClause, whereArgs...)
}

// QueryBuilderWithTx extends query builder with transaction support
type QueryBuilderWithTx struct {
	tx *Tx
	qb *QueryBuilder
}

// NewQueryBuilderWithTx creates a new query builder with transaction support
func NewQueryBuilderWithTx(tx *Tx, tableName string) *QueryBuilderWithTx {
	return &QueryBuilderWithTx{
		tx: tx,
		qb: SelectBase(tableName, ""),
	}
}

// Where adds a where clause
func (qb *QueryBuilderWithTx) Where(condition string, args ...interface{}) *QueryBuilderWithTx {
	qb.qb.Where(condition)
	return qb
}

// Join adds a join clause
func (qb *QueryBuilderWithTx) Join(table string, alias string, on string) *QueryBuilderWithTx {
	qb.qb.Join(table, alias, on)
	return qb
}

// Left adds a left join clause
func (qb *QueryBuilderWithTx) Left(table string, alias string, on string) *QueryBuilderWithTx {
	qb.qb.Left(table, alias, on)
	return qb
}

// OrderBy adds an order by clause
func (qb *QueryBuilderWithTx) OrderBy(clause string) *QueryBuilderWithTx {
	qb.qb.Steps = append(qb.qb.Steps, orderByStep{clause})
	return qb
}

// Limit adds a limit clause
func (qb *QueryBuilderWithTx) Limit(limit int64) *QueryBuilderWithTx {
	qb.qb.Steps = append(qb.qb.Steps, limitStep{limit})
	return qb
}

// Offset adds an offset clause
func (qb *QueryBuilderWithTx) Offset(offset int64) *QueryBuilderWithTx {
	qb.qb.Steps = append(qb.qb.Steps, offsetStep{offset})
	return qb
}

// Select executes a select query within the transaction
func (qb *QueryBuilderWithTx) Select(dest interface{}) error {
	query := qb.qb.Build()
	return qb.tx.Select(dest, query)
}

// Get executes a query to get a single record within the transaction
func (qb *QueryBuilderWithTx) Get(dest interface{}) error {
	query := qb.qb.Build()
	return qb.tx.Get(dest, query)
}

// Common transaction patterns with retry

// InsertObjectWithTxRetry inserts a record with transaction retry (original signature)
func InsertObjectWithTxRetry(ctx context.Context, object interface{}, tableName string) error {
	return WithTxRetry(ctx, func(ctx context.Context, tx *Tx) error {
		return InsertObjectWithTxContext(ctx, tx, object, tableName)
	})
}

// UpdateObjectWithTxRetry updates a record with transaction retry (original signature)
func UpdateObjectWithTxRetry(ctx context.Context, object interface{}, tableName, whereClause string, whereArgs ...interface{}) error {
	return WithTxRetry(ctx, func(ctx context.Context, tx *Tx) error {
		return UpdateObjectWithTxContext(ctx, tx, object, tableName, whereClause, whereArgs...)
	})
}

// DeleteWithTxRetry deletes a record with transaction retry
func DeleteWithTxRetry(ctx context.Context, tableName, whereClause string, whereArgs ...interface{}) error {
	return WithTxRetry(ctx, func(ctx context.Context, tx *Tx) error {
		return DeleteWithTx(ctx, tx, tableName, whereClause, whereArgs...)
	})
}

// InsertWithTxRetry inserts a map with transaction retry
func InsertWithTxRetry(ctx context.Context, tableName string, values map[string]interface{}, returning string) error {
	return WithTxRetry(ctx, func(ctx context.Context, tx *Tx) error {
		return InsertWithTx(ctx, tx, tableName, values, returning)
	})
}

// UpdateWithTxRetry updates a map with transaction retry
func UpdateWithTxRetry(ctx context.Context, tableName string, values map[string]interface{}, returning string) error {
	return WithTxRetry(ctx, func(ctx context.Context, tx *Tx) error {
		return UpdateWithTx(ctx, tx, tableName, values, returning)
	})
}
