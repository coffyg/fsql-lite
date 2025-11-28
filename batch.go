// batch.go - Batch insert/update operations for fsql-lite
package fsql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// BatchSize is the default size for batched operations
const BatchSize = 100

// BatchInsertExecutor handles batched insert operations
type BatchInsertExecutor struct {
	tableName   string
	fields      []string
	valuesBatch [][]interface{}
	batchSize   int
	fieldMap    map[string]bool
	returning   string

	// String builder for query construction
	sb *strings.Builder

	// Object mode fields
	objectMode bool
	objectType reflect.Type
	tagCache   *ModelTagCache
}

// NewBatchInsert creates a new batch insert executor
func NewBatchInsert(tableName string, fields []string, batchSize int) *BatchInsertExecutor {
	if batchSize <= 0 {
		batchSize = BatchSize
	}

	// Create field map for quick lookups
	fieldMap := make(map[string]bool, len(fields))
	for _, field := range fields {
		fieldMap[field] = true
	}

	return &BatchInsertExecutor{
		tableName:   tableName,
		fields:      fields,
		valuesBatch: make([][]interface{}, 0, batchSize),
		batchSize:   batchSize,
		fieldMap:    fieldMap,
		sb:          &strings.Builder{},
		objectMode:  false,
	}
}

// NewBatchInsertExecutor creates a new batch insert executor for structured objects
func NewBatchInsertExecutor(tableName string, batchSize int) *BatchInsertExecutor {
	if batchSize <= 0 {
		batchSize = BatchSize
	}

	return &BatchInsertExecutor{
		tableName:   tableName,
		valuesBatch: make([][]interface{}, 0, batchSize),
		batchSize:   batchSize,
		sb:          &strings.Builder{},
		objectMode:  true,
	}
}

// Add adds a record to the batch
func (b *BatchInsertExecutor) Add(values interface{}) error {
	if b.objectMode {
		return b.addObject(values)
	} else {
		valuesMap, ok := values.(map[string]interface{})
		if !ok {
			return fmt.Errorf("values must be a map[string]interface{} when not in object mode")
		}
		return b.addMap(valuesMap)
	}
}

// addMap adds a record from a map to the batch
func (b *BatchInsertExecutor) addMap(values map[string]interface{}) error {
	rowValues := make([]interface{}, len(b.fields))

	for i, field := range b.fields {
		value, ok := values[field]
		if !ok {
			return fmt.Errorf("missing field: %s", field)
		}
		rowValues[i] = value
	}

	b.valuesBatch = append(b.valuesBatch, rowValues)

	if len(b.valuesBatch) >= b.batchSize {
		return b.Flush()
	}

	return nil
}

// addObject adds a struct record to the batch
func (b *BatchInsertExecutor) addObject(obj interface{}) error {
	if b.tagCache == nil {
		objType := reflect.TypeOf(obj)
		if objType.Kind() == reflect.Ptr {
			objType = objType.Elem()
		}

		b.objectType = objType

		var err error
		b.tagCache, err = getModelTagCache(objType)
		if err != nil {
			return fmt.Errorf("failed to get model tag cache: %w", err)
		}

		var insertFields []string
		for _, field := range b.tagCache.Fields {
			if strings.Contains(field.Mode, "i") {
				insertFields = append(insertFields, field.DbName)
			}
		}

		b.fields = insertFields

		b.fieldMap = make(map[string]bool, len(insertFields))
		for _, field := range insertFields {
			b.fieldMap[field] = true
		}
	}

	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Type() != b.objectType {
		return fmt.Errorf("object type mismatch: expected %s, got %s", b.objectType.Name(), val.Type().Name())
	}

	rowValues := make([]interface{}, 0, len(b.fields))

	for i, tagField := range b.tagCache.Fields {
		if strings.Contains(tagField.Mode, "i") {
			fieldVal := val.Field(i).Interface()

			if tagField.InsertValue != "" {
				fieldVal = tagField.InsertValue
			}

			rowValues = append(rowValues, fieldVal)
		}
	}

	b.valuesBatch = append(b.valuesBatch, rowValues)

	if len(b.valuesBatch) >= b.batchSize {
		return b.Flush()
	}

	return nil
}

// SetReturning sets the returning field
func (b *BatchInsertExecutor) SetReturning(field string) *BatchInsertExecutor {
	b.returning = field
	return b
}

// Flush executes the current batch
func (b *BatchInsertExecutor) Flush() error {
	return b.FlushContext(context.Background())
}

// FlushContext executes the current batch with context
func (b *BatchInsertExecutor) FlushContext(ctx context.Context) error {
	if len(b.valuesBatch) == 0 {
		return nil
	}

	b.sb.Reset()
	b.sb.WriteString(`INSERT INTO "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" (`)

	for i, field := range b.fields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
	}

	b.sb.WriteString(") VALUES ")

	flattenedValues := make([]interface{}, 0, len(b.valuesBatch)*len(b.fields))
	paramCounter := 1

	for i, row := range b.valuesBatch {
		if i > 0 {
			b.sb.WriteString(", ")
		}

		b.sb.WriteString("(")
		for j := range b.fields {
			if j > 0 {
				b.sb.WriteString(", ")
			}
			b.sb.WriteString(fmt.Sprintf("$%d", paramCounter))
			paramCounter++

			flattenedValues = append(flattenedValues, row[j])
		}
		b.sb.WriteString(")")
	}

	if b.returning != "" {
		b.sb.WriteString(" RETURNING ")
		b.sb.WriteString(b.returning)
	}

	query := b.sb.String()
	_, err := DB.Exec(ctx, query, flattenedValues...)
	if err != nil {
		return err
	}

	b.valuesBatch = b.valuesBatch[:0]

	return nil
}

// FlushWithTx executes the current batch within a transaction
func (b *BatchInsertExecutor) FlushWithTx(tx *Tx) error {
	return b.FlushWithTxContext(context.Background(), tx)
}

// FlushWithTxContext executes the current batch within a transaction with context
func (b *BatchInsertExecutor) FlushWithTxContext(ctx context.Context, tx *Tx) error {
	if len(b.valuesBatch) == 0 {
		return nil
	}

	b.sb.Reset()
	b.sb.WriteString(`INSERT INTO "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" (`)

	for i, field := range b.fields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
	}

	b.sb.WriteString(") VALUES ")

	flattenedValues := make([]interface{}, 0, len(b.valuesBatch)*len(b.fields))
	paramCounter := 1

	for i, row := range b.valuesBatch {
		if i > 0 {
			b.sb.WriteString(", ")
		}

		b.sb.WriteString("(")
		for j := range b.fields {
			if j > 0 {
				b.sb.WriteString(", ")
			}
			b.sb.WriteString(fmt.Sprintf("$%d", paramCounter))
			paramCounter++

			flattenedValues = append(flattenedValues, row[j])
		}
		b.sb.WriteString(")")
	}

	if b.returning != "" {
		b.sb.WriteString(" RETURNING ")
		b.sb.WriteString(b.returning)
	}

	query := b.sb.String()
	_, err := tx.ExecContext(ctx, query, flattenedValues...)
	if err != nil {
		return err
	}

	b.valuesBatch = b.valuesBatch[:0]

	return nil
}

// BatchUpdateExecutor handles batched update operations
type BatchUpdateExecutor struct {
	tableName      string
	updateFields   []string
	conditionField string
	batchSize      int
	valuesBatch    [][]interface{}

	sb *strings.Builder
}

// NewBatchUpdate creates a new batch update executor
func NewBatchUpdate(tableName string, updateFields []string, conditionField string, batchSize int) *BatchUpdateExecutor {
	if batchSize <= 0 {
		batchSize = BatchSize
	}

	return &BatchUpdateExecutor{
		tableName:      tableName,
		updateFields:   updateFields,
		conditionField: conditionField,
		batchSize:      batchSize,
		valuesBatch:    make([][]interface{}, 0, batchSize),
		sb:             &strings.Builder{},
	}
}

// Add adds a record to the batch
func (b *BatchUpdateExecutor) Add(values map[string]interface{}, condition interface{}) error {
	rowValues := make([]interface{}, len(b.updateFields)+1)

	for i, field := range b.updateFields {
		value, ok := values[field]
		if !ok {
			return fmt.Errorf("missing field: %s", field)
		}
		rowValues[i] = value
	}

	rowValues[len(b.updateFields)] = condition

	b.valuesBatch = append(b.valuesBatch, rowValues)

	if len(b.valuesBatch) >= b.batchSize {
		return b.Flush()
	}

	return nil
}

// Flush executes the current batch
func (b *BatchUpdateExecutor) Flush() error {
	return b.FlushContext(context.Background())
}

// FlushContext executes the current batch with context
func (b *BatchUpdateExecutor) FlushContext(ctx context.Context) error {
	if len(b.valuesBatch) == 0 {
		return nil
	}

	b.sb.Reset()
	b.sb.WriteString(`UPDATE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" SET `)

	for i, field := range b.updateFields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
		b.sb.WriteString(" = CASE ")
		b.sb.WriteString(b.conditionField)

		paramCount := len(b.updateFields) + 1
		for rowIdx := range b.valuesBatch {
			b.sb.WriteString(fmt.Sprintf(" WHEN $%d THEN $%d",
				rowIdx*paramCount+paramCount,
				rowIdx*paramCount+i+1))
		}

		b.sb.WriteString(" ELSE ")
		b.sb.WriteString(field)
		b.sb.WriteString(" END")
	}

	b.sb.WriteString(` WHERE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`".`)
	b.sb.WriteString(b.conditionField)
	b.sb.WriteString(" IN (")

	paramCount := len(b.updateFields) + 1
	for rowIdx := range b.valuesBatch {
		if rowIdx > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(fmt.Sprintf("$%d", rowIdx*paramCount+paramCount))
	}
	b.sb.WriteString(")")

	flatValues := make([]interface{}, 0, len(b.valuesBatch)*(len(b.updateFields)+1))
	for _, rowValues := range b.valuesBatch {
		flatValues = append(flatValues, rowValues...)
	}

	query := b.sb.String()
	_, err := DB.Exec(ctx, query, flatValues...)

	b.valuesBatch = b.valuesBatch[:0]

	return err
}

// FlushWithTx executes the current batch within a transaction
func (b *BatchUpdateExecutor) FlushWithTx(tx *Tx) error {
	return b.FlushWithTxContext(context.Background(), tx)
}

// FlushWithTxContext executes the current batch within a transaction with context
func (b *BatchUpdateExecutor) FlushWithTxContext(ctx context.Context, tx *Tx) error {
	if len(b.valuesBatch) == 0 {
		return nil
	}

	b.sb.Reset()
	b.sb.WriteString(`UPDATE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" SET `)

	for i, field := range b.updateFields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
		b.sb.WriteString(" = CASE ")
		b.sb.WriteString(b.conditionField)

		paramCount := len(b.updateFields) + 1
		for rowIdx := range b.valuesBatch {
			b.sb.WriteString(fmt.Sprintf(" WHEN $%d THEN $%d",
				rowIdx*paramCount+paramCount,
				rowIdx*paramCount+i+1))
		}

		b.sb.WriteString(" ELSE ")
		b.sb.WriteString(field)
		b.sb.WriteString(" END")
	}

	b.sb.WriteString(` WHERE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`".`)
	b.sb.WriteString(b.conditionField)
	b.sb.WriteString(" IN (")

	paramCount := len(b.updateFields) + 1
	for rowIdx := range b.valuesBatch {
		if rowIdx > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(fmt.Sprintf("$%d", rowIdx*paramCount+paramCount))
	}
	b.sb.WriteString(")")

	flatValues := make([]interface{}, 0, len(b.valuesBatch)*(len(b.updateFields)+1))
	for _, rowValues := range b.valuesBatch {
		flatValues = append(flatValues, rowValues...)
	}

	query := b.sb.String()
	_, err := tx.ExecContext(ctx, query, flatValues...)

	b.valuesBatch = b.valuesBatch[:0]

	return err
}

// Pool for BatchInsertExecutors
var batchInsertPool = sync.Pool{
	New: func() interface{} {
		return &BatchInsertExecutor{
			valuesBatch: make([][]interface{}, 0, BatchSize),
			sb:          &strings.Builder{},
		}
	},
}

// GetBatchInsert gets a BatchInsertExecutor from the pool
func GetBatchInsert(tableName string, fields []string, batchSize int) *BatchInsertExecutor {
	b := batchInsertPool.Get().(*BatchInsertExecutor)

	b.tableName = tableName
	b.fields = fields
	b.valuesBatch = b.valuesBatch[:0]
	b.batchSize = batchSize
	b.returning = ""

	b.fieldMap = make(map[string]bool, len(fields))
	for _, field := range fields {
		b.fieldMap[field] = true
	}

	return b
}

// ReleaseBatchInsert returns a BatchInsertExecutor to the pool
func ReleaseBatchInsert(b *BatchInsertExecutor) {
	b.valuesBatch = b.valuesBatch[:0]
	b.fieldMap = nil

	batchInsertPool.Put(b)
}

// InsertBatchWithTx inserts multiple records within a transaction
func InsertBatchWithTx(ctx context.Context, tx *Tx, objects interface{}, tableName string) error {
	if tx.tx == nil {
		return ErrTxDone
	}

	val := reflect.ValueOf(objects)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("objects must be a slice")
	}
	if val.Len() == 0 {
		return nil
	}

	batchInsert := NewBatchInsertExecutor(tableName, BatchSize)

	for i := 0; i < val.Len(); i++ {
		obj := val.Index(i).Interface()
		err := batchInsert.Add(obj)
		if err != nil {
			return fmt.Errorf("failed to add object to batch: %w", err)
		}
	}

	err := batchInsert.FlushWithTxContext(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	return nil
}
