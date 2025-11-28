// transaction.go
package fsql

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Tx represents a database transaction
// WARNING: Tx is NOT safe for concurrent use by multiple goroutines.
type Tx struct {
	tx pgx.Tx
}

// TxOptions defines the options for transactions
type TxOptions struct {
	// Isolation level for the transaction (use pgx.TxIsoLevel constants)
	IsoLevel     pgx.TxIsoLevel
	AccessMode   pgx.TxAccessMode
	DeferrableMode pgx.TxDeferrableMode
	MaxRetries   int
}

// Default transaction options
var DefaultTxOptions = TxOptions{
	IsoLevel:       pgx.ReadCommitted,
	AccessMode:     pgx.ReadWrite,
	DeferrableMode: pgx.NotDeferrable,
	MaxRetries:     3,
}

// ErrTxDone is returned when attempting an operation on a completed transaction
var ErrTxDone = errors.New("transaction has already been committed or rolled back")

// ErrMaxRetriesExceeded is returned when transaction exceeds max retry attempts
var ErrMaxRetriesExceeded = errors.New("transaction max retries exceeded")

// Common retryable error substrings
var retryableErrors = []string{
	"deadlock detected",
	"serialize",
	"serialization",
	"conflict",
	"concurrent update",
	"could not serialize access",
	"deadlock",
	"lock wait timeout",
	"lock timeout",
	"connection reset",
	"40001", // Serialization failure
	"40P01", // Deadlock detected
}

// BeginTx starts a new transaction with the default options
func BeginTx(ctx context.Context) (*Tx, error) {
	return BeginTxWithOptions(ctx, DefaultTxOptions)
}

// BeginTxWithOptions starts a new transaction with the specified options
func BeginTxWithOptions(ctx context.Context, opts TxOptions) (*Tx, error) {
	if DB == nil {
		return nil, errors.New("database not initialized")
	}

	txOpts := pgx.TxOptions{
		IsoLevel:       opts.IsoLevel,
		AccessMode:     opts.AccessMode,
		DeferrableMode: opts.DeferrableMode,
	}

	tx, err := DB.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &Tx{tx: tx}, nil
}

// Commit commits the transaction (context optional for compatibility)
func (tx *Tx) Commit(ctx ...context.Context) error {
	if tx.tx == nil {
		return ErrTxDone
	}

	c := context.Background()
	if len(ctx) > 0 {
		c = ctx[0]
	}

	err := tx.tx.Commit(c)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Rollback aborts the transaction (context optional for compatibility)
func (tx *Tx) Rollback(ctx ...context.Context) error {
	if tx.tx == nil {
		return ErrTxDone
	}

	c := context.Background()
	if len(ctx) > 0 {
		c = ctx[0]
	}

	err := tx.tx.Rollback(c)
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	return nil
}

// Exec executes a query within the transaction (no context needed for compat)
func (tx *Tx) Exec(query string, args ...interface{}) (pgconn.CommandTag, error) {
	if tx.tx == nil {
		return pgconn.CommandTag{}, ErrTxDone
	}

	return tx.tx.Exec(context.Background(), query, args...)
}

// ExecContext executes a query within the transaction with context
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	if tx.tx == nil {
		return pgconn.CommandTag{}, ErrTxDone
	}

	return tx.tx.Exec(ctx, query, args...)
}

// Query executes a query that returns rows within the transaction (no context for compat)
func (tx *Tx) Query(query string, args ...interface{}) (pgx.Rows, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}

	return tx.tx.Query(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows with context
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}

	return tx.tx.Query(ctx, query, args...)
}

// QueryRow executes a query that returns a single row (no context for compat)
func (tx *Tx) QueryRow(query string, args ...interface{}) pgx.Row {
	if tx.tx == nil {
		return nil
	}

	return tx.tx.QueryRow(context.Background(), query, args...)
}

// QueryRowContext executes a query that returns a single row with context
func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) pgx.Row {
	if tx.tx == nil {
		return nil
	}

	return tx.tx.QueryRow(ctx, query, args...)
}

// TxFn defines a function that uses a transaction
type TxFn func(context.Context, *Tx) error

// WithTx executes a function within a transaction
// If the function returns an error, the transaction is rolled back
// If the function returns nil, the transaction is committed
func WithTx(ctx context.Context, fn TxFn) error {
	return WithTxOptions(ctx, DefaultTxOptions, fn)
}

// WithTxOptions executes a function within a transaction with options
func WithTxOptions(ctx context.Context, opts TxOptions, fn TxFn) error {
	tx, err := BeginTxWithOptions(ctx, opts)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback(ctx)
			panic(p) // Re-throw panic after rollback
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit(ctx)
}

// WithTxRetry executes a function within a transaction with retry logic
// If the function returns a retryable error, the transaction is retried
func WithTxRetry(ctx context.Context, fn TxFn) error {
	return WithTxRetryOptions(ctx, DefaultTxOptions, fn)
}

// WithTxRetryOptions executes a function within a transaction with retry logic and options
func WithTxRetryOptions(ctx context.Context, opts TxOptions, fn TxFn) error {
	var err error
	maxRetries := opts.MaxRetries

	for attempt := 0; attempt < maxRetries; attempt++ {
		attemptErr := WithTxOptions(ctx, opts, fn)
		if attemptErr == nil {
			return nil
		}

		// Store the error for possible return
		err = attemptErr

		// Check if we should retry
		if !isRetryableError(err) {
			return err
		}

		// Wait with exponential backoff with jitter before retrying
		baseBackoff := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
		jitter := time.Duration(float64(baseBackoff) * 0.2 * (rand.Float64() - 0.5))
		backoff := baseBackoff + jitter

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Continue with retry
		}
	}

	if err != nil {
		return fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, err)
	}

	return ErrMaxRetriesExceeded
}

// isRetryableError determines if an error can be retried
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	for _, retryMsg := range retryableErrors {
		if strings.Contains(errMsg, retryMsg) {
			return true
		}
	}

	return false
}

// WithReadTx executes a function within a read-only transaction
func WithReadTx(ctx context.Context, fn TxFn) error {
	opts := DefaultTxOptions
	opts.AccessMode = pgx.ReadOnly
	return WithTxOptions(ctx, opts, fn)
}

// WithSerializableTx executes a function within a serializable isolation level transaction
func WithSerializableTx(ctx context.Context, fn TxFn) error {
	opts := DefaultTxOptions
	opts.IsoLevel = pgx.Serializable
	return WithTxRetryOptions(ctx, opts, fn)
}

// WithReadCommittedTx executes a function within a read committed isolation level transaction
func WithReadCommittedTx(ctx context.Context, fn TxFn) error {
	opts := DefaultTxOptions
	opts.IsoLevel = pgx.ReadCommitted
	return WithTxOptions(ctx, opts, fn)
}

// InsertWithTx executes an insert within a transaction
func InsertWithTx(ctx context.Context, tx *Tx, tableName string, values map[string]interface{}, returning string) error {
	if tx.tx == nil {
		return ErrTxDone
	}

	query, args := GetInsertQuery(tableName, values, returning)

	if returning != "" {
		row := tx.tx.QueryRow(ctx, query, args...)
		var result interface{}
		return row.Scan(&result)
	}

	_, err := tx.tx.Exec(ctx, query, args...)
	return err
}

// UpdateWithTx executes an update within a transaction
func UpdateWithTx(ctx context.Context, tx *Tx, tableName string, values map[string]interface{}, returning string) error {
	if tx.tx == nil {
		return ErrTxDone
	}

	query, args := GetUpdateQuery(tableName, values, returning)
	_, err := tx.tx.Exec(ctx, query, args...)
	return err
}
