// compat.go - API compatibility layer with original fsql
// This file provides backward-compatible functions and types so code written
// for the original fsql package can work with fsql-lite without modifications.
package fsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
)

// =============================================================================
// GLOBAL VARIABLES COMPATIBILITY
// =============================================================================

// Db is an alias for DB for backward compatibility (original uses lowercase)
// NOTE: This is set by InitDB/InitDBPool, users should use DB directly
var Db = &dbCompat{}

// dbCompat wraps pgxpool.Pool to provide sqlx-like interface
type dbCompat struct{}

// Close closes the database connection pool
func (d *dbCompat) Close() {
	CloseDB()
}

// Exec executes a query without returning any rows (delegates to SafeExec)
func (d *dbCompat) Exec(query string, args ...interface{}) (pgconn.CommandTag, error) {
	return SafeExec(query, args...)
}

// Query executes a query that returns rows (delegates to SafeQuery)
func (d *dbCompat) Query(query string, args ...interface{}) (pgx.Rows, error) {
	return SafeQuery(query, args...)
}

// QueryRow executes a query that returns at most one row
func (d *dbCompat) QueryRow(query string, args ...interface{}) pgx.Row {
	return SafeQueryRow(query, args...)
}

// Get retrieves a single row into dest (struct scanning)
func (d *dbCompat) Get(dest interface{}, query string, args ...interface{}) error {
	return SafeGet(dest, query, args...)
}

// GetContext retrieves a single row into dest with context
func (d *dbCompat) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := DB.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return StructScan(rows, dest)
}

// Select retrieves multiple rows into dest (slice of structs)
func (d *dbCompat) Select(dest interface{}, query string, args ...interface{}) error {
	return SafeSelect(dest, query, args...)
}

// SelectContext retrieves multiple rows with context
func (d *dbCompat) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := DB.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	return StructsScan(rows, dest)
}

// NamedExec executes a named query
func (d *dbCompat) NamedExec(query string, arg interface{}) (pgconn.CommandTag, error) {
	return SafeNamedExec(query, arg)
}

// QueryRowContext executes a query that returns at most one row with context
func (d *dbCompat) QueryRowContext(ctx context.Context, query string, args ...interface{}) pgx.Row {
	return DB.QueryRow(ctx, query, args...)
}

// QueryContext executes a query that returns rows with context
func (d *dbCompat) QueryContext(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	return DB.Query(ctx, query, args...)
}

// ExecContext executes a query without returning rows with context
func (d *dbCompat) ExecContext(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	return DB.Exec(ctx, query, args...)
}

// DefaultDBTimeout is the default timeout for database operations
var DefaultDBTimeout = 30 * time.Second

// logger for fsql operations (optional)
var logger *zerolog.Logger

// SetLogger configures the global logger for fsql operations
func SetLogger(l *zerolog.Logger) {
	logger = l
}

// =============================================================================
// DBCONFIG COMPATIBILITY
// =============================================================================

// DBConfig mirrors the original fsql config structure
type DBConfig struct {
	MaxConnections int
	MinConnections int
}

// DefaultConfig provides reasonable production defaults
var DefaultConfig = DBConfig{
	MaxConnections: 50,
	MinConnections: 5,
}

// InitDB initializes the database (original fsql API signature)
// This is the main entry point matching original fsql exactly
func InitDB(database string, config ...DBConfig) {
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = config[0]
		DefaultConfig = cfg
	}

	_, err := InitDBWithPool(database, cfg.MaxConnections, cfg.MinConnections)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}

// InitDBPool initializes the database with pool configuration (original API)
func InitDBPool(database string, config ...DBConfig) {
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = config[0]
		DefaultConfig = cfg
	}

	_, err := InitDBWithPool(database, cfg.MaxConnections, cfg.MinConnections)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}

// InitDBCompat initializes DB with the original fsql signature (variadic DBConfig)
// This matches the original fsql InitDB signature exactly
func InitDBCompat(database string, config ...DBConfig) {
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = config[0]
		DefaultConfig = cfg
	}

	_, err := InitDBWithPool(database, cfg.MaxConnections, cfg.MinConnections)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}

// =============================================================================
// REPLICA SUPPORT (NO-OP STUBS)
// =============================================================================

// DBConnection represents a database connection with health tracking
type DBConnection struct {
	URI          string
	FailureCount int32
	State        int32
}

// InitDbReplicas initializes read replica connections (no-op in lite version)
func InitDbReplicas(databases []string, config ...DBConfig) {
	// No-op: fsql-lite doesn't support replicas
	// All reads go to the main pool
}

// GetReplika returns a read replica connection (returns main DB in lite version)
func GetReplika() *dbCompat {
	return Db
}

// CloseReplicas closes all replica connections (no-op in lite version)
func CloseReplicas() {
	// No-op: fsql-lite doesn't support replicas
}

// IsConnectionHealthy checks if the database connection is healthy
func IsConnectionHealthy(db interface{}) bool {
	if DB == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return DB.Ping(ctx) == nil
}

// =============================================================================
// SAFE WRAPPER FUNCTIONS (with automatic timeouts)
// =============================================================================

// SafeExec wraps DB.Exec with automatic timeout
func SafeExec(query string, args ...interface{}) (pgconn.CommandTag, error) {
	return SafeExecTimeout(DefaultDBTimeout, query, args...)
}

// SafeExecTimeout wraps DB.Exec with custom timeout
func SafeExecTimeout(timeout time.Duration, query string, args ...interface{}) (pgconn.CommandTag, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return DB.Exec(ctx, query, args...)
}

// SafeQuery wraps DB.Query (no timeout - iterator consumed after return)
func SafeQuery(query string, args ...interface{}) (pgx.Rows, error) {
	return DB.Query(context.Background(), query, args...)
}

// SafeQueryTimeout wraps DB.Query (no timeout - iterator consumed after return)
func SafeQueryTimeout(timeout time.Duration, query string, args ...interface{}) (pgx.Rows, error) {
	return DB.Query(context.Background(), query, args...)
}

// SafeGet wraps Get with automatic timeout
func SafeGet(dest interface{}, query string, args ...interface{}) error {
	return SafeGetTimeout(DefaultDBTimeout, dest, query, args...)
}

// SafeGetTimeout wraps Get with custom timeout
func SafeGetTimeout(timeout time.Duration, dest interface{}, query string, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := DB.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return StructScan(rows, dest)
}

// SafeSelect wraps Select with automatic timeout
func SafeSelect(dest interface{}, query string, args ...interface{}) error {
	return SafeSelectTimeout(DefaultDBTimeout, dest, query, args...)
}

// SafeSelectTimeout wraps Select with custom timeout
func SafeSelectTimeout(timeout time.Duration, dest interface{}, query string, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := DB.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	return StructsScan(rows, dest)
}

// SafeQueryRow wraps DB.QueryRow (no timeout - Scan happens after return)
func SafeQueryRow(query string, args ...interface{}) pgx.Row {
	return DB.QueryRow(context.Background(), query, args...)
}

// SafeQueryRowTimeout wraps DB.QueryRow (no timeout - Scan happens after return)
func SafeQueryRowTimeout(timeout time.Duration, query string, args ...interface{}) pgx.Row {
	return DB.QueryRow(context.Background(), query, args...)
}

// SafeNamedExec is not directly supported by pgx, provided for compatibility
// This converts named parameters to positional parameters
func SafeNamedExec(query string, arg interface{}) (pgconn.CommandTag, error) {
	return SafeNamedExecTimeout(DefaultDBTimeout, query, arg)
}

// SafeNamedExecTimeout converts named parameters to positional
func SafeNamedExecTimeout(timeout time.Duration, query string, arg interface{}) (pgconn.CommandTag, error) {
	// Convert named query to positional
	positionalQuery, args, err := namedToPositional(query, arg)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return SafeExecTimeout(timeout, positionalQuery, args...)
}

// SafeNamedQuery is provided for compatibility
func SafeNamedQuery(query string, arg interface{}) (pgx.Rows, error) {
	return SafeNamedQueryTimeout(DefaultDBTimeout, query, arg)
}

// SafeNamedQueryTimeout converts named parameters to positional
func SafeNamedQueryTimeout(timeout time.Duration, query string, arg interface{}) (pgx.Rows, error) {
	positionalQuery, args, err := namedToPositional(query, arg)
	if err != nil {
		return nil, err
	}
	return SafeQueryTimeout(timeout, positionalQuery, args...)
}

// SafeBegin starts a transaction (returns compatibility wrapper)
func SafeBegin() (*sql.Tx, error) {
	// This returns sql.Tx which isn't compatible with pgx
	// Users should use BeginTx instead
	return nil, errors.New("SafeBegin not supported in fsql-lite, use BeginTx(ctx) instead")
}

// SafeBeginx starts a transaction (returns our Tx wrapper)
func SafeBeginx() (*Tx, error) {
	return BeginTx(context.Background())
}

// namedToPositional converts a named query to positional parameters
func namedToPositional(query string, arg interface{}) (string, []interface{}, error) {
	v := reflect.ValueOf(arg)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() == reflect.Map {
		// Handle map[string]interface{}
		return namedMapToPositional(query, arg.(map[string]interface{}))
	}

	if v.Kind() == reflect.Struct {
		// Handle struct
		return namedStructToPositional(query, v)
	}

	return "", nil, fmt.Errorf("arg must be a map or struct, got %T", arg)
}

func namedMapToPositional(query string, m map[string]interface{}) (string, []interface{}, error) {
	var args []interface{}
	paramIdx := 1

	for key, val := range m {
		placeholder := ":" + key
		if strings.Contains(query, placeholder) {
			query = strings.Replace(query, placeholder, fmt.Sprintf("$%d", paramIdx), -1)
			args = append(args, val)
			paramIdx++
		}
	}

	return query, args, nil
}

func namedStructToPositional(query string, v reflect.Value) (string, []interface{}, error) {
	var args []interface{}
	paramIdx := 1
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		dbTag := field.Tag.Get("db")
		if dbTag == "" || dbTag == "-" {
			continue
		}

		placeholder := ":" + dbTag
		if strings.Contains(query, placeholder) {
			query = strings.Replace(query, placeholder, fmt.Sprintf("$%d", paramIdx), -1)
			args = append(args, v.Field(i).Interface())
			paramIdx++
		}
	}

	return query, args, nil
}

// =============================================================================
// TX COMPATIBILITY (context-less method wrappers)
// =============================================================================

// CommitNoCtx commits without requiring context (uses Background)
func (tx *Tx) CommitNoCtx() error {
	return tx.Commit(context.Background())
}

// RollbackNoCtx rollbacks without requiring context (uses Background)
func (tx *Tx) RollbackNoCtx() error {
	return tx.Rollback(context.Background())
}

// ExecNoCtx executes without context (alias for Exec which already doesn't need context)
func (tx *Tx) ExecNoCtx(query string, args ...interface{}) (pgconn.CommandTag, error) {
	return tx.Exec(query, args...)
}

// QueryNoCtx queries without context (alias for Query which already doesn't need context)
func (tx *Tx) QueryNoCtx(query string, args ...interface{}) (pgx.Rows, error) {
	return tx.Query(query, args...)
}

// QueryRowNoCtx queries single row without context (alias for QueryRow which already doesn't need context)
func (tx *Tx) QueryRowNoCtx(query string, args ...interface{}) pgx.Row {
	return tx.QueryRow(query, args...)
}

// Get retrieves a single item from the database within the transaction
func (tx *Tx) Get(dest interface{}, query string, args ...interface{}) error {
	return tx.GetContext(context.Background(), dest, query, args...)
}

// GetContext retrieves a single item with context
func (tx *Tx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if tx.tx == nil {
		return ErrTxDone
	}
	rows, err := tx.tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return StructScan(rows, dest)
}

// Select retrieves multiple items from the database within the transaction
func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	return tx.SelectContext(context.Background(), dest, query, args...)
}

// SelectContext retrieves multiple items with context
func (tx *Tx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if tx.tx == nil {
		return ErrTxDone
	}
	rows, err := tx.tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	return StructsScan(rows, dest)
}

// NamedExec executes a named query within the transaction
func (tx *Tx) NamedExec(query string, arg interface{}) (pgconn.CommandTag, error) {
	return tx.NamedExecContext(context.Background(), query, arg)
}

// NamedExecContext executes a named query with context
func (tx *Tx) NamedExecContext(ctx context.Context, query string, arg interface{}) (pgconn.CommandTag, error) {
	if tx.tx == nil {
		return pgconn.CommandTag{}, ErrTxDone
	}
	positionalQuery, args, err := namedToPositional(query, arg)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return tx.tx.Exec(ctx, positionalQuery, args...)
}

// =============================================================================
// TX FUNCTION COMPATIBILITY (old signature support)
// =============================================================================

// TxFnCompat is the original fsql TxFn signature (without context)
type TxFnCompat func(*Tx) error

// WithTxCompat executes with the old TxFn signature (no context passed to fn)
func WithTxCompat(ctx context.Context, fn TxFnCompat) error {
	return WithTx(ctx, func(ctx context.Context, tx *Tx) error {
		return fn(tx)
	})
}

// WithTxOptionsCompat executes with old signature and options
func WithTxOptionsCompat(ctx context.Context, opts TxOptions, fn TxFnCompat) error {
	return WithTxOptions(ctx, opts, func(ctx context.Context, tx *Tx) error {
		return fn(tx)
	})
}

// WithTxRetryCompat executes with old signature and retry
func WithTxRetryCompat(ctx context.Context, fn TxFnCompat) error {
	return WithTxRetry(ctx, func(ctx context.Context, tx *Tx) error {
		return fn(tx)
	})
}

// =============================================================================
// DELETE WITH TX
// =============================================================================

// DeleteWithTx deletes a record within a transaction
func DeleteWithTx(ctx context.Context, tx *Tx, tableName, whereClause string, whereArgs ...interface{}) error {
	if tx.tx == nil {
		return ErrTxDone
	}

	query := fmt.Sprintf(`DELETE FROM "%s" WHERE %s`, tableName, whereClause)
	_, err := tx.tx.Exec(ctx, query, whereArgs...)
	return err
}

// =============================================================================
// SELECT/GET WITH TX (original signatures)
// =============================================================================

// SelectWithTx selects records within a transaction (original API)
func SelectWithTx(tx *Tx, dest interface{}, query string, args ...interface{}) error {
	return tx.Select(dest, query, args...)
}

// GetWithTx gets a single record within a transaction (original API)
func GetWithTx(tx *Tx, dest interface{}, query string, args ...interface{}) error {
	return tx.Get(dest, query, args...)
}

// =============================================================================
// EXECUTE WITH RETRY
// =============================================================================

// ExecuteWithRetry executes a query with retry logic
func ExecuteWithRetry(query string, args ...interface{}) error {
	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		_, err := DB.Exec(context.Background(), query, args...)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(1<<uint(i)) * 100 * time.Millisecond)
			continue
		}
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("max retries exceeded: %w", lastErr)
	}
	return errors.New("max retries exceeded with unknown error")
}

// =============================================================================
// TXOPTIONS COMPATIBILITY
// =============================================================================

// TxOptionsCompat provides compatibility with original fsql TxOptions that uses sql.IsolationLevel
type TxOptionsCompat struct {
	Isolation  sql.IsolationLevel
	ReadOnly   bool
	Deferrable bool
	MaxRetries int
}

// ToTxOptions converts compatibility options to native options
func (o TxOptionsCompat) ToTxOptions() TxOptions {
	opts := TxOptions{
		MaxRetries: o.MaxRetries,
	}

	// Convert isolation level
	switch o.Isolation {
	case sql.LevelReadUncommitted:
		opts.IsoLevel = pgx.ReadUncommitted
	case sql.LevelReadCommitted:
		opts.IsoLevel = pgx.ReadCommitted
	case sql.LevelRepeatableRead:
		opts.IsoLevel = pgx.RepeatableRead
	case sql.LevelSerializable:
		opts.IsoLevel = pgx.Serializable
	default:
		opts.IsoLevel = pgx.ReadCommitted
	}

	// Convert access mode
	if o.ReadOnly {
		opts.AccessMode = pgx.ReadOnly
	} else {
		opts.AccessMode = pgx.ReadWrite
	}

	// Convert deferrable mode
	if o.Deferrable {
		opts.DeferrableMode = pgx.Deferrable
	} else {
		opts.DeferrableMode = pgx.NotDeferrable
	}

	return opts
}

// DefaultTxOptionsCompat provides default options in original format
var DefaultTxOptionsCompat = TxOptionsCompat{
	Isolation:  sql.LevelDefault,
	ReadOnly:   false,
	Deferrable: false,
	MaxRetries: 3,
}

// =============================================================================
// GetTxById COMPATIBILITY (stub)
// =============================================================================

// GetTxById returns a transaction by ID for compatibility with existing code
func GetTxById(id string) (*Tx, error) {
	return nil, errors.New("transaction retrieval by ID is not supported")
}
