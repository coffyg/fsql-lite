// fsql-lite: Lightweight ORM wrapper around pgxpool
// No database/sql, no sqlx, no replica complexity - just direct pool access
package fsql

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Global database connection pool
var (
	DB            *pgxpool.Pool
	DbInitialised bool = false
)

// InitDB initializes the global database pool
func InitDB(databaseURL string, maxCon int, minCon int) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database URL: %w", err)
	}

	// Connection pool configuration
	poolConfig.MaxConns = int32(maxCon)
	poolConfig.MinConns = int32(minCon)

	DB, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Use simple protocol - no prepared statements
	DB.Config().ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	DB.Config().ConnConfig.StatementCacheCapacity = 0

	// Test connection
	if err := DB.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	DbInitialised = true

	return DB, nil
}

// CloseDB closes the global database connection
func CloseDB() {
	if DB != nil {
		DB.Close()
		DB = nil
		DbInitialised = false
	}
}

// GetPoolStats returns connection pool statistics
func GetPoolStats() (totalConns, acquiredConns, idleConns int32, emptyAcquireCount int64, acquireDuration time.Duration) {
	if DB == nil {
		return 0, 0, 0, 0, 0
	}

	stats := DB.Stat()
	return stats.TotalConns(), stats.AcquiredConns(), stats.IdleConns(),
		stats.EmptyAcquireCount(), stats.AcquireDuration()
}

// extractTableName attempts to extract table name from SQL query for logging
func extractTableName(query string) string {
	// Same logic as original fsql
	// TODO: Copy from fsql if needed for logging
	return "unknown"
}
