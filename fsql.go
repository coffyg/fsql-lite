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

// InitDBWithPool initializes the global database pool with explicit pool settings
func InitDBWithPool(databaseURL string, maxCon int, minCon int) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database URL: %w", err)
	}

	// Connection pool configuration
	poolConfig.MaxConns = int32(maxCon)
	poolConfig.MinConns = int32(minCon)

	// Use simple protocol - no prepared statements (MUST be set BEFORE creating pool)
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	poolConfig.ConnConfig.StatementCacheCapacity = 0

	DB, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

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

// PoolPressure returns real-time pool pressure indicators
// Use this instead of raw stats for monitoring - avoids scary cumulative numbers
type PoolPressure struct {
	TotalConns    int32   // Total connections in pool
	ActiveConns   int32   // Currently acquired/in-use connections
	IdleConns     int32   // Available connections
	UsagePercent  float64 // Active / Total * 100
	AvgWaitMs     float64 // Average wait time per acquire (cumulative)
	TotalWaits    int64   // Total times pool was empty on acquire
	IsUnderPressure bool  // True if >80% usage OR active > total-2
}

// GetPoolPressure returns actionable pool pressure info
func GetPoolPressure() PoolPressure {
	if DB == nil {
		return PoolPressure{}
	}

	stats := DB.Stat()
	total := stats.TotalConns()
	active := stats.AcquiredConns()
	idle := stats.IdleConns()
	waits := stats.EmptyAcquireCount()
	waitDuration := stats.AcquireDuration()

	var usagePercent float64
	if total > 0 {
		usagePercent = float64(active) / float64(total) * 100
	}

	var avgWaitMs float64
	if waits > 0 {
		avgWaitMs = float64(waitDuration.Milliseconds()) / float64(waits)
	}

	// Under pressure if: >80% used OR less than 2 idle connections
	underPressure := usagePercent > 80 || (total > 0 && idle < 2)

	return PoolPressure{
		TotalConns:      total,
		ActiveConns:     active,
		IdleConns:       idle,
		UsagePercent:    usagePercent,
		AvgWaitMs:       avgWaitMs,
		TotalWaits:      waits,
		IsUnderPressure: underPressure,
	}
}

// extractTableName attempts to extract table name from SQL query for logging
func extractTableName(query string) string {
	// Same logic as original fsql
	// TODO: Copy from fsql if needed for logging
	return "unknown"
}
