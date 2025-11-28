// prepared.go - Prepared statement cache for fsql-lite
// Note: pgx doesn't use traditional prepared statements in the same way as database/sql
// This file provides API compatibility with original fsql
package fsql

import (
	"sync"
	"sync/atomic"
	"time"
)

// PreparedStmt represents a prepared statement with usage statistics
type PreparedStmt struct {
	query      string
	useCount   int64
	lastUsed   time.Time
	isSelect   bool
	paramCount int
}

// PreparedStmtCache manages a cache of prepared statements
type PreparedStmtCache struct {
	statements    map[string]*PreparedStmt
	hits          int64
	misses        int64
	maxStatements int
	ttl           time.Duration
	mu            sync.RWMutex
}

// GlobalStmtCache is the global prepared statement cache
var GlobalStmtCache = NewPreparedStmtCache(100, 30*time.Minute)

// NewPreparedStmtCache creates a new prepared statement cache
func NewPreparedStmtCache(maxStatements int, ttl time.Duration) *PreparedStmtCache {
	return &PreparedStmtCache{
		statements:    make(map[string]*PreparedStmt, maxStatements),
		maxStatements: maxStatements,
		ttl:           ttl,
	}
}

// Get retrieves a prepared statement from the cache
func (c *PreparedStmtCache) Get(query string) *PreparedStmt {
	c.mu.RLock()
	stmt, ok := c.statements[query]
	c.mu.RUnlock()

	if ok {
		atomic.AddInt64(&c.hits, 1)
		atomic.AddInt64(&stmt.useCount, 1)
		return stmt
	}

	atomic.AddInt64(&c.misses, 1)
	return nil
}

// Add adds a prepared statement to the cache
func (c *PreparedStmtCache) Add(query string, isSelect bool, paramCount int) *PreparedStmt {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.statements) >= c.maxStatements {
		c.evictLRU()
	}

	ps := &PreparedStmt{
		query:      query,
		useCount:   1,
		lastUsed:   time.Now(),
		isSelect:   isSelect,
		paramCount: paramCount,
	}

	c.statements[query] = ps
	return ps
}

// evictLRU removes the least recently used statement
func (c *PreparedStmtCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time

	for k, v := range c.statements {
		if oldestKey == "" || v.lastUsed.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.lastUsed
		}
	}

	if oldestKey != "" {
		delete(c.statements, oldestKey)
	}
}

// Clear closes all statements and clears the cache
func (c *PreparedStmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.statements = make(map[string]*PreparedStmt, c.maxStatements)
}

// Stats returns cache statistics
func (c *PreparedStmtCache) Stats() map[string]int64 {
	c.mu.RLock()
	stmtCount := len(c.statements)
	c.mu.RUnlock()

	return map[string]int64{
		"size":   int64(stmtCount),
		"hits":   atomic.LoadInt64(&c.hits),
		"misses": atomic.LoadInt64(&c.misses),
	}
}

// GetPreparedStmt retrieves or creates a prepared statement
// In pgx, prepared statements work differently - this is for API compatibility
func GetPreparedStmt(query string, isSelect bool) (string, error) {
	cached := GlobalStmtCache.Get(query)
	if cached != nil {
		cached.lastUsed = time.Now()
		return cached.query, nil
	}

	paramCount := countParameters(query)
	GlobalStmtCache.Add(query, isSelect, paramCount)

	return query, nil
}

// countParameters counts the number of parameters in a query
func countParameters(query string) int {
	count := 0
	inString := false
	escape := false

	for i := 0; i < len(query); i++ {
		c := query[i]

		if c == '\'' && !escape {
			inString = !inString
		}

		if c == '\\' {
			escape = !escape
		} else {
			escape = false
		}

		if !inString && c == '$' && i+1 < len(query) {
			if i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
				count++
			}
		}
	}

	return count
}
