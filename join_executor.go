// join_executor.go - Join query optimization for fsql-lite
package fsql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// JoinQueryExecutor provides optimized execution of join queries
type JoinQueryExecutor struct {
	stmtCache           sync.Map
	queryStructureCache sync.Map
	cacheHits           int64
	cacheMisses         int64
}

// Global instance for join optimization
var joinExecutor = &JoinQueryExecutor{}

// Common join keywords for fast detection
var joinKeywords = []string{" JOIN ", " LEFT JOIN ", " RIGHT JOIN ", " INNER JOIN "}

// ExecuteJoinQueryWithCache executes a join query with caching
func ExecuteJoinQueryWithCache(query string, args []interface{}, result interface{}) error {
	return ExecuteJoinQueryOptimized(query, args, result)
}

// ExecuteJoinQueryOptimized executes a join query with full optimization
func ExecuteJoinQueryOptimized(query string, args []interface{}, result interface{}) error {
	isJoin := isJoinQuery(query)

	if !isJoin {
		return Select(result, query, args...)
	}

	cachedQuery, cachedArgs := CachedQuery(query, args)

	resultType := reflect.TypeOf(result)
	if resultType.Kind() == reflect.Ptr && resultType.Elem().Kind() == reflect.Slice {
		sliceVal := reflect.ValueOf(result).Elem()
		if sliceVal.Len() == 0 && sliceVal.Cap() < 32 {
			newSlice := reflect.MakeSlice(sliceVal.Type(), 0, 32)
			sliceVal.Set(newSlice)
		}
	}

	if cachedQuery != query {
		atomic.AddInt64(&joinExecutor.cacheHits, 1)
	} else {
		atomic.AddInt64(&joinExecutor.cacheMisses, 1)
	}

	return Select(result, cachedQuery, cachedArgs...)
}

// isJoinQuery determines if a query is a join query
func isJoinQuery(query string) bool {
	for _, keyword := range joinKeywords {
		if strings.Contains(query, keyword) {
			return true
		}
	}
	return false
}

// Model caching to reduce struct analysis overhead
var modelStructCache = sync.Map{}

// FindModelFields pre-analyzes models to speed up field mapping
func FindModelFields(modelType interface{}) {
	modelName := getModelTypeName(modelType)

	if _, ok := modelStructCache.Load(modelName); ok {
		return
	}

	structFields := analyzeModelStructure(modelType)
	modelStructCache.Store(modelName, structFields)
}

// getModelTypeName gets a string representation of model type
func getModelTypeName(modelType interface{}) string {
	if modelType == nil {
		return "nil"
	}

	t := reflect.TypeOf(modelType)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t.String()
}

// analyzeModelStructure analyzes a model's structure for fast field mapping
func analyzeModelStructure(modelType interface{}) map[string][]int {
	fieldMap := make(map[string][]int)

	t := reflect.TypeOf(modelType)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return fieldMap
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if field.PkgPath != "" {
			continue
		}

		tag := field.Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}

		dbName := strings.Split(tag, ",")[0]
		fieldMap[dbName] = append([]int{}, i)
	}

	return fieldMap
}

// JoinResultCache maintains a cache of join query results
type JoinResultCache struct {
	cache      map[string]interface{}
	expiration map[string]time.Time
	ttl        time.Duration
	mutex      sync.RWMutex
}

// NewJoinResultCache creates a new join query cache
func NewJoinResultCache(ttl time.Duration) *JoinResultCache {
	return &JoinResultCache{
		cache:      make(map[string]interface{}),
		expiration: make(map[string]time.Time),
		ttl:        ttl,
	}
}

// Get retrieves a result from the cache
func (c *JoinResultCache) Get(key string) (interface{}, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result, found := c.cache[key]
	if !found {
		return nil, false
	}

	exp, _ := c.expiration[key]
	if time.Now().After(exp) {
		return nil, false
	}

	return result, true
}

// Set adds a result to the cache
func (c *JoinResultCache) Set(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache[key] = value
	c.expiration[key] = time.Now().Add(c.ttl)
}

// OptimizeJoinQuery optimizes a join query for better performance
func OptimizeJoinQuery(query string) string {
	isSimpleJoin := strings.Contains(query, "JOIN") && !strings.Contains(query, "OUTER JOIN")

	if isSimpleJoin {
		return query + " /*+ HASH_JOIN */"
	}

	return query
}

// IdBatch is a batch of IDs for efficient loading
type IdBatch struct {
	TableName string
	Ids       []string
	KeyField  string
}

// ExecuteJoinQuery executes a join query with optimizations
func ExecuteJoinQuery(ctx context.Context, query string, args []interface{}, result interface{}) error {
	optimizedQuery := OptimizeJoinQuery(query)

	cachedQuery, cachedArgs := CachedQuery(optimizedQuery, args)

	resultType := reflect.TypeOf(result)
	if resultType.Kind() != reflect.Ptr || resultType.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("result must be a pointer to a slice")
	}

	rows, err := DB.Query(ctx, cachedQuery, cachedArgs...)
	if err != nil {
		return err
	}
	return StructsScan(rows, result)
}

// Object pool for join operations
var (
	joinResultCache = NewJoinResultCache(5 * time.Minute)
	joinQueryMutex  sync.Mutex
	joinPlanCache   = make(map[string]string)
)

// JoinPlan represents a precomputed join execution plan
type JoinPlan struct {
	MainQuery    string
	JoinQueries  []string
	JoinKeyField string
}

// ExecuteWithPrepared executes a query using optimized path
func ExecuteWithPrepared(query string, args []interface{}, result interface{}) error {
	rows, err := DB.Query(context.Background(), query, args...)
	if err != nil {
		return err
	}
	return StructsScan(rows, result)
}

// ScanRows and ScanSingle are defined in scanner.go
