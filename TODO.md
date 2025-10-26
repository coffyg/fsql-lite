# fsql-lite TODO

**Goal:** Lightweight ORM wrapper around pgxpool - no database/sql, no sqlx, no replica complexity. Direct connection pool access with proper timeout behavior.

## 1. Core Connection Management (`fsql.go`)

- [ ] **Global DB variable** - `var DB *pgxpool.Pool`
- [ ] **InitDB function** - Direct pgxpool.NewWithConfig
  - ParseConfig from URL
  - Set MaxConns/MinConns
  - QueryExecModeSimpleProtocol
  - StatementCacheCapacity = 0
  - Ping to verify connection
- [ ] **CloseDB function** - Clean shutdown
- [ ] **GetPoolStats function** - Return pgxpool.Stat() metrics
  - TotalConns, AcquiredConns, IdleConns
  - EmptyAcquireCount, EmptyAcquireWaitTime

## 2. ORM Layer - Struct Tags (`orm.go` + `cache.go`)

### Struct Tag Parsing
- [ ] **InitModelTagCache(model interface{}, tableName string)**
  - Parse struct tags: `db:"column"` `dbMode:"i,u,s,l"` `dbInsertValue:"NOW()"`
  - Cache struct metadata globally
  - Support mode flags:
    - `i` = insert field
    - `u` = update field
    - `s` = skip (select only, no insert/update)
    - `l` = linked field (for JOINs)
  - Store default insert values

### Query Generation
- [ ] **GetInsertQuery(tableName string, valuesMap map[string]interface{}, returning string) (string, []interface{})**
  - Generate INSERT query from struct metadata
  - Support JSONB type detection and `::jsonb` casting
  - Handle default values (NOW(), NULL, true, false, DEFAULT)
  - Support RETURNING clause

- [ ] **GetUpdateQuery(tableName string, valuesMap map[string]interface{}, returning string) (string, []interface{})**
  - Generate UPDATE query with WHERE on returning field (uuid)
  - Support JSONB type detection and `::jsonb` casting
  - Return RETURNING clause

### Field Helpers
- [ ] **GetSelectFields(tableName, aliasTableName string) ([]string, []string)**
  - Return field list for SELECT queries
  - Support table aliases for JOINs
  - Return (fieldStrings, fieldNames)

- [ ] **GetInsertFields(tableName string) ([]string, []string)**
  - Return insert field list

- [ ] **GetUpdateFields(tableName string) ([]string, []string)**
  - Return update field list

### JSONB Support
- [ ] **isJSONBType(val interface{}) bool**
  - Check if value implements driver.Valuer
  - Detect LocalizedText, Dictionary types
  - Return true if should be cast as ::jsonb

### Link System
- [ ] **Store linked fields** in model metadata
  - Map FieldName → TableAlias
  - Used for JOIN query building

## 3. Filter System (`filters.go`)

### Filter Types
```go
type Filter map[string]interface{}
type Sort map[string]string
```

### Operators Support
- [ ] **Basic operators:**
  - `$eq` or default (empty) = equals
  - `$ne` = not equals
  - `$gt`, `$gte`, `$lt`, `$lte` = comparisons
  - `$in`, `$nin` = array operations (use pgx arrays)

- [ ] **String operators:**
  - `$like` = LIKE pattern
  - `$prefix` = LIKE 'value%'
  - `$suffix` = LIKE '%value'

- [ ] **Case-insensitive (€ prefix):**
  - `€eq`, `€like`, `€prefix`, `€suffix`
  - Use LOWER() on both sides

### Filter Query Building
- [ ] **FilterQuery(baseQuery, t string, filters *Filter, sort *Sort, table string, perPage, page int) (string, []interface{}, error)**
  - Build WHERE clause from filters
  - Build ORDER BY from sort map
  - Add LIMIT/OFFSET for pagination
  - Return (query, args, error)

- [ ] **BuildFilterCount(baseQuery string) string**
  - Strip LIMIT/OFFSET/ORDER BY
  - Wrap in SELECT COUNT(*) FROM (...)
  - Return count query

- [ ] **constructConditions(t string, filters *Filter, table string) ([]string, []interface{}, error)**
  - Parse filter map into WHERE conditions
  - Handle operators and build placeholders
  - Return conditions and args

## 4. Query Execution Helpers (`query.go`)

### Direct pgxpool execution
- [ ] **Insert(ctx context.Context, tableName string, values map[string]interface{}, returning string) error**
  - Call GetInsertQuery
  - Execute with DB.QueryRow(ctx, query, args...)
  - Scan RETURNING value

- [ ] **Update(ctx context.Context, tableName string, values map[string]interface{}, returning string) error**
  - Call GetUpdateQuery
  - Execute with DB.QueryRow(ctx, query, args...)
  - Scan RETURNING value

- [ ] **SelectOne(ctx context.Context, dest interface{}, query string, args ...interface{}) error**
  - Execute query
  - Scan single row into dest

- [ ] **SelectMany(ctx context.Context, dest interface{}, query string, args ...interface{}) error**
  - Execute query
  - Scan multiple rows into slice

## 5. Query Builder (`builder.go`)

- [ ] **SelectBase(table string) *QueryBuilder**
  - Fluent query builder for SELECT with JOINs

- [ ] **QueryBuilder.Where(condition string) *QueryBuilder**
  - Add WHERE conditions

- [ ] **QueryBuilder.Join/Left(table, alias, on string) *QueryBuilder**
  - Add JOIN clauses

- [ ] **QueryBuilder.Build() string**
  - Generate final SELECT query with all fields from base and joined tables

## 6. Supporting Code

### Utils
- [ ] **extractTableName(query string) string**
  - Extract table name from SQL query for logging

- [ ] **GenNewUUID(table string) string**
  - Generate UUID for new records

### Scanner (`scanner.go`)
- [ ] **Row/Rows scanning helpers** for pgx
  - Map pgx.Rows to struct fields
  - Handle nested structs for linked fields

## 7. go.mod Setup

```go
module github.com/coffyg/fsql-lite

require (
    github.com/jackc/pgx/v5 v5.x.x
    github.com/google/uuid v1.x.x
)
```

## 8. Testing

- [ ] Basic insert/update tests
- [ ] Filter query tests
- [ ] JSONB type tests
- [ ] Link/JOIN tests
- [ ] Pool stats tests

---

## Key Differences from fsql

**What we DROP:**
- ❌ No database/sql compatibility layer
- ❌ No sqlx dependency
- ❌ No read replica support
- ❌ No health checking
- ❌ No Safe* wrapper functions
- ❌ No prepared statement caching
- ❌ No extensive sync.Pool optimizations
- ❌ No transaction helpers (use pgxpool.Begin directly)

**What we KEEP:**
- ✅ Struct tag parsing and caching
- ✅ GetInsertQuery/GetUpdateQuery
- ✅ Filter system with operators
- ✅ JSONB type detection
- ✅ Link support for JOINs
- ✅ Query builder
- ✅ Direct pgxpool access

**Benefits:**
- Timeouts work correctly (context controls actual connection acquisition)
- Simpler stack = easier debugging
- No hidden wrapper layers
- Direct pool visibility
