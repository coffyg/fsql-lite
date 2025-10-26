# fsql-lite TODO

**Goal:** Lightweight ORM wrapper around pgxpool - no database/sql, no sqlx, no replica complexity. Direct connection pool access with proper timeout behavior.

## 1. Core Connection Management (`fsql.go`)

- [x] **Global DB variable** - `var DB *pgxpool.Pool`
- [x] **InitDB function** - Direct pgxpool.NewWithConfig
  - ParseConfig from URL
  - Set MaxConns/MinConns
  - QueryExecModeSimpleProtocol
  - StatementCacheCapacity = 0
  - Ping to verify connection
- [x] **CloseDB function** - Clean shutdown
- [x] **GetPoolStats function** - Return pgxpool.Stat() metrics
  - TotalConns, AcquiredConns, IdleConns
  - EmptyAcquireCount, AcquireDuration

## 2. ORM Layer - Struct Tags (`orm.go` + `cache.go`)

### Struct Tag Parsing
- [x] **InitModelTagCache(model interface{}, tableName string)** - Copied from fsql
  - Parse struct tags: `db:"column"` `dbMode:"i,u,s,l"` `dbInsertValue:"NOW()"`
  - Cache struct metadata globally
  - Support mode flags:
    - `i` = insert field
    - `u` = update field
    - `s` = skip (select only, no insert/update)
    - `l` = linked field (for JOINs)
  - Store default insert values

### Query Generation
- [x] **GetInsertQuery(tableName string, valuesMap map[string]interface{}, returning string) (string, []interface{})** - Copied from fsql
  - Generate INSERT query from struct metadata
  - Support JSONB type detection and `::jsonb` casting
  - Handle default values (NOW(), NULL, true, false, DEFAULT)
  - Support RETURNING clause

- [x] **GetUpdateQuery(tableName string, valuesMap map[string]interface{}, returning string) (string, []interface{})** - Copied from fsql
  - Generate UPDATE query with WHERE on returning field (uuid)
  - Support JSONB type detection and `::jsonb` casting
  - Return RETURNING clause

### Field Helpers
- [x] **GetSelectFields(tableName, aliasTableName string) ([]string, []string)** - Copied from fsql
  - Return field list for SELECT queries
  - Support table aliases for JOINs
  - Return (fieldStrings, fieldNames)

- [x] **GetInsertFields(tableName string) ([]string, []string)** - Copied from fsql
  - Return insert field list

- [x] **GetUpdateFields(tableName string) ([]string, []string)** - Copied from fsql
  - Return update field list

### JSONB Support
- [x] **isJSONBType(val interface{}) bool** - Copied from fsql
  - Check if value implements driver.Valuer
  - Detect LocalizedText, Dictionary types
  - Return true if should be cast as ::jsonb

### Link System
- [x] **Store linked fields** in model metadata - Copied from fsql
  - Map FieldName → TableAlias
  - Used for JOIN query building

## 3. Filter System (`filters.go`)

### Filter Types
```go
type Filter map[string]interface{}
type Sort map[string]string
```

### Operators Support
- [x] **Basic operators:** - Copied from fsql, adapted for pgx
  - `$eq` or default (empty) = equals
  - `$ne` = not equals
  - `$gt`, `$gte`, `$lt`, `$lte` = comparisons
  - `$in`, `$nin` = array operations (pgx handles arrays natively)

- [x] **String operators:** - Copied from fsql
  - `$like` = LIKE pattern
  - `$prefix` = LIKE 'value%'
  - `$suffix` = LIKE '%value'

- [x] **Case-insensitive (€ prefix):** - Copied from fsql
  - `€eq`, `€like`, `€prefix`, `€suffix`
  - Use LOWER() on both sides

### Filter Query Building
- [x] **FilterQuery(baseQuery, t string, filters *Filter, sort *Sort, table string, perPage, page int) (string, []interface{}, error)** - Copied from fsql
  - Build WHERE clause from filters
  - Build ORDER BY from sort map
  - Add LIMIT/OFFSET for pagination
  - Return (query, args, error)

- [x] **BuildFilterCount(baseQuery string) string** - Copied from fsql
  - Strip LIMIT/OFFSET/ORDER BY
  - Wrap in SELECT COUNT(*) FROM (...)
  - Return count query

- [x] **GetFilterCount(query string, args []interface{}) (int, error)** - Adapted for pgx
  - Uses DB.QueryRow(context.Background(), ...) instead of sqlx

- [x] **constructConditions(t string, filters *Filter, table string) ([]string, []interface{}, error)** - Copied from fsql
  - Parse filter map into WHERE conditions
  - Handle operators and build placeholders
  - Return conditions and args

## 4. Query Execution Helpers (`query.go`)

### Direct pgxpool execution
- [x] **Insert(ctx context.Context, tableName string, values map[string]interface{}, returning string) error**
  - Call GetInsertQuery
  - Execute with DB.QueryRow(ctx, query, args...) or DB.Exec
  - Scan RETURNING value into values map

- [x] **Update(ctx context.Context, tableName string, values map[string]interface{}, returning string) error**
  - Call GetUpdateQuery
  - Execute with DB.QueryRow(ctx, query, args...) or DB.Exec
  - Scan RETURNING value into values map

- [x] **SelectOne(ctx context.Context, dest interface{}, query string, args ...interface{}) error**
  - Execute query with DB.Query
  - Scan single row into dest using StructScan

- [x] **SelectMany(ctx context.Context, dest interface{}, query string, args ...interface{}) error**
  - Execute query with DB.Query
  - Scan multiple rows into slice using StructsScan

- [x] **Exec(ctx context.Context, query string, args ...interface{}) error**
  - Execute query without returning rows

## 5. Query Builder (`builder.go`)

- [x] **SelectBase(table string) *QueryBuilder** - Already in orm.go
  - Fluent query builder for SELECT with JOINs

- [x] **QueryBuilder.Where(condition string) *QueryBuilder** - Already in orm.go
  - Add WHERE conditions

- [x] **QueryBuilder.Join/Left(table, alias, on string) *QueryBuilder** - Already in orm.go
  - Add JOIN clauses

- [x] **QueryBuilder.Build() string** - Already in orm.go
  - Generate final SELECT query with all fields from base and joined tables

## 6. Supporting Code

### Utils
- [x] **extractTableName(query string) string** - Stub in fsql.go (copy from fsql if needed)
  - Extract table name from SQL query for logging

- [x] **GenNewUUID(table string) string** - Already in orm.go
  - Generate UUID for new records

### Scanner (`scanner.go`)
- [x] **StructScan(rows pgx.Rows, dest interface{}) error**
  - Scan single row from pgx.Rows into struct using db tags
  - Note: Only works with Rows, not Row from QueryRow

- [x] **StructsScan(rows pgx.Rows, dest interface{}) error**
  - Map pgx.Rows to slice of structs using db tags
  - Handles both value and pointer slice elements

- [x] **Get(dest interface{}, query string, args ...interface{}) error**
  - Helper for scanning single row

- [x] **Select(dest interface{}, query string, args ...interface{}) error**
  - Helper for scanning multiple rows

## 7. go.mod Setup

```go
module github.com/coffyg/fsql-lite

require (
    github.com/jackc/pgx/v5 v5.x.x
    github.com/google/uuid v1.x.x
)
```

## 8. Testing

- [x] Basic insert/update tests
- [x] Filter query tests
- [x] JSONB type tests
- [x] Link/JOIN tests
- [x] Pool stats tests

**All tests passing (7/7):**
- TestAIModelInsertAndFetch - Insert and fetch by UUID
- TestListAIModel - Pagination with filters and sorting
- TestLinkedFields - LEFT JOIN with nested struct scanning
- TestQueryBuilderWhereAndJoin - Complex query builder with WHERE + JOIN
- TestFilters - $in operator with array values
- TestUpdate - Update existing records
- TestPoolStats - Connection pool metrics

Run tests: `./test.sh` or `go test`

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
