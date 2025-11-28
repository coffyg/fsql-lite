# fsql-lite

Lightweight PostgreSQL ORM wrapper around [pgxpool](https://github.com/jackc/pgx) - designed as a drop-in replacement for [fsql](https://github.com/coffyg/fsql) without sqlx dependency.

## Why fsql-lite?

- **pgx native**: Uses pgx v5 directly instead of sqlx, avoiding connection pool issues with PgBouncer transaction pooling mode
- **API compatible**: Drop-in replacement for fsql - same function signatures, same struct tags
- **Minimal overhead**: Uses [sqlx/reflectx](https://github.com/jmoiron/sqlx) for efficient struct scanning without the full sqlx stack
- **PgBouncer friendly**: Uses `QueryExecModeSimpleProtocol` by default - no prepared statements

## Installation

```bash
go get github.com/coffyg/fsql-lite
```

## Quick Start

```go
package main

import (
    "context"
    fsql "github.com/coffyg/fsql-lite"
)

type User struct {
    UUID     string `db:"uuid" dbMode:"i"`
    Email    string `db:"email" dbMode:"i,u"`
    Name     string `db:"name" dbMode:"i,u"`
    Created  string `db:"created_at" dbMode:"i" dbInsertValue:"NOW()"`
}

func main() {
    // Initialize database
    fsql.InitDB("postgres://user:pass@localhost:5432/mydb", fsql.DBConfig{
        MaxConnections: 50,
        MinConnections: 5,
    })
    defer fsql.Db.Close()

    // Initialize model cache (call once at startup)
    fsql.InitModelTagCache(User{}, "users")

    // Insert
    values := map[string]interface{}{
        "uuid":  "abc-123",
        "email": "user@example.com",
        "name":  "John Doe",
    }
    fsql.Insert(context.Background(), "users", values, "uuid")

    // Select single
    var user User
    fsql.Db.Get(&user, "SELECT * FROM users WHERE uuid = $1", "abc-123")

    // Select multiple
    var users []User
    fsql.Db.Select(&users, "SELECT * FROM users WHERE name LIKE $1", "%John%")
}
```

## Struct Tags

| Tag | Description |
|-----|-------------|
| `db:"column_name"` | Database column name |
| `dbMode:"i"` | Include in INSERT queries |
| `dbMode:"u"` | Include in UPDATE queries |
| `dbMode:"i,u"` | Include in both INSERT and UPDATE |
| `dbMode:"l"` | Linked field (from JOINed table) |
| `dbMode:"s"` | Skip in SELECT (computed fields) |
| `dbInsertValue:"NOW()"` | Default value for INSERT |

## Features

### Struct Scanning

Uses sqlx's battle-tested `reflectx` package for efficient struct scanning:

```go
var user User
fsql.Db.Get(&user, "SELECT * FROM users WHERE uuid = $1", id)

var users []User
fsql.Db.Select(&users, "SELECT * FROM users")
```

### Transactions

```go
err := fsql.WithTx(ctx, func(ctx context.Context, tx *fsql.Tx) error {
    tx.Exec("INSERT INTO users (uuid, email) VALUES ($1, $2)", uuid, email)
    tx.Exec("INSERT INTO logs (user_uuid, action) VALUES ($1, $2)", uuid, "created")
    return nil // commits on success, rolls back on error
})

// With retry (handles deadlocks, serialization failures)
err := fsql.WithTxRetry(ctx, func(ctx context.Context, tx *fsql.Tx) error {
    // ...
})

// Read-only transaction
err := fsql.WithReadTx(ctx, func(ctx context.Context, tx *fsql.Tx) error {
    // ...
})
```

### Batch Operations

```go
batch := fsql.NewBatchInsert("users", []string{"uuid", "email", "name"}, 100)
for _, user := range users {
    batch.Add(map[string]interface{}{
        "uuid":  user.UUID,
        "email": user.Email,
        "name":  user.Name,
    })
}
batch.Flush() // Executes bulk INSERT
```

### Query Builder

```go
// Initialize model caches
fsql.InitModelTagCache(User{}, "users")
fsql.InitModelTagCache(Profile{}, "profiles")

// Build query with JOINs
query := fsql.SelectBase("users", "").
    Join("profiles", "p", "p.user_uuid = users.uuid").
    Where("users.active = true").
    Build()

var results []UserWithProfile
fsql.Db.Select(&results, query)
```

### Filters (API pagination)

```go
filters := &fsql.Filter{
    "email[$like]": "%@example.com",
    "created_at[$gte]": "2024-01-01",
}
sort := &fsql.Sort{"created_at": "DESC"}

query, args, _ := fsql.FilterQuery(baseQuery, "users", filters, sort, "users", 20, 1)
fsql.Db.Select(&users, query, args...)

// Count query
countQuery := fsql.BuildFilterCount(query)
var count int
fsql.Db.QueryRow(countQuery, args...).Scan(&count)
```

Filter operators:
- `$eq` / `€eq` - Equals (€ = case-insensitive)
- `$like` / `€like` - LIKE pattern
- `$prefix` / `€prefix` - Starts with
- `$suffix` / `€suffix` - Ends with
- `$gt`, `$gte`, `$lt`, `$lte` - Comparisons
- `$in`, `$nin` - IN / NOT IN array
- `$ne` - Not equals

### Safe Wrappers (with timeouts)

```go
// Default 30s timeout
fsql.SafeExec("DELETE FROM sessions WHERE expired_at < NOW()")
fsql.SafeGet(&user, "SELECT * FROM users WHERE uuid = $1", id)
fsql.SafeSelect(&users, "SELECT * FROM users WHERE active = true")

// Custom timeout
fsql.SafeExecTimeout(5*time.Second, query, args...)
fsql.SafeGetTimeout(10*time.Second, &user, query, args...)
```

### JSONB Support

fsql-lite automatically handles JSONB fields with `sql.Scanner` interface:

```go
type Settings struct {
    Theme string `json:"theme"`
    Notifications bool `json:"notifications"`
}

func (s *Settings) Scan(value interface{}) error {
    return json.Unmarshal(value.([]byte), s)
}

func (s Settings) Value() (driver.Value, error) {
    return json.Marshal(s)
}

type User struct {
    UUID     string    `db:"uuid"`
    Settings *Settings `db:"settings"` // Automatically scanned from JSONB
}
```

## API Reference

### Initialization

| Function | Description |
|----------|-------------|
| `InitDB(connString, config)` | Initialize connection pool |
| `InitDBWithPool(url, max, min)` | Initialize with explicit pool settings |
| `InitModelTagCache(model, table)` | Cache struct metadata for a table |
| `CloseDB()` | Close connection pool |

### Query Methods

| Method | Description |
|--------|-------------|
| `Db.Get(dest, query, args...)` | Scan single row into struct |
| `Db.Select(dest, query, args...)` | Scan multiple rows into slice |
| `Db.Exec(query, args...)` | Execute without returning rows |
| `Db.Query(query, args...)` | Execute returning pgx.Rows |
| `Db.QueryRow(query, args...)` | Execute returning single row |

### Transaction Methods

| Method | Description |
|--------|-------------|
| `BeginTx(ctx)` | Start transaction |
| `WithTx(ctx, fn)` | Execute function in transaction |
| `WithTxRetry(ctx, fn)` | Transaction with retry on conflicts |
| `WithReadTx(ctx, fn)` | Read-only transaction |
| `tx.Commit()` | Commit transaction |
| `tx.Rollback()` | Rollback transaction |
| `tx.Get(dest, query, args...)` | Query single row in transaction |
| `tx.Select(dest, query, args...)` | Query multiple rows in transaction |

## Migration from fsql

1. Replace import: `github.com/coffyg/fsql` → `github.com/coffyg/fsql-lite`
2. Change connection string if needed (pgx format)
3. That's it - same struct tags, same function signatures

## Performance

The scanner uses several optimizations:
- **Traversal caching**: Field paths cached per (type, columns) combination
- **reflectx mapper**: sqlx's efficient struct mapping
- **Zero-alloc string matching**: Case-insensitive search without allocations
- **Pool recycling**: String builders and slices reused via sync.Pool

Run benchmarks:
```bash
./bench.sh
```

## Testing

```bash
./test.sh
```

Requires PostgreSQL running on localhost:5433 (or modify `setup_test_db.sh`).

## License

MIT
