# pgkit

PostgreSQL toolkit for Go built on [pgx v5](https://github.com/jackc/pgx). Two packages:

- **`qb`** — standalone query builder, no database dependency
- **`db`** — connection pool manager + query execution built on top of `qb`

```
go get github.com/skolio/pgkit
```

Requires Go 1.23+.

---

## qb — Query Builder

Use `qb` alone when you only need to build SQL strings (e.g. in a repository layer that receives a `*pgxpool.Pool` externally).

```go
import "github.com/skolio/pgkit/qb"
```

### SELECT

```go
sql, args, err := qb.New("users").
    Columns("id", "name", "email").
    Where(qb.Where("school_id", qb.OpEq, schoolID)).
    Where(qb.Where("active", qb.OpEq, true)).
    OrderBy("name", qb.Asc, qb.NullsLast).
    Limit(20).
    Offset(40).
    BuildSelect()
// SELECT "id", "name", "email" FROM "users"
// WHERE "school_id" = $1 AND "active" = $2
// ORDER BY "name" ASC NULLS LAST LIMIT 20 OFFSET 40
```

### INSERT

```go
sql, args, err := qb.New("users").
    BuildInsert(map[string]any{
        "name":  "Alice",
        "email": "alice@example.com",
    })
// INSERT INTO "users" ("email", "name") VALUES ($1, $2) RETURNING "id"
```

### UPDATE

```go
sql, args, err := qb.New("users").
    Where(qb.Where("id", qb.OpEq, userID)).
    BuildUpdate(map[string]any{"name": "Bob"})
// UPDATE "users" SET "name" = $1 WHERE "id" = $2
```

### DELETE

```go
sql, args, err := qb.New("users").
    Where(qb.Where("id", qb.OpEq, userID)).
    BuildDelete()
// DELETE FROM "users" WHERE "id" = $1
```

### WHERE conditions

```go
// Equality / comparison
qb.Where("age", qb.OpGte, 18)

// IN / NOT IN
qb.WhereIn("status", []string{"active", "pending"})

// NULL checks
qb.WhereNull("deleted_at")
qb.WhereNotNull("verified_at")

// BETWEEN
qb.WhereBetween("created_at", from, to)

// OR group
qb.OrGroup(
    qb.Where("status", qb.OpEq, "active"),
    qb.Where("status", qb.OpEq, "pending"),
)
// → ("status" = $1 OR "status" = $2)

// EXISTS subquery
qb.WhereExists(qb.New("orders").Where(qb.Where("user_id", qb.OpEq, uid)))

// Raw fragment
qb.WhereRaw("lower(email) = ?", email)

// JSONB
qb.WhereJSONContains("metadata", map[string]any{"role": "admin"})
qb.WhereJSONHasKey("settings", "theme")

// Full-text search
qb.WhereTextSearch("search_vector", "golang & postgres")
```

### JOIN

```go
qb.New("orders o").
    InnerJoin("users u", "u.id = o.user_id").
    LeftJoin("coupons c", "c.id = o.coupon_id").
    Columns("o.id", "u.name", "o.total")
```

### ON CONFLICT (upsert)

```go
qb.New("users").
    OnConflict(`(email) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()`).
    BuildInsert(data)
```

### RETURNING

```go
qb.New("orders").ReturningAll().BuildInsert(data)              // RETURNING *
qb.New("orders").Returning("id", "created_at").BuildInsert(data)
qb.New("orders").ReturningNone().BuildInsert(data)             // suppress RETURNING
```

### Window functions

```go
qb.New("orders").
    Columns("user_id", "total").
    WindowCol("RANK()", "user_id", "total DESC", "rank").
    BuildSelect()
// RANK() OVER (PARTITION BY "user_id" ORDER BY total DESC) AS "rank"
```

### CTEs

```go
qb.New("recent_users").
    With("recent_users", "SELECT id FROM users WHERE created_at > $1", cutoff).
    BuildSelect()
```

### UNION

```go
active := qb.New("users").Where(qb.Where("status", qb.OpEq, "active"))
pending := qb.New("users").Where(qb.Where("status", qb.OpEq, "pending"))
active.UnionAll(pending).BuildSelect()
```

### Locking

```go
qb.New("accounts").
    Where(qb.Where("id", qb.OpEq, id)).
    ForUpdate(qb.NoWait).
    BuildSelect()
// SELECT * FROM "accounts" WHERE "id" = $1 FOR UPDATE NOWAIT
```

---

## db — Connection Pool + Execution

`db` wraps `qb` with named connection pools, transactions, and typed scanning.

```go
import (
    "github.com/skolio/pgkit/db"
    "github.com/skolio/pgkit/qb"
)
```

### Create a client

Pass one or more named pools. Each pool has its own DSN and credentials — no restriction on count or naming.

```go
client, err := db.New(ctx, db.Config{},
    db.NamedPool{
        Name: "write",
        PoolConfig: db.PoolConfig{ConnString: os.Getenv("WRITE_DATABASE_URL")},
    },
    db.NamedPool{
        Name: "read",
        PoolConfig: db.PoolConfig{ConnString: os.Getenv("READ_DATABASE_URL")},
    },
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

You can register as many pools as you need with any names:

```go
client, err := db.New(ctx, db.Config{},
    db.NamedPool{Name: "admin",     PoolConfig: db.PoolConfig{ConnString: adminDSN, MaxConns: 5}},
    db.NamedPool{Name: "write",     PoolConfig: db.PoolConfig{ConnString: writeDSN, MaxConns: 25}},
    db.NamedPool{Name: "read",      PoolConfig: db.PoolConfig{ConnString: readDSN,  MaxConns: 25}},
    db.NamedPool{Name: "analytics", PoolConfig: db.PoolConfig{ConnString: analyticsDSN}},
)
```

#### PoolConfig fields

| Field | Default | Description |
|---|---|---|
| `ConnString` | required | Full PostgreSQL DSN |
| `MaxConns` | 10 | Maximum open connections |
| `MinConns` | 2 | Minimum idle connections |
| `MaxConnIdleTime` | 5m | Close idle connections after |
| `MaxConnLifetime` | 1h | Recycle connections after |
| `HealthCheckPeriod` | 1m | Interval between background pings |
| `ConnectTimeout` | 20s | Timeout for connect + startup ping |
| `ForceIPv4` | false | Skip IPv6 addresses (use on GCP Cloud Run) |

#### Config fields

| Field | Default | Description |
|---|---|---|
| `QueryTimeout` | 0 (none) | Per-query context timeout |
| `Logger` | `slog.Default()` | Query debug/error logger |

### Query (SELECT)

```go
// Uses the "read" pool
rows, err := client.Query(ctx, client.QB("users").
    Where(qb.Where("active", qb.OpEq, true)).
    Limit(10))
// rows → []map[string]any

// Use immediately after a write (avoids read-replica lag)
rows, err := client.QueryWrite(ctx, client.QB("users").Where(...))

// Target a specific pool by name
rows, err := client.QueryPool(ctx, "analytics", client.QB("events").Limit(100))

// Single row — returns ErrNoRows if not found
row, err := client.QueryOne(ctx, client.QB("users").Where(qb.Where("id", qb.OpEq, id)))
```

### Write (INSERT / UPDATE / DELETE)

```go
// INSERT — returns the generated uuid from RETURNING "id"
id, err := client.Insert(ctx, client.QB("users"), map[string]any{
    "name":  "Alice",
    "email": "alice@example.com",
})

// Batch INSERT
id, err := client.InsertBatch(ctx, client.QB("tags"), []map[string]any{
    {"name": "go"},
    {"name": "postgres"},
})

// UPDATE — returns rows affected
n, err := client.Update(ctx,
    client.QB("users").Where(qb.Where("id", qb.OpEq, id)),
    map[string]any{"name": "Bob"})

// DELETE — returns rows affected
n, err := client.Delete(ctx,
    client.QB("users").Where(qb.Where("id", qb.OpEq, id)))
```

### Raw SQL

```go
// Raw write on the "write" pool
n, err := client.ExecSQL(ctx, "UPDATE counters SET val = val + 1 WHERE key = $1", "hits")

// Raw write on any named pool
n, err := client.ExecPoolSQL(ctx, "admin", "VACUUM ANALYZE users")

// Raw SELECT on the "read" pool
rows, err := client.QuerySQL(ctx, "SELECT id, name FROM users WHERE active = $1", []any{true})
```

### Typed scanning with generics

```go
type User struct {
    ID    uuid.UUID `db:"id"`
    Name  string    `db:"name"`
    Email string    `db:"email"`
}

// Scan all rows into []User
users, err := db.QueryInto[User](ctx, client, client.QB("users").Limit(20))

// Scan one row — returns ErrNoRows if not found
user, err := db.QueryOneInto[User](ctx, client,
    client.QB("users").Where(qb.Where("id", qb.OpEq, id)))

// Scan INSERT RETURNING into a struct
type Created struct {
    ID        uuid.UUID `db:"id"`
    CreatedAt time.Time `db:"created_at"`
}
result, err := db.InsertInto[Created](ctx, client,
    client.QB("users").Returning("id", "created_at"),
    map[string]any{"name": "Alice"},
)
```

### Transactions

```go
// Transaction on the "write" pool
err := client.WithTx(ctx, func(tx db.Tx) error {
    id, err := tx.Insert(ctx, tx.QB("orders"), orderData)
    if err != nil {
        return err
    }
    _, err = tx.Update(ctx,
        tx.QB("inventory").Where(qb.Where("product_id", qb.OpEq, productID)),
        map[string]any{"stock": stock - 1})
    return err
})

// Transaction on any named pool
err := client.WithPoolTx(ctx, "admin", func(tx db.Tx) error {
    _, err := tx.ExecRaw(ctx, "CREATE INDEX CONCURRENTLY ...")
    return err
})

// Serializable with automatic retry on 40001
err := client.WithRetryTx(ctx, 3, func(tx db.Tx) error {
    return transfer(ctx, tx, from, to, amount)
})

// Custom isolation level
err := client.WithTxOpts(ctx, db.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx db.Tx) error {
    ...
})
```

#### Savepoints

```go
err := client.WithTx(ctx, func(tx db.Tx) error {
    tx.Savepoint(ctx, "before_charge")
    if err := charge(ctx, tx); err != nil {
        tx.RollbackTo(ctx, "before_charge")
    }
    tx.ReleaseSavepoint(ctx, "before_charge")
    return nil
})
```

#### Advisory locks

```go
// Blocks until the lock is acquired; released automatically when the tx ends
err := client.WithTx(ctx, func(tx db.Tx) error {
    return db.AcquireAdvisoryLock(ctx, tx, userID)
})

// Non-blocking — returns false immediately if already held
err := client.WithTx(ctx, func(tx db.Tx) error {
    ok, err := db.TryAdvisoryLock(ctx, tx, userID)
    if !ok {
        return errors.New("already processing")
    }
    ...
})
```

### Batch queries

```go
b := db.NewBatch()
b.AddSelect(client.QB("users").Limit(5))
b.AddInsert(client.QB("events"), eventData)
b.Add("SELECT pg_sleep(0)")

results, err := client.SendWrite(ctx, b) // [][]map[string]any, one entry per queued query
results, err := client.SendRead(ctx, b)
```

### LISTEN / NOTIFY

```go
// Publish a notification
err := client.Notify(ctx, "orders", `{"id":"123","status":"paid"}`)

// Subscribe — blocks until ctx is cancelled
err := client.Listen(ctx, "orders", func(n db.Notification) error {
    fmt.Println(n.Channel, n.Payload)
    return nil
})

// Multiple channels at once
err := client.ListenMulti(ctx, []string{"orders", "shipments"}, handler)
```

### Get a raw pool

```go
pool := client.Pool("analytics") // *pgxpool.Pool — use pgx directly
conn, err := pool.Acquire(ctx)
defer conn.Release()
```

### Health check

```go
// Pings all registered pools concurrently
if err := client.HealthCheck(ctx); err != nil {
    log.Println("database unhealthy:", err)
}
```

---

## Error helpers

```go
db.ErrNoRows    // no rows returned
db.ErrEmptyRows // empty slice passed to batch insert
db.ErrEmptyData // empty map passed to insert/update

db.IsNotFound(err)              // true if err is ErrNoRows
db.IsUniqueViolation(err)       // PostgreSQL error 23505
db.IsForeignKeyViolation(err)   // PostgreSQL error 23503
db.IsSerializationFailure(err)  // PostgreSQL error 40001
