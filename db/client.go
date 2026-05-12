// Package db provides PostgreSQL connection pooling and query execution.
// It depends on github.com/skolio/pgkit/qb for query building.
//
// Use qb standalone when you only need to build SQL strings.
// Use db when you need pool management, transactions, and execution.
//
// Create a client with one or more named pools, each with its own credentials:
//
//	client, err := db.New(ctx, db.Config{},
//	    db.NamedPool{Name: "write", PoolConfig: db.PoolConfig{ConnString: writeDSN}},
//	    db.NamedPool{Name: "read",  PoolConfig: db.PoolConfig{ConnString: readDSN}},
//	)
//
// Access a pool by name:
//
//	pool := client.Pool("read") // *pgxpool.Pool
//
// Or use the high-level query API (uses "read"/"write" pools by convention):
//
//	rows, err := client.Query(ctx, client.QB("users").Limit(20))
package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/skolio/pgkit/qb"
)

// Config holds executor-level settings shared across all pools.
type Config struct {
	// QueryTimeout is applied per-query via context. 0 = no limit.
	QueryTimeout time.Duration

	// Logger is used for query debug/error logging.
	// Defaults to slog.Default() if nil.
	Logger *slog.Logger
}

// Client owns one or more named connection pools and exposes a
// query builder and execution API.
//
// Create once at startup; safe for concurrent use across goroutines.
type Client struct {
	pools *poolManager
	exec  *executor
}

// New creates a Client and establishes all provided named pools.
// At least one NamedPool is required. Each pool can have independent
// credentials, DSN, and sizing. Cancel ctx to abort connection attempts.
func New(ctx context.Context, cfg Config, pools ...NamedPool) (*Client, error) {
	pm, err := newPoolManager(ctx, pools)
	if err != nil {
		return nil, fmt.Errorf("db.New: %w", err)
	}
	return &Client{
		pools: pm,
		exec:  newExecutor(cfg.Logger, cfg.QueryTimeout),
	}, nil
}

// Pool returns the *pgxpool.Pool registered under name.
// Returns nil if no pool with that name was registered.
func (c *Client) Pool(name string) *pgxpool.Pool {
	return c.pools.Get(name)
}

// mustPool returns the pool or panics with a clear message.
func (c *Client) mustPool(name string) *pgxpool.Pool {
	p := c.pools.Get(name)
	if p == nil {
		panic(fmt.Sprintf("db: pool %q is not registered", name))
	}
	return p
}

// ─── Query builder ────────────────────────────────────────────────────────────

// QB returns a new qb.Builder targeting table.
func (c *Client) QB(table string) *qb.Builder {
	return qb.New(table)
}

// ─── Read operations ──────────────────────────────────────────────────────────

// Query executes b.BuildSelect() on the "read" pool and returns all rows.
func (c *Client) Query(ctx context.Context, b *qb.Builder) ([]map[string]any, error) {
	sql, args, err := b.BuildSelect()
	if err != nil {
		return nil, fmt.Errorf("db: build SELECT: %w", err)
	}
	return c.exec.ExecSelect(ctx, c.mustPool("read"), sql, args)
}

// QueryOne executes b.BuildSelect() on the "read" pool and returns the first row.
// Returns ErrNoRows if no rows match.
func (c *Client) QueryOne(ctx context.Context, b *qb.Builder) (map[string]any, error) {
	sql, args, err := b.BuildSelect()
	if err != nil {
		return nil, fmt.Errorf("db: build SELECT: %w", err)
	}
	return c.exec.ExecSelectOne(ctx, c.mustPool("read"), sql, args)
}

// QueryWrite executes a SELECT on the "write" pool.
// Use immediately after a write when read replicas may lag.
func (c *Client) QueryWrite(ctx context.Context, b *qb.Builder) ([]map[string]any, error) {
	sql, args, err := b.BuildSelect()
	if err != nil {
		return nil, fmt.Errorf("db: build SELECT: %w", err)
	}
	return c.exec.ExecSelect(ctx, c.mustPool("write"), sql, args)
}

// QueryPool executes b.BuildSelect() on the named pool and returns all rows.
// Use when you need to target a specific pool by name.
func (c *Client) QueryPool(ctx context.Context, poolName string, b *qb.Builder) ([]map[string]any, error) {
	sql, args, err := b.BuildSelect()
	if err != nil {
		return nil, fmt.Errorf("db: build SELECT: %w", err)
	}
	return c.exec.ExecSelect(ctx, c.mustPool(poolName), sql, args)
}

// ─── Write operations ─────────────────────────────────────────────────────────

// Insert executes b.BuildInsert(data) on the "write" pool and returns the generated UUID from
// RETURNING "id".
func (c *Client) Insert(ctx context.Context, b *qb.Builder, data map[string]any) (uuid.UUID, error) {
	sql, args, err := b.BuildInsert(data)
	if err != nil {
		return uuid.Nil, fmt.Errorf("db: build INSERT: %w", err)
	}
	return c.exec.ExecInsert(ctx, c.mustPool("write"), sql, args)
}

// InsertBatch executes b.BuildInsertBatch(rows) on the "write" pool and returns the last inserted UUID.
func (c *Client) InsertBatch(ctx context.Context, b *qb.Builder, rows []map[string]any) (uuid.UUID, error) {
	sql, args, err := b.BuildInsertBatch(rows)
	if err != nil {
		return uuid.Nil, fmt.Errorf("db: build INSERT BATCH: %w", err)
	}
	return c.exec.ExecInsert(ctx, c.mustPool("write"), sql, args)
}

// Update executes b.BuildUpdate(data) on the "write" pool and returns rows affected.
func (c *Client) Update(ctx context.Context, b *qb.Builder, data map[string]any) (int64, error) {
	sql, args, err := b.BuildUpdate(data)
	if err != nil {
		return 0, fmt.Errorf("db: build UPDATE: %w", err)
	}
	return c.exec.ExecWrite(ctx, c.mustPool("write"), sql, args)
}

// Delete executes b.BuildDelete() on the "write" pool and returns rows affected.
func (c *Client) Delete(ctx context.Context, b *qb.Builder) (int64, error) {
	sql, args, err := b.BuildDelete()
	if err != nil {
		return 0, fmt.Errorf("db: build DELETE: %w", err)
	}
	return c.exec.ExecWrite(ctx, c.mustPool("write"), sql, args)
}

// ─── Raw SQL ──────────────────────────────────────────────────────────────────

// ExecSQL executes a raw write statement on the "write" pool.
func (c *Client) ExecSQL(ctx context.Context, sql string, args ...any) (int64, error) {
	return c.exec.ExecWrite(ctx, c.mustPool("write"), sql, args)
}

// ExecPoolSQL executes a raw write statement on the named pool.
func (c *Client) ExecPoolSQL(ctx context.Context, poolName string, sql string, args ...any) (int64, error) {
	return c.exec.ExecWrite(ctx, c.mustPool(poolName), sql, args)
}

// QuerySQL executes a raw SELECT on the "read" pool and returns all rows.
func (c *Client) QuerySQL(ctx context.Context, sql string, args []any) ([]map[string]any, error) {
	return c.exec.ExecSelect(ctx, c.mustPool("read"), sql, args)
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

// HealthCheck pings all registered pools concurrently.
func (c *Client) HealthCheck(ctx context.Context) error {
	return c.pools.HealthCheck(ctx)
}

// Close closes all pools. Call during graceful shutdown.
func (c *Client) Close() {
	c.pools.Close()
}
