package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/skolio/pgkit/qb"
)

// Batch accumulates multiple queries and sends them in a single network
// round-trip. Results are returned in the same order queries were added.
type Batch struct {
	entries []batchEntry
}

type batchEntry struct {
	sql  string
	args []any
}

// NewBatch creates an empty Batch.
func NewBatch() *Batch { return &Batch{} }

// Add queues a raw SQL statement.
func (b *Batch) Add(sql string, args ...any) *Batch {
	b.entries = append(b.entries, batchEntry{sql: sql, args: args})
	return b
}

// AddSelect queues a SELECT from a Builder.
func (b *Batch) AddSelect(builder *qb.Builder) error {
	sql, args, err := builder.BuildSelect()
	if err != nil {
		return fmt.Errorf("db/batch: build SELECT: %w", err)
	}
	b.entries = append(b.entries, batchEntry{sql: sql, args: args})
	return nil
}

// AddInsert queues an INSERT from a Builder + data map.
func (b *Batch) AddInsert(builder *qb.Builder, data map[string]any) error {
	sql, args, err := builder.BuildInsert(data)
	if err != nil {
		return fmt.Errorf("db/batch: build INSERT: %w", err)
	}
	b.entries = append(b.entries, batchEntry{sql: sql, args: args})
	return nil
}

// AddUpdate queues an UPDATE from a Builder + data map.
func (b *Batch) AddUpdate(builder *qb.Builder, data map[string]any) error {
	sql, args, err := builder.BuildUpdate(data)
	if err != nil {
		return fmt.Errorf("db/batch: build UPDATE: %w", err)
	}
	b.entries = append(b.entries, batchEntry{sql: sql, args: args})
	return nil
}

// AddDelete queues a DELETE from a Builder.
func (b *Batch) AddDelete(builder *qb.Builder) error {
	sql, args, err := builder.BuildDelete()
	if err != nil {
		return fmt.Errorf("db/batch: build DELETE: %w", err)
	}
	b.entries = append(b.entries, batchEntry{sql: sql, args: args})
	return nil
}

// Len returns the number of queued queries.
func (b *Batch) Len() int { return len(b.entries) }

// SendWrite sends all batched queries over the write pool in one round-trip.
func (c *Client) SendWrite(ctx context.Context, b *Batch) ([][]map[string]any, error) {
	return sendBatch(ctx, c.mustPool("write"), b)
}

// SendRead sends all batched queries over the read pool in one round-trip.
func (c *Client) SendRead(ctx context.Context, b *Batch) ([][]map[string]any, error) {
	return sendBatch(ctx, c.mustPool("read"), b)
}

func sendBatch(ctx context.Context, pool *pgxpool.Pool, b *Batch) ([][]map[string]any, error) {
	if len(b.entries) == 0 {
		return nil, ErrEmptyRows
	}

	batch := &pgx.Batch{}
	for _, e := range b.entries {
		batch.Queue(e.sql, e.args...)
	}

	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	results := make([][]map[string]any, len(b.entries))
	for i := range b.entries {
		rows, err := br.Query()
		if err != nil {
			return nil, fmt.Errorf("db/batch: query %d: %w", i, err)
		}
		collected, collectErr := collectRows(rows)
		rows.Close()
		if collectErr != nil {
			return nil, fmt.Errorf("db/batch: collect %d: %w", i, collectErr)
		}
		results[i] = collected
	}

	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("db/batch: close: %w", err)
	}
	return results, nil
}
