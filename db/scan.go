package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/skolio/pgkit/qb"
)

// QueryInto executes b.BuildSelect() and scans each row into T using
// struct field tags `db:"column_name"` (pgx RowToStructByName).
//
//	type Student struct {
//	    ID   uuid.UUID `db:"id"`
//	    Name string    `db:"name"`
//	}
//	students, err := db.QueryInto[Student](ctx, client, client.QB("students").Limit(20))
func QueryInto[T any](ctx context.Context, c *Client, b *qb.Builder) ([]T, error) {
	sql, args, err := b.BuildSelect()
	if err != nil {
		return nil, fmt.Errorf("db/scan: build SELECT: %w", err)
	}
	rows, err := c.mustPool("read").Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("db/scan: query: %w", err)
	}
	defer rows.Close()

	results, err := pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if err != nil {
		return nil, fmt.Errorf("db/scan: collect rows: %w", err)
	}
	return results, nil
}

// QueryOneInto executes b.BuildSelect() LIMIT 1 and scans the first row into T.
// Returns ErrNoRows if no row is found.
func QueryOneInto[T any](ctx context.Context, c *Client, b *qb.Builder) (*T, error) {
	b.Limit(1)
	sql, args, err := b.BuildSelect()
	if err != nil {
		return nil, fmt.Errorf("db/scan: build SELECT: %w", err)
	}
	rows, err := c.mustPool("read").Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("db/scan: query: %w", err)
	}
	defer rows.Close()

	result, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, fmt.Errorf("db/scan: collect row: %w", err)
	}
	return &result, nil
}

// InsertInto executes BuildInsert and scans RETURNING columns into T.
// The Builder must have Returning(…) or ReturningAll() set.
//
//	type CreatedOrder struct {
//	    ID        uuid.UUID `db:"id"`
//	    CreatedAt time.Time `db:"created_at"`
//	}
//	order, err := db.InsertInto[CreatedOrder](ctx, client,
//	    client.QB("orders").ReturningAll(),
//	    map[string]any{"user_id": uid, "total": 99.99},
//	)
func InsertInto[T any](ctx context.Context, c *Client, b *qb.Builder, data map[string]any) (*T, error) {
	sql, args, err := b.BuildInsert(data)
	if err != nil {
		return nil, fmt.Errorf("db/scan: build INSERT: %w", err)
	}
	rows, err := c.mustPool("write").Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("db/scan: insert query: %w", err)
	}
	defer rows.Close()

	result, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, fmt.Errorf("db/scan: insert collect: %w", err)
	}
	return &result, nil
}

// UpdateInto executes BuildUpdate and scans the first RETURNING row into T.
// Requires Returning(…) or ReturningAll() on the Builder.
func UpdateInto[T any](ctx context.Context, c *Client, b *qb.Builder, data map[string]any) (*T, error) {
	sql, args, err := b.BuildUpdate(data)
	if err != nil {
		return nil, fmt.Errorf("db/scan: build UPDATE: %w", err)
	}
	rows, err := c.mustPool("write").Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("db/scan: update query: %w", err)
	}
	defer rows.Close()

	result, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, fmt.Errorf("db/scan: update collect: %w", err)
	}
	return &result, nil
}

// TxQueryInto executes b.BuildSelect() inside a raw pgx.Tx and scans rows into T.
// Use when you need the low-level pgx transaction directly.
func TxQueryInto[T any](ctx context.Context, tx pgx.Tx, b *qb.Builder) ([]T, error) {
	sql, args, err := b.BuildSelect()
	if err != nil {
		return nil, fmt.Errorf("db/scan: build SELECT: %w", err)
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("db/scan: tx query: %w", err)
	}
	defer rows.Close()

	results, err := pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if err != nil {
		return nil, fmt.Errorf("db/scan: tx collect rows: %w", err)
	}
	return results, nil
}

// ScanUUID scans a single uuid.UUID from a QueryRow result.
func ScanUUID(row pgx.Row) (uuid.UUID, error) {
	var id uuid.UUID
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNoRows
		}
		return uuid.Nil, fmt.Errorf("db/scan: UUID scan: %w", err)
	}
	return id, nil
}
