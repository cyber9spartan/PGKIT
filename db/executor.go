package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type executor struct {
	logger       *slog.Logger
	queryTimeout time.Duration
}

func newExecutor(logger *slog.Logger, timeout time.Duration) *executor {
	if logger == nil {
		logger = slog.Default()
	}
	return &executor{logger: logger, queryTimeout: timeout}
}

func (e *executor) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.queryTimeout > 0 {
		return context.WithTimeout(ctx, e.queryTimeout)
	}
	return ctx, func() {}
}

func (e *executor) log(ctx context.Context, op string, dur time.Duration, err error) {
	attrs := []any{slog.String("op", op), slog.Duration("dur", dur)}
	if err != nil {
		attrs = append(attrs, slog.String("err", err.Error()))
		e.logger.ErrorContext(ctx, "db: query failed", attrs...)
		return
	}
	e.logger.DebugContext(ctx, "db: query ok", attrs...)
}

// ExecWrite executes a write statement and returns rows affected.
func (e *executor) ExecWrite(ctx context.Context, pool *pgxpool.Pool, sql string, args []any) (int64, error) {
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()

	start := time.Now()
	tag, err := pool.Exec(ctx, sql, args...)
	e.log(ctx, "write", time.Since(start), err)
	if err != nil {
		return 0, fmt.Errorf("db: exec: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ExecInsert executes INSERT … RETURNING "id" and scans the returned UUID.
func (e *executor) ExecInsert(ctx context.Context, pool *pgxpool.Pool, sql string, args []any) (uuid.UUID, error) {
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()

	start := time.Now()
	row := pool.QueryRow(ctx, sql, args...)
	var id uuid.UUID
	err := row.Scan(&id)
	e.log(ctx, "insert", time.Since(start), err)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNoRows
		}
		return uuid.Nil, fmt.Errorf("db: insert scan: %w", err)
	}
	return id, nil
}

// ExecSelect executes a SELECT and returns all rows as []map[string]any.
func (e *executor) ExecSelect(ctx context.Context, pool *pgxpool.Pool, sql string, args []any) ([]map[string]any, error) {
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()

	start := time.Now()
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		e.log(ctx, "select", time.Since(start), err)
		return nil, fmt.Errorf("db: query: %w", err)
	}
	defer rows.Close()

	results, err := collectRows(rows)
	e.log(ctx, "select", time.Since(start), err)
	return results, err
}

// ExecSelectOne executes a SELECT and returns the first row, or ErrNoRows.
func (e *executor) ExecSelectOne(ctx context.Context, pool *pgxpool.Pool, sql string, args []any) (map[string]any, error) {
	rows, err := e.ExecSelect(ctx, pool, sql, args)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNoRows
	}
	return rows[0], nil
}

// ExecTxWrite runs a write inside an existing transaction.
func (e *executor) ExecTxWrite(ctx context.Context, tx pgx.Tx, sql string, args []any) (int64, error) {
	start := time.Now()
	tag, err := tx.Exec(ctx, sql, args...)
	e.log(ctx, "tx-write", time.Since(start), err)
	if err != nil {
		return 0, fmt.Errorf("db: tx exec: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ExecTxInsert runs INSERT … RETURNING "id" inside an existing transaction.
func (e *executor) ExecTxInsert(ctx context.Context, tx pgx.Tx, sql string, args []any) (uuid.UUID, error) {
	start := time.Now()
	row := tx.QueryRow(ctx, sql, args...)
	var id uuid.UUID
	err := row.Scan(&id)
	e.log(ctx, "tx-insert", time.Since(start), err)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNoRows
		}
		return uuid.Nil, fmt.Errorf("db: tx insert scan: %w", err)
	}
	return id, nil
}

// ExecTxSelect runs a SELECT inside an existing transaction.
func (e *executor) ExecTxSelect(ctx context.Context, tx pgx.Tx, sql string, args []any) ([]map[string]any, error) {
	start := time.Now()
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		e.log(ctx, "tx-select", time.Since(start), err)
		return nil, fmt.Errorf("db: tx query: %w", err)
	}
	defer rows.Close()

	results, err := collectRows(rows)
	e.log(ctx, "tx-select", time.Since(start), err)
	return results, err
}

// collectRows scans all pgx.Rows into []map[string]any.
// [16]byte UUID values are converted to uuid.UUID string representation.
func collectRows(rows pgx.Rows) ([]map[string]any, error) {
	fields := rows.FieldDescriptions()
	var results []map[string]any

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("db: row values: %w", err)
		}
		row := make(map[string]any, len(fields))
		for i, fd := range fields {
			val := values[i]
			if b, ok := val.([16]byte); ok {
				val = uuid.UUID(b).String()
			} else if b, ok := val.([]byte); ok && len(b) == 16 {
				if u, err := uuid.FromBytes(b); err == nil {
					val = u.String()
				}
			}
			row[string(fd.Name)] = val
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: rows iteration: %w", err)
	}
	return results, nil
}
