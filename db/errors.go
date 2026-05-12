package db

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNoRows    = errors.New("db: no rows found")
	ErrEmptyRows = errors.New("db: batch rows must not be empty")
)

// IsNoRows reports whether err represents a "no rows" result.
func IsNoRows(err error) bool {
	return errors.Is(err, ErrNoRows) || errors.Is(err, pgx.ErrNoRows)
}

func pgErrCode(err error) string {
	var e *pgconn.PgError
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// IsUniqueViolation reports a unique constraint violation (23505).
func IsUniqueViolation(err error) bool { return pgErrCode(err) == "23505" }

// IsForeignKeyViolation reports a foreign key violation (23503).
func IsForeignKeyViolation(err error) bool { return pgErrCode(err) == "23503" }

// IsNotNullViolation reports a NOT NULL violation (23502).
func IsNotNullViolation(err error) bool { return pgErrCode(err) == "23502" }

// IsCheckViolation reports a CHECK constraint violation (23514).
func IsCheckViolation(err error) bool { return pgErrCode(err) == "23514" }

// IsDeadlock reports a PostgreSQL deadlock (40P01).
func IsDeadlock(err error) bool { return pgErrCode(err) == "40P01" }

// IsSerializationFailure reports a serialization failure (40001).
// These are safe to retry in SERIALIZABLE transactions.
func IsSerializationFailure(err error) bool { return pgErrCode(err) == "40001" }

// IsInvalidTextRepresentation reports invalid input syntax (22P02).
// Commonly caused by malformed UUIDs or invalid enum values.
func IsInvalidTextRepresentation(err error) bool { return pgErrCode(err) == "22P02" }

// IsUndefinedTable reports a missing table error (42P01).
func IsUndefinedTable(err error) bool { return pgErrCode(err) == "42P01" }

// IsConnectionException reports a connection failure (08xxx class).
func IsConnectionException(err error) bool {
	code := pgErrCode(err)
	return len(code) >= 2 && code[:2] == "08"
}

// PgError unwraps the raw pgconn.PgError from err.
// The error gives access to Code, Detail, Hint, Schema, Table, Column, etc.
func PgError(err error) (*pgconn.PgError, bool) {
	var e *pgconn.PgError
	return e, errors.As(err, &e)
}

func tooManyParamsErr(got int) error {
	return fmt.Errorf("db: query has %d parameters, exceeding PostgreSQL limit of 65535", got)
}
