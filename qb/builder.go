// Package qb is a standalone PostgreSQL query builder.
// It has no dependency on any database driver beyond pgx (used only for
// identifier quoting). Import it without importing pool or connection code.
//
//	import "github.com/skolio/pgkit/qb"
//
//	sql, args, err := qb.New("students").
//	    Columns("id", "name").
//	    Where(qb.Where("school_id", qb.OpEq, schoolID)).
//	    Where(qb.WhereExists(
//	        qb.New("classes").Where(qb.Where("id", qb.OpEq, classID)),
//	    )).
//	    OrderBy("name", qb.Asc, qb.NullsLast).
//	    Limit(20).
//	    BuildSelect()
package qb

import (
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ─── Types ────────────────────────────────────────────────────────────────────

// JoinType is the SQL JOIN variant.
type JoinType string

const (
	JoinInner JoinType = "INNER"
	JoinLeft  JoinType = "LEFT"
	JoinRight JoinType = "RIGHT"
	JoinFull  JoinType = "FULL"
	JoinCross JoinType = "CROSS"
)

// SortDir is the ORDER BY direction.
type SortDir string

const (
	Asc  SortDir = "ASC"
	Desc SortDir = "DESC"
)

// NullsOrder controls NULL placement in ORDER BY.
type NullsOrder string

const (
	NullsFirst NullsOrder = "NULLS FIRST"
	NullsLast  NullsOrder = "NULLS LAST"
)

// LockMode controls row-level locking appended to SELECT.
type LockMode string

const (
	LockForUpdate      LockMode = "FOR UPDATE"
	LockForShare       LockMode = "FOR SHARE"
	LockForNoKeyUpdate LockMode = "FOR NO KEY UPDATE"
	LockForKeyShare    LockMode = "FOR KEY SHARE"
)

// LockWait controls wait behaviour when a lock cannot be acquired.
type LockWait string

const (
	Wait       LockWait = ""
	NoWait     LockWait = "NOWAIT"
	SkipLocked LockWait = "SKIP LOCKED"
)

type joinClause struct {
	kind      JoinType
	table     string
	condition string
	lateral   bool
	sub       *Builder
	alias     string
}

type orderClause struct {
	column string
	dir    SortDir
	nulls  NullsOrder
}

type cteClause struct {
	name      string
	query     string
	args      []any
	recursive bool
}

type unionClause struct {
	all bool
	qb  *Builder
}

type returningMode int

const (
	returningUnset   returningMode = iota // not explicitly set — use the Build* default
	returningID                           // RETURNING "id"
	returningAll                          // RETURNING *
	returningColumns                      // RETURNING col1, col2, …
	returningNone                         // explicitly suppressed — no RETURNING clause
)

// ─── Builder ──────────────────────────────────────────────────────────────────

// Builder constructs parameterised PostgreSQL queries.
// All methods return the same pointer for chaining.
// Errors accumulate and are returned on the first Build* call.
type Builder struct {
	table string

	selectCols []string
	distinct   bool

	joins []joinClause

	groups []condGroup

	groupByCols []string
	havingExpr  string
	havingArgs  []any

	orders []orderClause

	limitVal  int
	offsetVal int

	unions []unionClause

	onConflict string

	retMode returningMode
	retCols []string

	ctes []cteClause

	lockMode LockMode
	lockWait LockWait

	errs []error
}

// New creates a Builder targeting table.
func New(table string) *Builder {
	return &Builder{table: table}
}

// ─── SELECT ───────────────────────────────────────────────────────────────────

// Columns appends expressions to the SELECT list.
// Plain column names are auto-quoted; expressions like "COUNT(*)", "u.email",
// "SUM(total) AS total" are written verbatim.
func (b *Builder) Columns(cols ...string) *Builder {
	b.selectCols = append(b.selectCols, cols...)
	return b
}

// Distinct adds DISTINCT to the SELECT clause.
func (b *Builder) Distinct() *Builder {
	b.distinct = true
	return b
}

// ─── JOIN ─────────────────────────────────────────────────────────────────────

// Join appends any JOIN variant.
// table and condition are written verbatim — never pass user input directly.
// For CROSS JOIN pass "" as condition.
func (b *Builder) Join(kind JoinType, table, condition string) *Builder {
	b.joins = append(b.joins, joinClause{kind: kind, table: table, condition: condition})
	return b
}

func (b *Builder) InnerJoin(table, condition string) *Builder {
	return b.Join(JoinInner, table, condition)
}

func (b *Builder) LeftJoin(table, condition string) *Builder {
	return b.Join(JoinLeft, table, condition)
}

func (b *Builder) RightJoin(table, condition string) *Builder {
	return b.Join(JoinRight, table, condition)
}

func (b *Builder) FullJoin(table, condition string) *Builder {
	return b.Join(JoinFull, table, condition)
}

// LateralJoin appends a LATERAL subquery join:
//
//	LEFT JOIN LATERAL (SELECT …) AS alias ON TRUE
func (b *Builder) LateralJoin(kind JoinType, sub *Builder, alias string) *Builder {
	b.joins = append(b.joins, joinClause{kind: kind, lateral: true, sub: sub, alias: alias})
	return b
}

// ─── WHERE ────────────────────────────────────────────────────────────────────

// Where appends a single Condition ANDed into the WHERE clause.
func (b *Builder) Where(c Condition) *Builder {
	if err := c.Validate(); err != nil {
		b.errs = append(b.errs, err)
		return b
	}
	b.groups = append(b.groups, condGroup{cond: &c})
	return b
}

// WhereGroup appends a condGroup (single or OR-group) ANDed into WHERE.
//
//	b.WhereGroup(qb.OrGroup(
//	    qb.Where("status", qb.OpEq, "active"),
//	    qb.Where("status", qb.OpEq, "pending"),
//	))
//	→ WHERE ("status" = $1 OR "status" = $2)
func (b *Builder) WhereGroup(g condGroup) *Builder {
	if g.isOr {
		for _, c := range g.group {
			if err := c.Validate(); err != nil {
				b.errs = append(b.errs, err)
				return b
			}
		}
	}
	b.groups = append(b.groups, g)
	return b
}

// ─── GROUP BY / HAVING ────────────────────────────────────────────────────────

// GroupBy appends columns to GROUP BY.
func (b *Builder) GroupBy(cols ...string) *Builder {
	b.groupByCols = append(b.groupByCols, cols...)
	return b
}

// Having sets a raw HAVING expression. Use ? as placeholder.
//
//	b.Having("SUM(total) > ?", 500)
func (b *Builder) Having(expr string, args ...any) *Builder {
	b.havingExpr = expr
	b.havingArgs = args
	return b
}

// ─── ORDER BY ─────────────────────────────────────────────────────────────────

// OrderBy appends a column to ORDER BY.
//
//	b.OrderBy("created_at", qb.Desc, qb.NullsLast)
func (b *Builder) OrderBy(col string, dir SortDir, nulls ...NullsOrder) *Builder {
	o := orderClause{column: col, dir: dir}
	if len(nulls) > 0 {
		o.nulls = nulls[0]
	}
	b.orders = append(b.orders, o)
	return b
}

// ─── LIMIT / OFFSET ───────────────────────────────────────────────────────────

// Limit sets the maximum number of rows. 0 = no limit.
func (b *Builder) Limit(n int) *Builder { b.limitVal = n; return b }

// Offset sets the number of rows to skip. 0 = no offset.
func (b *Builder) Offset(n int) *Builder { b.offsetVal = n; return b }

// ─── Locking ──────────────────────────────────────────────────────────────────

// ForUpdate appends FOR UPDATE [NOWAIT | SKIP LOCKED].
func (b *Builder) ForUpdate(wait LockWait) *Builder {
	b.lockMode = LockForUpdate
	b.lockWait = wait
	return b
}

// ForShare appends FOR SHARE [NOWAIT | SKIP LOCKED].
func (b *Builder) ForShare(wait LockWait) *Builder {
	b.lockMode = LockForShare
	b.lockWait = wait
	return b
}

// Lock sets a custom lock mode.
func (b *Builder) Lock(mode LockMode, wait LockWait) *Builder {
	b.lockMode = mode
	b.lockWait = wait
	return b
}

// ─── RETURNING ────────────────────────────────────────────────────────────────

// ReturningID appends RETURNING "id". Default for BuildInsert.
func (b *Builder) ReturningID() *Builder { b.retMode = returningID; return b }

// ReturningAll appends RETURNING *.
func (b *Builder) ReturningAll() *Builder { b.retMode = returningAll; return b }

// Returning appends RETURNING col1, col2, …
func (b *Builder) Returning(cols ...string) *Builder {
	b.retMode = returningColumns
	b.retCols = cols
	return b
}

// ReturningNone suppresses any RETURNING clause, overriding the INSERT default.
func (b *Builder) ReturningNone() *Builder { b.retMode = returningNone; return b }

// WindowCol appends a window function expression to the SELECT list.
//
// fn is the window function call (e.g. "RANK()", "SUM(total)", "LAG(price)").
// partitionBy is the PARTITION BY column (empty string = no partition).
// orderBy is the ORDER BY expression written verbatim (e.g. "total DESC").
// alias is the AS name for the column.
//
//	qb.New("orders").
//	    Columns("user_id", "country", "total").
//	    WindowCol("RANK()", "country", "total DESC", "rank").
//	    WindowCol("SUM(total)", "country", "created_at", "running_total")
//	→ SELECT "user_id", "country", "total",
//	         RANK() OVER (PARTITION BY "country" ORDER BY total DESC) AS "rank",
//	         SUM(total) OVER (PARTITION BY "country" ORDER BY created_at) AS "running_total"
func (b *Builder) WindowCol(fn, partitionBy, orderBy, alias string) *Builder {
	var over string
	switch {
	case partitionBy != "" && orderBy != "":
		over = fmt.Sprintf("PARTITION BY %s ORDER BY %s", quoteIdentExpr(partitionBy), orderBy)
	case partitionBy != "":
		over = fmt.Sprintf("PARTITION BY %s", quoteIdentExpr(partitionBy))
	case orderBy != "":
		over = fmt.Sprintf("ORDER BY %s", orderBy)
	}
	expr := fmt.Sprintf("%s OVER (%s) AS %s", fn, over, pgx.Identifier{alias}.Sanitize())
	b.selectCols = append(b.selectCols, expr)
	return b
}

// ─── WITH (CTEs) ──────────────────────────────────────────────────────────────

// With prepends a CTE. CTE args are prepended to the full argument list
// so their $N parameters come first.
//
//	b.With("recent", "SELECT id FROM orders WHERE created_at > $1", cutoff)
func (b *Builder) With(name, query string, args ...any) *Builder {
	b.ctes = append(b.ctes, cteClause{name: name, query: query, args: args})
	return b
}

// WithRecursive prepends a RECURSIVE CTE.
func (b *Builder) WithRecursive(name, query string, args ...any) *Builder {
	b.ctes = append(b.ctes, cteClause{name: name, query: query, args: args, recursive: true})
	return b
}

// ─── UNION ────────────────────────────────────────────────────────────────────

// Union appends UNION (deduplicating) with another builder.
func (b *Builder) Union(other *Builder) *Builder {
	b.unions = append(b.unions, unionClause{all: false, qb: other})
	return b
}

// UnionAll appends UNION ALL (no deduplication).
func (b *Builder) UnionAll(other *Builder) *Builder {
	b.unions = append(b.unions, unionClause{all: true, qb: other})
	return b
}

// ─── ON CONFLICT ──────────────────────────────────────────────────────────────

// OnConflict sets the ON CONFLICT clause for INSERT (upsert).
//
//	b.OnConflict("(email) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()")
//	b.OnConflict("(id) DO NOTHING")
func (b *Builder) OnConflict(clause string) *Builder {
	b.onConflict = clause
	return b
}

// ─── Build Methods ────────────────────────────────────────────────────────────

// BuildSelect produces a full parameterised SELECT statement starting at $1.
func (b *Builder) BuildSelect() (sql string, args []any, err error) {
	sql, args, err = b.buildSelectFrom(1)
	if err != nil {
		return
	}
	if len(args) > MaxQueryParams {
		return "", nil, tooManyParamsErr(len(args))
	}
	return
}

// buildSelectFrom produces SELECT with parameters starting at paramStart.
// Used internally for subqueries, UNION segments, and lateral joins so that
// parameter numbers continue correctly from the outer query.
func (b *Builder) buildSelectFrom(paramStart int) (string, []any, error) {
	if err := b.firstError(); err != nil {
		return "", nil, err
	}

	var sb strings.Builder
	var args []any
	idx := paramStart

	// WITH
	cteArgs, next := b.writeCTEs(&sb, idx)
	args = append(args, cteArgs...)
	idx = next

	sb.WriteString("SELECT ")
	if b.distinct {
		sb.WriteString("DISTINCT ")
	}
	sb.WriteString(b.renderSelectCols())
	sb.WriteString(" FROM ")
	sb.WriteString(quoteTableExpr(b.table))

	// JOINs
	for _, j := range b.joins {
		switch {
		case j.lateral:
			subSQL, subArgs, err := j.sub.buildSelectFrom(idx)
			if err != nil {
				return "", nil, fmt.Errorf("qb: lateral join: %w", err)
			}
			args = append(args, subArgs...)
			idx += len(subArgs)
			fmt.Fprintf(&sb, " %s JOIN LATERAL (%s) %s ON TRUE",
				j.kind, subSQL, pgx.Identifier{j.alias}.Sanitize())
		case j.kind == JoinCross:
			fmt.Fprintf(&sb, " CROSS JOIN %s", j.table)
		default:
			fmt.Fprintf(&sb, " %s JOIN %s ON %s", j.kind, j.table, j.condition)
		}
	}

	// WHERE
	where, whereArgs, err := b.buildWhereFrom(idx)
	if err != nil {
		return "", nil, err
	}
	if where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(where)
	}
	args = append(args, whereArgs...)
	idx += len(whereArgs)

	// GROUP BY
	if len(b.groupByCols) > 0 {
		quoted := make([]string, len(b.groupByCols))
		for i, c := range b.groupByCols {
			quoted[i] = quoteIdentExpr(c)
		}
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(quoted, ", "))
	}

	// HAVING
	if b.havingExpr != "" {
		expr, hArgs := injectParams(b.havingExpr, idx, b.havingArgs)
		sb.WriteString(" HAVING ")
		sb.WriteString(expr)
		args = append(args, hArgs...)
		idx += len(hArgs)
	}

	// ORDER BY
	if len(b.orders) > 0 {
		parts := make([]string, len(b.orders))
		for i, o := range b.orders {
			part := fmt.Sprintf("%s %s", quoteIdentExpr(o.column), o.dir)
			if o.nulls != "" {
				part += " " + string(o.nulls)
			}
			parts[i] = part
		}
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(parts, ", "))
	}

	// LIMIT / OFFSET — written as integer literals, never user data
	if b.limitVal > 0 {
		fmt.Fprintf(&sb, " LIMIT %d", b.limitVal)
	}
	if b.offsetVal > 0 {
		fmt.Fprintf(&sb, " OFFSET %d", b.offsetVal)
	}

	// Locking
	if b.lockMode != "" {
		sb.WriteString(" ")
		sb.WriteString(string(b.lockMode))
		if b.lockWait != Wait {
			sb.WriteString(" ")
			sb.WriteString(string(b.lockWait))
		}
	}

	// UNION / UNION ALL — each segment continues parameter numbering from idx
	for _, u := range b.unions {
		subSQL, subArgs, err := u.qb.buildSelectFrom(idx)
		if err != nil {
			return "", nil, fmt.Errorf("qb: UNION: %w", err)
		}
		if u.all {
			sb.WriteString(" UNION ALL ")
		} else {
			sb.WriteString(" UNION ")
		}
		sb.WriteString(subSQL)
		args = append(args, subArgs...)
		idx += len(subArgs)
	}

	_ = idx
	return sb.String(), args, nil
}

// BuildInsert produces:
//
//	[WITH …] INSERT INTO "table" (cols) VALUES ($1,…) [ON CONFLICT …] RETURNING "id"
//
// Default RETURNING is "id". Override with ReturningAll() / Returning(…) / ReturningNone().
func (b *Builder) BuildInsert(data map[string]any) (sql string, args []any, err error) {
	if err = b.firstError(); err != nil {
		return
	}
	if len(data) == 0 {
		return "", nil, ErrEmptyData
	}

	var sb strings.Builder
	cteArgs, idx := b.writeCTEs(&sb, 1)
	args = append(args, cteArgs...)

	cols, vals, params := b.flattenData(data, idx)
	args = append(args, vals...)

	fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES (%s)", quoteTableExpr(b.table), cols, params)

	if b.onConflict != "" {
		sb.WriteString(" ON CONFLICT ")
		sb.WriteString(b.onConflict)
	}
	sb.WriteString(b.returningClause(returningID))

	if len(args) > MaxQueryParams {
		return "", nil, tooManyParamsErr(len(args))
	}
	return sb.String(), args, nil
}

// BuildInsertBatch inserts multiple rows in one statement.
//
//	INSERT INTO "table" (col1, col2) VALUES ($1,$2), ($3,$4), … RETURNING "id"
//
// All rows must have identical key sets (first row defines the column list).
func (b *Builder) BuildInsertBatch(rows []map[string]any) (sql string, args []any, err error) {
	if err = b.firstError(); err != nil {
		return
	}
	if len(rows) == 0 {
		return "", nil, ErrEmptyRows
	}

	var sb strings.Builder
	cteArgs, idx := b.writeCTEs(&sb, 1)
	args = append(args, cteArgs...)

	keys := sortedKeys(rows[0])
	quotedCols := make([]string, len(keys))
	for i, k := range keys {
		quotedCols[i] = pgx.Identifier{k}.Sanitize()
	}
	fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES ", quoteTableExpr(b.table), strings.Join(quotedCols, ", "))

	rowFrags := make([]string, len(rows))
	for r, row := range rows {
		placeholders := make([]string, len(keys))
		for i, k := range keys {
			val, ok := row[k]
			if !ok {
				return "", nil, fmt.Errorf("qb: batch row %d missing column %q", r, k)
			}
			placeholders[i] = fmt.Sprintf("$%d", idx)
			args = append(args, val)
			idx++
		}
		rowFrags[r] = "(" + strings.Join(placeholders, ", ") + ")"
	}
	sb.WriteString(strings.Join(rowFrags, ", "))

	if b.onConflict != "" {
		sb.WriteString(" ON CONFLICT ")
		sb.WriteString(b.onConflict)
	}
	sb.WriteString(b.returningClause(returningID))

	if len(args) > MaxQueryParams {
		return "", nil, tooManyParamsErr(len(args))
	}
	return sb.String(), args, nil
}

// BuildUpdate produces:
//
//	[WITH …] UPDATE "table" SET col=$1,… WHERE … [RETURNING …]
func (b *Builder) BuildUpdate(data map[string]any) (sql string, args []any, err error) {
	if err = b.firstError(); err != nil {
		return
	}
	if len(data) == 0 {
		return "", nil, ErrEmptyData
	}

	var sb strings.Builder
	cteArgs, idx := b.writeCTEs(&sb, 1)
	args = append(args, cteArgs...)

	setClauses, setArgs, nextIdx := b.buildSet(data, idx)
	args = append(args, setArgs...)

	fmt.Fprintf(&sb, "UPDATE %s SET %s", quoteTableExpr(b.table), setClauses)

	where, whereArgs, err := b.buildWhereFrom(nextIdx)
	if err != nil {
		return "", nil, err
	}
	if where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(where)
	}
	args = append(args, whereArgs...)
	sb.WriteString(b.returningClause(returningUnset))

	if len(args) > MaxQueryParams {
		return "", nil, tooManyParamsErr(len(args))
	}
	return sb.String(), args, nil
}

// BuildDelete produces:
//
//	[WITH …] DELETE FROM "table" WHERE … [RETURNING …]
func (b *Builder) BuildDelete() (sql string, args []any, err error) {
	if err = b.firstError(); err != nil {
		return
	}

	var sb strings.Builder
	cteArgs, idx := b.writeCTEs(&sb, 1)
	args = append(args, cteArgs...)

	fmt.Fprintf(&sb, "DELETE FROM %s", quoteTableExpr(b.table))

	where, whereArgs, err := b.buildWhereFrom(idx)
	if err != nil {
		return "", nil, err
	}
	if where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(where)
	}
	args = append(args, whereArgs...)
	sb.WriteString(b.returningClause(returningUnset))

	if len(args) > MaxQueryParams {
		return "", nil, tooManyParamsErr(len(args))
	}
	return sb.String(), args, nil
}

// ─── Internal rendering ───────────────────────────────────────────────────────

func (b *Builder) renderSelectCols() string {
	if len(b.selectCols) == 0 {
		return "*"
	}
	parts := make([]string, len(b.selectCols))
	for i, c := range b.selectCols {
		parts[i] = quoteIdentExpr(c)
	}
	return strings.Join(parts, ", ")
}

func (b *Builder) writeCTEs(sb *strings.Builder, startIdx int) (args []any, nextIdx int) {
	nextIdx = startIdx
	if len(b.ctes) == 0 {
		return nil, nextIdx
	}

	hasRecursive := false
	for _, c := range b.ctes {
		if c.recursive {
			hasRecursive = true
			break
		}
	}

	frags := make([]string, len(b.ctes))
	for i, c := range b.ctes {
		frags[i] = fmt.Sprintf("%s AS (%s)", pgx.Identifier{c.name}.Sanitize(), c.query)
		args = append(args, c.args...)
		nextIdx += len(c.args)
	}

	if hasRecursive {
		sb.WriteString("WITH RECURSIVE ")
	} else {
		sb.WriteString("WITH ")
	}
	sb.WriteString(strings.Join(frags, ", "))
	sb.WriteString(" ")
	return args, nextIdx
}

func (b *Builder) buildWhereFrom(startIdx int) (clause string, args []any, err error) {
	if len(b.groups) == 0 {
		return "", nil, nil
	}

	parts := make([]string, 0, len(b.groups))
	idx := startIdx

	for _, g := range b.groups {
		if g.isOr {
			orParts := make([]string, 0, len(g.group))
			for _, cond := range g.group {
				frag, condArgs, condErr := b.renderCond(cond, idx)
				if condErr != nil {
					return "", nil, condErr
				}
				orParts = append(orParts, frag)
				args = append(args, condArgs...)
				idx += len(condArgs)
			}
			parts = append(parts, "("+strings.Join(orParts, " OR ")+")")
		} else {
			frag, condArgs, condErr := b.renderCond(*g.cond, idx)
			if condErr != nil {
				return "", nil, condErr
			}
			parts = append(parts, frag)
			args = append(args, condArgs...)
			idx += len(condArgs)
		}
	}

	return strings.Join(parts, " AND "), args, nil
}

func (b *Builder) renderCond(cond Condition, idx int) (frag string, args []any, err error) {
	col := quoteIdentExpr(cond.Column)

	switch cond.Operator {
	case OpExists:
		subSQL, subArgs, subErr := cond.Sub.buildSelectFrom(idx)
		if subErr != nil {
			return "", nil, fmt.Errorf("qb: EXISTS subquery: %w", subErr)
		}
		return fmt.Sprintf("EXISTS (%s)", subSQL), subArgs, nil

	case OpNotExists:
		subSQL, subArgs, subErr := cond.Sub.buildSelectFrom(idx)
		if subErr != nil {
			return "", nil, fmt.Errorf("qb: NOT EXISTS subquery: %w", subErr)
		}
		return fmt.Sprintf("NOT EXISTS (%s)", subSQL), subArgs, nil

	case OpSubquery:
		op := cond.Value.(Operator)
		subSQL, subArgs, subErr := cond.Sub.buildSelectFrom(idx)
		if subErr != nil {
			return "", nil, fmt.Errorf("qb: subquery condition: %w", subErr)
		}
		return fmt.Sprintf("%s %s (%s)", col, string(op), subSQL), subArgs, nil

	case OpRaw:
		raw := cond.Value.(rawExpr)
		expr, rawArgs := injectParams(raw.expr, idx, raw.args)
		return expr, rawArgs, nil

	case OpIsNull:
		return fmt.Sprintf("%s IS NULL", col), nil, nil

	case OpNotNull:
		return fmt.Sprintf("%s IS NOT NULL", col), nil, nil

	case OpIn, OpNotIn:
		items, sliceErr := toAnySlice(cond.Value)
		if sliceErr != nil {
			return "", nil, fmt.Errorf("qb: %s %q: %w", cond.Operator, cond.Column, sliceErr)
		}
		if len(items) == 0 {
			return "", nil, fmt.Errorf("qb: %s %q: slice must not be empty", cond.Operator, cond.Column)
		}
		placeholders := make([]string, len(items))
		for i, v := range items {
			placeholders[i] = fmt.Sprintf("$%d", idx)
			args = append(args, v)
			idx++
		}
		return fmt.Sprintf("%s %s (%s)", col, string(cond.Operator), strings.Join(placeholders, ", ")), args, nil

	case OpBetween, OpNotBetween:
		pair, betErr := toAnySlice(cond.Value)
		if betErr != nil || len(pair) != 2 {
			return "", nil, fmt.Errorf("qb: %s %q requires a 2-element slice", cond.Operator, cond.Column)
		}
		frag = fmt.Sprintf("%s %s $%d AND $%d", col, string(cond.Operator), idx, idx+1)
		return frag, pair, nil

	case OpAny, OpAll:
		// col = ANY($n) or col = ALL($n) — value bound as a PG array
		return fmt.Sprintf("%s %s($%d)", col, string(cond.Operator), idx), []any{cond.Value}, nil

	default:
		// scalar: =, <>, <, <=, >, >=, LIKE, ILIKE, @@, ?, ?&, ?|, @>, <@, &&
		return fmt.Sprintf("%s %s $%d", col, string(cond.Operator), idx), []any{cond.Value}, nil
	}
}

func (b *Builder) buildSet(data map[string]any, paramStart int) (clause string, args []any, nextIdx int) {
	keys := sortedKeys(data)
	parts := make([]string, len(keys))
	idx := paramStart
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s = $%d", pgx.Identifier{k}.Sanitize(), idx)
		args = append(args, data[k])
		idx++
	}
	return strings.Join(parts, ", "), args, idx
}

func (b *Builder) flattenData(data map[string]any, startIdx int) (cols string, vals []any, params string) {
	keys := sortedKeys(data)
	quotedCols := make([]string, len(keys))
	placeholders := make([]string, len(keys))
	for i, k := range keys {
		quotedCols[i] = pgx.Identifier{k}.Sanitize()
		vals = append(vals, data[k])
		placeholders[i] = fmt.Sprintf("$%d", startIdx+i)
	}
	cols = strings.Join(quotedCols, ", ")
	params = strings.Join(placeholders, ", ")
	return
}

func (b *Builder) returningClause(def returningMode) string {
	mode := b.retMode
	if mode == returningUnset {
		mode = def // apply Build* default only when caller hasn't set anything
	}
	switch mode {
	case returningID:
		return ` RETURNING "id"`
	case returningAll:
		return " RETURNING *"
	case returningColumns:
		quoted := make([]string, len(b.retCols))
		for i, c := range b.retCols {
			quoted[i] = quoteIdentExpr(c)
		}
		return " RETURNING " + strings.Join(quoted, ", ")
	default: // returningNone or any unknown — no clause
		return ""
	}
}

func (b *Builder) firstError() error {
	if len(b.errs) > 0 {
		return b.errs[0]
	}
	return nil
}

// ─── Identifier quoting ───────────────────────────────────────────────────────

// QuoteIdent safely quotes a single PostgreSQL identifier.
//
//	QuoteIdent("my table") → "my table"
func QuoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// quoteIdentExpr handles:
//  1. Raw expressions (contains ( ) * / space) → verbatim
//  2. Qualified names "alias.col" → "alias"."col"
//  3. Plain names → "name"
func quoteIdentExpr(expr string) string {
	if strings.ContainsAny(expr, "()*/ ") {
		return expr
	}
	if dot := strings.IndexByte(expr, '.'); dot >= 0 {
		left := expr[:dot]
		right := expr[dot+1:]
		var lq string
		if strings.HasPrefix(left, `"`) && strings.HasSuffix(left, `"`) {
			lq = left
		} else {
			lq = pgx.Identifier{left}.Sanitize()
		}
		return lq + "." + quoteIdentExpr(right)
	}
	if strings.HasPrefix(expr, `"`) && strings.HasSuffix(expr, `"`) {
		return expr
	}
	return pgx.Identifier{expr}.Sanitize()
}

// quoteTableExpr handles: plain, schema.table, table alias.
func quoteTableExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, `"`) || strings.HasPrefix(expr, "(") {
		return expr
	}
	if strings.Contains(expr, ".") {
		dotIdx := strings.Index(expr, ".")
		schema := expr[:dotIdx]
		rest := expr[dotIdx+1:]
		if spaceIdx := strings.Index(rest, " "); spaceIdx >= 0 {
			table := rest[:spaceIdx]
			alias := strings.TrimSpace(rest[spaceIdx+1:])
			return pgx.Identifier{schema}.Sanitize() + "." +
				pgx.Identifier{table}.Sanitize() + " " +
				pgx.Identifier{alias}.Sanitize()
		}
		return pgx.Identifier{schema}.Sanitize() + "." + pgx.Identifier{rest}.Sanitize()
	}
	if spaceIdx := strings.Index(expr, " "); spaceIdx >= 0 {
		table := expr[:spaceIdx]
		alias := strings.TrimSpace(expr[spaceIdx+1:])
		if !strings.Contains(alias, " ") {
			return pgx.Identifier{table}.Sanitize() + " " + pgx.Identifier{alias}.Sanitize()
		}
		return expr
	}
	return pgx.Identifier{expr}.Sanitize()
}

// injectParams replaces ? placeholders in expr with $N starting at startIdx.
func injectParams(expr string, startIdx int, args []any) (string, []any) {
	for i := range args {
		expr = strings.Replace(expr, "?", fmt.Sprintf("$%d", startIdx+i), 1)
	}
	return expr, args
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
