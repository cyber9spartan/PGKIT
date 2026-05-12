package qb

import (
	"fmt"
	"reflect"
)

// Operator is a SQL comparison or containment operator.
type Operator string

const (
	// Scalar comparison
	OpEq    Operator = "="
	OpNotEq Operator = "<>"
	OpLt    Operator = "<"
	OpLte   Operator = "<="
	OpGt    Operator = ">"
	OpGte   Operator = ">="

	// Pattern matching
	OpLike  Operator = "LIKE"
	OpILike Operator = "ILIKE"

	// Membership
	OpIn    Operator = "IN"
	OpNotIn Operator = "NOT IN"

	// Null checks (no value needed)
	OpIsNull  Operator = "IS NULL"
	OpNotNull Operator = "IS NOT NULL"

	// Range
	OpBetween    Operator = "BETWEEN"
	OpNotBetween Operator = "NOT BETWEEN"

	// PostgreSQL array operators — value must be a Go slice bound as a pgx array.
	OpAny Operator = "= ANY" // col = ANY($n)
	OpAll Operator = "= ALL" // col = ALL($n)

	// Array containment
	OpArrayContains    Operator = "@>"  // array_col @> $n
	OpArrayContainedBy Operator = "<@"  // array_col <@ $n
	OpArrayOverlap     Operator = "&&"  // array_col && $n

	// JSONB operators
	OpJSONContains    Operator = "@>"  // jsonb_col @> $n
	OpJSONContainedBy Operator = "<@"  // jsonb_col <@ $n
	OpJSONHasKey      Operator = "?"   // jsonb_col ? $n
	OpJSONHasAllKeys  Operator = "?&"  // jsonb_col ?& $n
	OpJSONHasAnyKey   Operator = "?|"  // jsonb_col ?| $n

	// Full-text search
	OpTextSearch Operator = "@@" // tsvector_col @@ tsquery

	// Internal — handled specially by Builder.renderCond
	OpExists    Operator = "__EXISTS__"
	OpNotExists Operator = "__NOT_EXISTS__"
	OpSubquery  Operator = "__SUBQUERY__"
	OpRaw       Operator = "__RAW__"
)

// rawExpr holds a literal SQL fragment with ? placeholders.
type rawExpr struct {
	expr string
	args []any
}

// Condition is a single predicate in a WHERE clause.
type Condition struct {
	Column   string
	Operator Operator
	Value    any

	// Sub is set for EXISTS / NOT EXISTS / column-IN-subquery conditions.
	Sub *Builder
}

func (c Condition) Validate() error {
	switch c.Operator {
	case OpExists, OpNotExists:
		if c.Sub == nil {
			return fmt.Errorf("qb: EXISTS condition requires a sub-query")
		}
	case OpSubquery:
		if c.Sub == nil {
			return fmt.Errorf("qb: subquery condition requires a sub-query")
		}
	case OpRaw:
		// always valid
	default:
		if c.Column == "" {
			return fmt.Errorf("qb: condition column must not be empty")
		}
	}
	return nil
}

// condGroup holds either a single Condition (ANDed) or multiple joined with OR.
type condGroup struct {
	isOr  bool
	cond  *Condition
	group []Condition
}

// OrGroup wraps conditions joined with OR inside parentheses:
//
//	WHERE (status = $1 OR status = $2) AND total > $3
func OrGroup(conds ...Condition) condGroup {
	return condGroup{isOr: true, group: conds}
}

// ─── Condition constructors ──────────────────────────────────────────────────

func Where(col string, op Operator, val any) Condition {
	return Condition{Column: col, Operator: op, Value: val}
}

func WhereNull(col string) Condition    { return Condition{Column: col, Operator: OpIsNull} }
func WhereNotNull(col string) Condition { return Condition{Column: col, Operator: OpNotNull} }

func WhereIn(col string, vals any) Condition {
	return Condition{Column: col, Operator: OpIn, Value: vals}
}

func WhereNotIn(col string, vals any) Condition {
	return Condition{Column: col, Operator: OpNotIn, Value: vals}
}

func WhereBetween(col string, low, high any) Condition {
	return Condition{Column: col, Operator: OpBetween, Value: []any{low, high}}
}

func WhereNotBetween(col string, low, high any) Condition {
	return Condition{Column: col, Operator: OpNotBetween, Value: []any{low, high}}
}

// WhereAny matches rows where col = ANY(array).
// val must be a typed slice; pgx binds it as a PostgreSQL array.
func WhereAny(col string, val any) Condition {
	return Condition{Column: col, Operator: OpAny, Value: val}
}

// WhereAll matches rows where col = ALL(array).
func WhereAll(col string, val any) Condition {
	return Condition{Column: col, Operator: OpAll, Value: val}
}

// WhereJSONContains checks jsonb_col @> val (JSON superset).
func WhereJSONContains(col string, val any) Condition {
	return Condition{Column: col, Operator: OpJSONContains, Value: val}
}

// WhereJSONHasKey checks jsonb_col ? key.
func WhereJSONHasKey(col, key string) Condition {
	return Condition{Column: col, Operator: OpJSONHasKey, Value: key}
}

// WhereArrayContains checks array_col @> val.
func WhereArrayContains(col string, val any) Condition {
	return Condition{Column: col, Operator: OpArrayContains, Value: val}
}

// WhereArrayOverlap checks array_col && val (any element in common).
func WhereArrayOverlap(col string, val any) Condition {
	return Condition{Column: col, Operator: OpArrayOverlap, Value: val}
}

// WhereTextSearch checks tsvector_col @@ tsquery.
func WhereTextSearch(col, tsquery string) Condition {
	return Condition{Column: col, Operator: OpTextSearch, Value: tsquery}
}

// WhereExists appends EXISTS (subquery).
func WhereExists(sub *Builder) Condition {
	return Condition{Operator: OpExists, Sub: sub}
}

// WhereNotExists appends NOT EXISTS (subquery).
func WhereNotExists(sub *Builder) Condition {
	return Condition{Operator: OpNotExists, Sub: sub}
}

// WhereSubquery appends col op (subquery) — e.g. id IN (SELECT id FROM ...).
// op should be OpIn / OpNotIn or a comparison operator.
func WhereSubquery(col string, op Operator, sub *Builder) Condition {
	return Condition{Column: col, Operator: OpSubquery, Value: op, Sub: sub}
}

// WhereRaw appends a raw SQL fragment. Use ? as placeholder.
//
//	WhereRaw("lower(email) = ?", email)
//	WhereRaw("age BETWEEN ? AND ?", 18, 65)
func WhereRaw(expr string, args ...any) Condition {
	return Condition{Operator: OpRaw, Value: rawExpr{expr: expr, args: args}}
}

// ─── helpers ────────────────────────────────────────────────────────────────

func toAnySlice(v any) ([]any, error) {
	if v == nil {
		return nil, fmt.Errorf("qb: IN/BETWEEN value is nil")
	}
	if s, ok := v.([]any); ok {
		return s, nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("qb: expected slice for IN/BETWEEN, got %T", v)
	}
	out := make([]any, rv.Len())
	for i := range rv.Len() {
		out[i] = rv.Index(i).Interface()
	}
	return out, nil
}
