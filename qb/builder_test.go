package qb_test

import (
	"strings"
	"testing"

	"github.com/skolio/pgkit/qb"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustSelect(t *testing.T, b *qb.Builder) (string, []any) {
	t.Helper()
	sql, args, err := b.BuildSelect()
	if err != nil {
		t.Fatalf("BuildSelect error: %v", err)
	}
	return sql, args
}

func mustInsert(t *testing.T, b *qb.Builder, data map[string]any) (string, []any) {
	t.Helper()
	sql, args, err := b.BuildInsert(data)
	if err != nil {
		t.Fatalf("BuildInsert error: %v", err)
	}
	return sql, args
}

func mustUpdate(t *testing.T, b *qb.Builder, data map[string]any) (string, []any) {
	t.Helper()
	sql, args, err := b.BuildUpdate(data)
	if err != nil {
		t.Fatalf("BuildUpdate error: %v", err)
	}
	return sql, args
}

func mustDelete(t *testing.T, b *qb.Builder) (string, []any) {
	t.Helper()
	sql, args, err := b.BuildDelete()
	if err != nil {
		t.Fatalf("BuildDelete error: %v", err)
	}
	return sql, args
}

func assertSQL(t *testing.T, got, want string) {
	t.Helper()
	// normalise whitespace for comparison
	got = strings.Join(strings.Fields(got), " ")
	want = strings.Join(strings.Fields(want), " ")
	if got != want {
		t.Errorf("\ngot:  %s\nwant: %s", got, want)
	}
}

func assertArgs(t *testing.T, got []any, want ...any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("args length: got %d, want %d — got: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("arg[%d]: got %v (%T), want %v (%T)", i, got[i], got[i], want[i], want[i])
		}
	}
}

// ─── SELECT ──────────────────────────────────────────────────────────────────

func TestBuildSelect_Star(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users"))
	assertSQL(t, sql, `SELECT * FROM "users"`)
	assertArgs(t, args)
}

func TestBuildSelect_Columns(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Columns("id", "name", "email"))
	assertSQL(t, sql, `SELECT "id", "name", "email" FROM "users"`)
	assertArgs(t, args)
}

func TestBuildSelect_Distinct(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").Distinct().Columns("country"))
	assertSQL(t, sql, `SELECT DISTINCT "country" FROM "users"`)
}

func TestBuildSelect_SchemaTable(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("public.users"))
	assertSQL(t, sql, `SELECT * FROM "public"."users"`)
}

func TestBuildSelect_TableAlias(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users u").Columns("u.id", "u.name"))
	assertSQL(t, sql, `SELECT "u"."id", "u"."name" FROM "users" "u"`)
}

func TestBuildSelect_RawExpressionColumn(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("orders").Columns("COUNT(*) AS total", "SUM(amount) AS revenue"))
	assertSQL(t, sql, `SELECT COUNT(*) AS total, SUM(amount) AS revenue FROM "orders"`)
}

// ─── WHERE conditions ────────────────────────────────────────────────────────

func TestWhere_Eq(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Where(qb.Where("id", qb.OpEq, 42)))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "id" = $1`)
	assertArgs(t, args, 42)
}

func TestWhere_NotEq(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Where(qb.Where("status", qb.OpNotEq, "deleted")))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "status" <> $1`)
	assertArgs(t, args, "deleted")
}

func TestWhere_MultipleAnded(t *testing.T) {
	sql, args := mustSelect(t, qb.New("students").
		Where(qb.Where("school_id", qb.OpEq, 1)).
		Where(qb.Where("grade", qb.OpGte, 5)))
	assertSQL(t, sql, `SELECT * FROM "students" WHERE "school_id" = $1 AND "grade" >= $2`)
	assertArgs(t, args, 1, 5)
}

func TestWhere_Null(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Where(qb.WhereNull("deleted_at")))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "deleted_at" IS NULL`)
	assertArgs(t, args)
}

func TestWhere_NotNull(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").Where(qb.WhereNotNull("verified_at")))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "verified_at" IS NOT NULL`)
}

func TestWhere_Like(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Where(qb.Where("name", qb.OpILike, "%john%")))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "name" ILIKE $1`)
	assertArgs(t, args, "%john%")
}

func TestWhere_In(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Where(qb.WhereIn("id", []int{1, 2, 3})))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "id" IN ($1, $2, $3)`)
	assertArgs(t, args, 1, 2, 3)
}

func TestWhere_NotIn(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Where(qb.WhereNotIn("status", []string{"banned", "deleted"})))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "status" NOT IN ($1, $2)`)
	assertArgs(t, args, "banned", "deleted")
}

func TestWhere_Between(t *testing.T) {
	sql, args := mustSelect(t, qb.New("orders").Where(qb.WhereBetween("total", 100, 500)))
	assertSQL(t, sql, `SELECT * FROM "orders" WHERE "total" BETWEEN $1 AND $2`)
	assertArgs(t, args, 100, 500)
}

func TestWhere_NotBetween(t *testing.T) {
	sql, args := mustSelect(t, qb.New("orders").Where(qb.WhereNotBetween("total", 100, 500)))
	assertSQL(t, sql, `SELECT * FROM "orders" WHERE "total" NOT BETWEEN $1 AND $2`)
	assertArgs(t, args, 100, 500)
}

func TestWhere_OrGroup(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").
		WhereGroup(qb.OrGroup(
			qb.Where("status", qb.OpEq, "active"),
			qb.Where("status", qb.OpEq, "pending"),
		)).
		Where(qb.Where("school_id", qb.OpEq, 10)))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE ("status" = $1 OR "status" = $2) AND "school_id" = $3`)
	assertArgs(t, args, "active", "pending", 10)
}

func TestWhere_QualifiedColumn(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users u").Where(qb.Where("u.school_id", qb.OpEq, 5)))
	assertSQL(t, sql, `SELECT * FROM "users" "u" WHERE "u"."school_id" = $1`)
	assertArgs(t, args, 5)
}

func TestWhere_Raw(t *testing.T) {
	sql, args := mustSelect(t, qb.New("users").Where(qb.WhereRaw("lower(email) = ?", "test@example.com")))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE lower(email) = $1`)
	assertArgs(t, args, "test@example.com")
}

func TestWhere_RawMultiple(t *testing.T) {
	sql, args := mustSelect(t, qb.New("orders").Where(qb.WhereRaw("total BETWEEN ? AND ?", 10, 99)))
	assertSQL(t, sql, `SELECT * FROM "orders" WHERE total BETWEEN $1 AND $2`)
	assertArgs(t, args, 10, 99)
}

func TestWhere_Any(t *testing.T) {
	ids := []int{1, 2, 3}
	sql, args := mustSelect(t, qb.New("users").Where(qb.WhereAny("id", ids)))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "id" = ANY($1)`)
	// value is passed through as-is (a slice); just check length and type
	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}
	got, ok := args[0].([]int)
	if !ok || len(got) != 3 {
		t.Errorf("arg[0]: got %v (%T), want []int{1,2,3}", args[0], args[0])
	}
}

func TestWhere_JSONContains(t *testing.T) {
	sql, args := mustSelect(t, qb.New("profiles").Where(qb.WhereJSONContains("metadata", `{"role":"admin"}`)))
	assertSQL(t, sql, `SELECT * FROM "profiles" WHERE "metadata" @> $1`)
	assertArgs(t, args, `{"role":"admin"}`)
}

func TestWhere_JSONHasKey(t *testing.T) {
	sql, args := mustSelect(t, qb.New("profiles").Where(qb.WhereJSONHasKey("metadata", "avatar")))
	assertSQL(t, sql, `SELECT * FROM "profiles" WHERE "metadata" ? $1`)
	assertArgs(t, args, "avatar")
}

func TestWhere_ArrayContains(t *testing.T) {
	tags := []string{"go", "postgres"}
	sql, args := mustSelect(t, qb.New("posts").Where(qb.WhereArrayContains("tags", tags)))
	assertSQL(t, sql, `SELECT * FROM "posts" WHERE "tags" @> $1`)
	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}
	got, ok := args[0].([]string)
	if !ok || len(got) != 2 {
		t.Errorf("arg[0]: got %v (%T)", args[0], args[0])
	}
}

func TestWhere_ArrayOverlap(t *testing.T) {
	tags := []string{"go", "rust"}
	sql, args := mustSelect(t, qb.New("posts").Where(qb.WhereArrayOverlap("tags", tags)))
	assertSQL(t, sql, `SELECT * FROM "posts" WHERE "tags" && $1`)
	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}
	got, ok := args[0].([]string)
	if !ok || len(got) != 2 {
		t.Errorf("arg[0]: got %v (%T)", args[0], args[0])
	}
}

func TestWhere_TextSearch(t *testing.T) {
	sql, args := mustSelect(t, qb.New("docs").Where(qb.WhereTextSearch("body_tsvector", "golang & postgres")))
	assertSQL(t, sql, `SELECT * FROM "docs" WHERE "body_tsvector" @@ $1`)
	assertArgs(t, args, "golang & postgres")
}

// ─── Subqueries ───────────────────────────────────────────────────────────────

func TestWhere_Exists(t *testing.T) {
	sub := qb.New("enrollments").
		Columns("1").
		Where(qb.Where("enrollments.student_id", qb.OpEq, 99))

	sql, args := mustSelect(t, qb.New("students").Where(qb.WhereExists(sub)))
	assertSQL(t, sql, `SELECT * FROM "students" WHERE EXISTS (SELECT "1" FROM "enrollments" WHERE "enrollments"."student_id" = $1)`)
	assertArgs(t, args, 99)
}

func TestWhere_NotExists(t *testing.T) {
	sub := qb.New("bans").Columns("1").Where(qb.Where("bans.user_id", qb.OpEq, 7))
	sql, args := mustSelect(t, qb.New("users").Where(qb.WhereNotExists(sub)))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE NOT EXISTS (SELECT "1" FROM "bans" WHERE "bans"."user_id" = $1)`)
	assertArgs(t, args, 7)
}

func TestWhere_SubqueryIN(t *testing.T) {
	sub := qb.New("active_schools").Columns("id").Where(qb.Where("country", qb.OpEq, "IN"))
	sql, args := mustSelect(t, qb.New("students").Where(qb.WhereSubquery("school_id", qb.OpIn, sub)))
	assertSQL(t, sql, `SELECT * FROM "students" WHERE "school_id" IN (SELECT "id" FROM "active_schools" WHERE "country" = $1)`)
	assertArgs(t, args, "IN")
}

func TestWhere_ExistsParamContinuation(t *testing.T) {
	// Outer query already uses $1; EXISTS subquery must use $2
	sub := qb.New("orders").Columns("1").Where(qb.Where("user_id", qb.OpEq, 99))
	sql, args := mustSelect(t, qb.New("users").
		Where(qb.Where("active", qb.OpEq, true)).
		Where(qb.WhereExists(sub)))
	assertSQL(t, sql, `SELECT * FROM "users" WHERE "active" = $1 AND EXISTS (SELECT "1" FROM "orders" WHERE "user_id" = $2)`)
	assertArgs(t, args, true, 99)
}

// ─── JOIN ─────────────────────────────────────────────────────────────────────

func TestJoin_Inner(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("orders o").
		Columns("o.id", "u.name").
		InnerJoin("users u", "u.id = o.user_id"))
	assertSQL(t, sql, `SELECT "o"."id", "u"."name" FROM "orders" "o" INNER JOIN users u ON u.id = o.user_id`)
}

func TestJoin_Left(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("students s").
		LeftJoin("classes c", "c.id = s.class_id"))
	assertSQL(t, sql, `SELECT * FROM "students" "s" LEFT JOIN classes c ON c.id = s.class_id`)
}

func TestJoin_Multiple(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("orders o").
		InnerJoin("users u", "u.id = o.user_id").
		LeftJoin("discounts d", "d.id = o.discount_id"))
	assertSQL(t, sql, `SELECT * FROM "orders" "o" INNER JOIN users u ON u.id = o.user_id LEFT JOIN discounts d ON d.id = o.discount_id`)
}

func TestJoin_Lateral(t *testing.T) {
	sub := qb.New("order_items").
		Columns("product_id", "qty").
		Where(qb.Where("order_id", qb.OpEq, 5)).
		Limit(3)
	sql, args := mustSelect(t, qb.New("orders").
		Columns("orders.id", "items.product_id").
		LateralJoin(qb.JoinLeft, sub, "items"))
	assertSQL(t, sql, `SELECT "orders"."id", "items"."product_id" FROM "orders" LEFT JOIN LATERAL (SELECT "product_id", "qty" FROM "order_items" WHERE "order_id" = $1 LIMIT 3) "items" ON TRUE`)
	assertArgs(t, args, 5)
}

// ─── GROUP BY / HAVING ────────────────────────────────────────────────────────

func TestGroupBy(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("orders").
		Columns("user_id", "COUNT(*) AS total").
		GroupBy("user_id"))
	assertSQL(t, sql, `SELECT "user_id", COUNT(*) AS total FROM "orders" GROUP BY "user_id"`)
}

func TestHaving(t *testing.T) {
	sql, args := mustSelect(t, qb.New("orders").
		Columns("user_id", "SUM(total) AS revenue").
		GroupBy("user_id").
		Having("SUM(total) > ?", 1000))
	assertSQL(t, sql, `SELECT "user_id", SUM(total) AS revenue FROM "orders" GROUP BY "user_id" HAVING SUM(total) > $1`)
	assertArgs(t, args, 1000)
}

func TestHaving_AfterWhere(t *testing.T) {
	// HAVING args must continue numbering after WHERE args
	sql, args := mustSelect(t, qb.New("orders").
		Columns("user_id", "SUM(total) AS revenue").
		Where(qb.Where("school_id", qb.OpEq, 3)).
		GroupBy("user_id").
		Having("SUM(total) > ?", 500))
	assertSQL(t, sql, `SELECT "user_id", SUM(total) AS revenue FROM "orders" WHERE "school_id" = $1 GROUP BY "user_id" HAVING SUM(total) > $2`)
	assertArgs(t, args, 3, 500)
}

// ─── ORDER BY ─────────────────────────────────────────────────────────────────

func TestOrderBy(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").OrderBy("name", qb.Asc))
	assertSQL(t, sql, `SELECT * FROM "users" ORDER BY "name" ASC`)
}

func TestOrderBy_Multiple(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").
		OrderBy("created_at", qb.Desc).
		OrderBy("name", qb.Asc))
	assertSQL(t, sql, `SELECT * FROM "users" ORDER BY "created_at" DESC, "name" ASC`)
}

func TestOrderBy_NullsLast(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").OrderBy("score", qb.Desc, qb.NullsLast))
	assertSQL(t, sql, `SELECT * FROM "users" ORDER BY "score" DESC NULLS LAST`)
}

func TestOrderBy_NullsFirst(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").OrderBy("score", qb.Asc, qb.NullsFirst))
	assertSQL(t, sql, `SELECT * FROM "users" ORDER BY "score" ASC NULLS FIRST`)
}

// ─── LIMIT / OFFSET ───────────────────────────────────────────────────────────

func TestLimitOffset(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").Limit(10).Offset(20))
	assertSQL(t, sql, `SELECT * FROM "users" LIMIT 10 OFFSET 20`)
}

func TestLimit_NoOffset(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("users").Limit(5))
	assertSQL(t, sql, `SELECT * FROM "users" LIMIT 5`)
}

// ─── Locking ──────────────────────────────────────────────────────────────────

func TestForUpdate(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("accounts").
		Where(qb.Where("id", qb.OpEq, 1)).
		ForUpdate(qb.Wait))
	assertSQL(t, sql, `SELECT * FROM "accounts" WHERE "id" = $1 FOR UPDATE`)
}

func TestForUpdate_SkipLocked(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("jobs").
		Where(qb.Where("status", qb.OpEq, "pending")).
		ForUpdate(qb.SkipLocked))
	assertSQL(t, sql, `SELECT * FROM "jobs" WHERE "status" = $1 FOR UPDATE SKIP LOCKED`)
}

func TestForUpdate_NoWait(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("accounts").ForUpdate(qb.NoWait))
	assertSQL(t, sql, `SELECT * FROM "accounts" FOR UPDATE NOWAIT`)
}

func TestForShare(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("accounts").ForShare(qb.Wait))
	assertSQL(t, sql, `SELECT * FROM "accounts" FOR SHARE`)
}

// ─── CTE ─────────────────────────────────────────────────────────────────────

func TestWith_CTE(t *testing.T) {
	sql, args := mustSelect(t,
		qb.New("recent").
			With("recent", "SELECT id, total FROM orders WHERE created_at > $1", "2024-01-01").
			Columns("id", "total").
			Where(qb.Where("total", qb.OpGt, 100)))
	assertSQL(t, sql, `WITH "recent" AS (SELECT id, total FROM orders WHERE created_at > $1) SELECT "id", "total" FROM "recent" WHERE "total" > $2`)
	assertArgs(t, args, "2024-01-01", 100)
}

func TestWith_RecursiveCTE(t *testing.T) {
	sql, _ := mustSelect(t,
		qb.New("tree").
			WithRecursive("tree",
				"SELECT id, parent_id FROM categories WHERE parent_id IS NULL "+
					"UNION ALL "+
					"SELECT c.id, c.parent_id FROM categories c JOIN tree t ON t.id = c.parent_id",
			).
			Columns("id", "parent_id"))
	if !strings.Contains(sql, "WITH RECURSIVE") {
		t.Errorf("expected WITH RECURSIVE, got: %s", sql)
	}
}

// ─── UNION ────────────────────────────────────────────────────────────────────

func TestUnion(t *testing.T) {
	a := qb.New("admins").Columns("id", "name")
	b := qb.New("teachers").Columns("id", "name")
	sql, args := mustSelect(t, a.Union(b))
	assertSQL(t, sql, `SELECT "id", "name" FROM "admins" UNION SELECT "id", "name" FROM "teachers"`)
	assertArgs(t, args)
}

func TestUnionAll(t *testing.T) {
	a := qb.New("logs_2023").Columns("id", "event")
	b := qb.New("logs_2024").Columns("id", "event")
	sql, _ := mustSelect(t, a.UnionAll(b))
	assertSQL(t, sql, `SELECT "id", "event" FROM "logs_2023" UNION ALL SELECT "id", "event" FROM "logs_2024"`)
}

func TestUnion_ParamContinuation(t *testing.T) {
	// $1 in first query, UNION query must use $2
	a := qb.New("students").Where(qb.Where("school_id", qb.OpEq, 1))
	b := qb.New("alumni").Where(qb.Where("school_id", qb.OpEq, 2))
	sql, args := mustSelect(t, a.UnionAll(b))
	assertSQL(t, sql, `SELECT * FROM "students" WHERE "school_id" = $1 UNION ALL SELECT * FROM "alumni" WHERE "school_id" = $2`)
	assertArgs(t, args, 1, 2)
}

// ─── Window functions ─────────────────────────────────────────────────────────

func TestWindowCol_PartitionAndOrder(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("orders").
		Columns("user_id", "country", "total").
		WindowCol("RANK()", "country", "total DESC", "rank"))
	assertSQL(t, sql, `SELECT "user_id", "country", "total", RANK() OVER (PARTITION BY "country" ORDER BY total DESC) AS "rank" FROM "orders"`)
}

func TestWindowCol_OrderOnly(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("orders").
		Columns("user_id", "total").
		WindowCol("ROW_NUMBER()", "", "created_at ASC", "row_num"))
	assertSQL(t, sql, `SELECT "user_id", "total", ROW_NUMBER() OVER (ORDER BY created_at ASC) AS "row_num" FROM "orders"`)
}

func TestWindowCol_PartitionOnly(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("sales").
		Columns("region", "amount").
		WindowCol("SUM(amount)", "region", "", "region_total"))
	assertSQL(t, sql, `SELECT "region", "amount", SUM(amount) OVER (PARTITION BY "region") AS "region_total" FROM "sales"`)
}

func TestWindowCol_Multiple(t *testing.T) {
	sql, _ := mustSelect(t, qb.New("orders").
		Columns("user_id", "total").
		WindowCol("RANK()", "country", "total DESC", "rank").
		WindowCol("LAG(total)", "country", "created_at", "prev_total"))
	if !strings.Contains(sql, "RANK() OVER") {
		t.Error("expected RANK() OVER")
	}
	if !strings.Contains(sql, "LAG(total) OVER") {
		t.Error("expected LAG(total) OVER")
	}
}

// ─── BUILD INSERT ─────────────────────────────────────────────────────────────

func TestBuildInsert_Basic(t *testing.T) {
	sql, args := mustInsert(t, qb.New("users"), map[string]any{
		"name":  "Alice",
		"email": "alice@example.com",
	})
	// keys are sorted: email, name
	assertSQL(t, sql, `INSERT INTO "users" ("email", "name") VALUES ($1, $2) RETURNING "id"`)
	assertArgs(t, args, "alice@example.com", "Alice")
}

func TestBuildInsert_OnConflict_DoNothing(t *testing.T) {
	sql, _ := mustInsert(t,
		qb.New("users").OnConflict("(email) DO NOTHING"),
		map[string]any{"email": "a@b.com", "name": "A"},
	)
	if !strings.Contains(sql, "ON CONFLICT (email) DO NOTHING") {
		t.Errorf("missing ON CONFLICT clause: %s", sql)
	}
}

func TestBuildInsert_OnConflict_Upsert(t *testing.T) {
	sql, _ := mustInsert(t,
		qb.New("users").OnConflict("(email) DO UPDATE SET name = EXCLUDED.name"),
		map[string]any{"email": "a@b.com", "name": "A"},
	)
	if !strings.Contains(sql, "DO UPDATE SET name = EXCLUDED.name") {
		t.Errorf("missing upsert clause: %s", sql)
	}
}

func TestBuildInsert_ReturningAll(t *testing.T) {
	sql, _ := mustInsert(t,
		qb.New("users").ReturningAll(),
		map[string]any{"name": "Bob"},
	)
	if !strings.Contains(sql, "RETURNING *") {
		t.Errorf("expected RETURNING *: %s", sql)
	}
}

func TestBuildInsert_ReturningColumns(t *testing.T) {
	sql, _ := mustInsert(t,
		qb.New("users").Returning("id", "created_at"),
		map[string]any{"name": "Bob"},
	)
	assertSQL(t, sql, `INSERT INTO "users" ("name") VALUES ($1) RETURNING "id", "created_at"`)
}

func TestBuildInsert_ReturningNone(t *testing.T) {
	sql, _ := mustInsert(t,
		qb.New("logs").ReturningNone(),
		map[string]any{"message": "hello"},
	)
	if strings.Contains(sql, "RETURNING") {
		t.Errorf("unexpected RETURNING: %s", sql)
	}
}

func TestBuildInsert_Empty_Error(t *testing.T) {
	_, _, err := qb.New("users").BuildInsert(map[string]any{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

// ─── BUILD INSERT BATCH ───────────────────────────────────────────────────────

func TestBuildInsertBatch(t *testing.T) {
	sql, args := func() (string, []any) {
		s, a, err := qb.New("users").BuildInsertBatch([]map[string]any{
			{"email": "a@b.com", "name": "Alice"},
			{"email": "b@c.com", "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("BuildInsertBatch error: %v", err)
		}
		return s, a
	}()
	assertSQL(t, sql, `INSERT INTO "users" ("email", "name") VALUES ($1, $2), ($3, $4) RETURNING "id"`)
	assertArgs(t, args, "a@b.com", "Alice", "b@c.com", "Bob")
}

func TestBuildInsertBatch_Empty_Error(t *testing.T) {
	_, _, err := qb.New("users").BuildInsertBatch(nil)
	if err == nil {
		t.Fatal("expected error for empty rows")
	}
}

func TestBuildInsertBatch_MissingKey_Error(t *testing.T) {
	_, _, err := qb.New("users").BuildInsertBatch([]map[string]any{
		{"name": "Alice", "email": "a@b.com"},
		{"name": "Bob"}, // missing email
	})
	if err == nil {
		t.Fatal("expected error for missing column in batch row")
	}
}

// ─── BUILD UPDATE ─────────────────────────────────────────────────────────────

func TestBuildUpdate_Basic(t *testing.T) {
	sql, args := mustUpdate(t,
		qb.New("users").Where(qb.Where("id", qb.OpEq, 42)),
		map[string]any{"name": "Alice Updated", "status": "active"},
	)
	// keys sorted: name, status
	assertSQL(t, sql, `UPDATE "users" SET "name" = $1, "status" = $2 WHERE "id" = $3`)
	assertArgs(t, args, "Alice Updated", "active", 42)
}

func TestBuildUpdate_NoWhere(t *testing.T) {
	sql, args := mustUpdate(t,
		qb.New("settings"),
		map[string]any{"maintenance": true},
	)
	assertSQL(t, sql, `UPDATE "settings" SET "maintenance" = $1`)
	assertArgs(t, args, true)
}

func TestBuildUpdate_Returning(t *testing.T) {
	sql, _ := mustUpdate(t,
		qb.New("users").Where(qb.Where("id", qb.OpEq, 1)).Returning("id", "updated_at"),
		map[string]any{"name": "X"},
	)
	if !strings.Contains(sql, `RETURNING "id", "updated_at"`) {
		t.Errorf("missing RETURNING: %s", sql)
	}
}

func TestBuildUpdate_Empty_Error(t *testing.T) {
	_, _, err := qb.New("users").BuildUpdate(map[string]any{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

// ─── BUILD DELETE ─────────────────────────────────────────────────────────────

func TestBuildDelete_Basic(t *testing.T) {
	sql, args := mustDelete(t, qb.New("users").Where(qb.Where("id", qb.OpEq, 99)))
	assertSQL(t, sql, `DELETE FROM "users" WHERE "id" = $1`)
	assertArgs(t, args, 99)
}

func TestBuildDelete_NoWhere(t *testing.T) {
	sql, args := mustDelete(t, qb.New("temp_sessions"))
	assertSQL(t, sql, `DELETE FROM "temp_sessions"`)
	assertArgs(t, args)
}

func TestBuildDelete_Returning(t *testing.T) {
	sql, _ := mustDelete(t, qb.New("users").
		Where(qb.Where("id", qb.OpEq, 1)).
		Returning("id", "email"))
	if !strings.Contains(sql, `RETURNING "id", "email"`) {
		t.Errorf("missing RETURNING: %s", sql)
	}
}

func TestBuildDelete_MultipleWhere(t *testing.T) {
	sql, args := mustDelete(t, qb.New("sessions").
		Where(qb.Where("user_id", qb.OpEq, 5)).
		Where(qb.Where("expired", qb.OpEq, true)))
	assertSQL(t, sql, `DELETE FROM "sessions" WHERE "user_id" = $1 AND "expired" = $2`)
	assertArgs(t, args, 5, true)
}

// ─── Error accumulation ───────────────────────────────────────────────────────

func TestWhere_EmptyColumn_AccumulatesError(t *testing.T) {
	b := qb.New("users").Where(qb.Condition{Column: "", Operator: qb.OpEq, Value: 1})
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for empty column name")
	}
}

func TestWhere_In_EmptySlice_Error(t *testing.T) {
	b := qb.New("users").Where(qb.WhereIn("id", []int{}))
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for empty IN slice")
	}
}

// ─── QuoteIdent ───────────────────────────────────────────────────────────────

func TestQuoteIdent(t *testing.T) {
	cases := []struct{ input, want string }{
		{"users", `"users"`},
		{"my table", `"my table"`},
		{"user_id", `"user_id"`},
	}
	for _, tc := range cases {
		got := qb.QuoteIdent(tc.input)
		if got != tc.want {
			t.Errorf("QuoteIdent(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ─── Full query: real-world example ──────────────────────────────────────────

func TestFullQuery_StudentReport(t *testing.T) {
	activeSchools := qb.New("schools").
		Columns("id").
		Where(qb.Where("active", qb.OpEq, true))

	sql, args := mustSelect(t,
		qb.New("students s").
			Columns("s.id", "s.name", "COUNT(e.id) AS enrollments").
			LeftJoin("enrollments e", "e.student_id = s.id").
			Where(qb.WhereSubquery("s.school_id", qb.OpIn, activeSchools)).
			Where(qb.WhereNotNull("s.verified_at")).
			WhereGroup(qb.OrGroup(
				qb.Where("s.grade", qb.OpGte, 9),
				qb.Where("s.special", qb.OpEq, true),
			)).
			GroupBy("s.id", "s.name").
			Having("COUNT(e.id) > ?", 2).
			OrderBy("enrollments", qb.Desc, qb.NullsLast).
			Limit(50).
			Offset(0),
	)

	if !strings.Contains(sql, `LEFT JOIN enrollments e`) {
		t.Error("missing LEFT JOIN")
	}
	if !strings.Contains(sql, `IN (SELECT "id" FROM "schools"`) {
		t.Error("missing subquery IN")
	}
	if !strings.Contains(sql, `IS NOT NULL`) {
		t.Error("missing IS NOT NULL")
	}
	if !strings.Contains(sql, `HAVING COUNT(e.id) > $`) {
		t.Error("missing HAVING")
	}
	if !strings.Contains(sql, `NULLS LAST`) {
		t.Error("missing NULLS LAST")
	}
	// $1 = true (active schools subquery), then outer WHERE args continue
	if args[0] != true {
		t.Errorf("first arg: got %v, want true", args[0])
	}
}
