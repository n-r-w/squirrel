//go:build itest

package itests

import (
	"testing"

	"github.com/georgysavva/scany/v2/pgxscan"
	sq "github.com/n-r-w/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectBuilderComplexQuery(t *testing.T) {
	t.Parallel()

	pool, ctx := newTestPool(t)
	setupSQL := `
CREATE TABLE users (
	id bigserial PRIMARY KEY,
	name text NOT NULL,
	email text NOT NULL,
	status text NOT NULL,
	age integer NOT NULL
);
CREATE TABLE orders (
	id bigserial PRIMARY KEY,
	user_id bigint NOT NULL REFERENCES users(id),
	amount double precision NOT NULL,
	state text NOT NULL
);
INSERT INTO users (name, email, status, age) VALUES
	('Alice', 'alice@example.com', 'active', 30),
	('Bob', 'bob@example.com', 'inactive', 35),
	('Cara', 'cara@example.com', 'active', 42),
	('Dan', 'dan@example.com', 'active', 19);
INSERT INTO orders (user_id, amount, state) VALUES
	(1, 120.5, 'paid'),
	(1, 40, 'refunded'),
	(2, 15, 'refunded'),
	(3, 200, 'paid'),
	(3, 50, 'paid'),
	(4, 5, 'paid');
`
	execSetup(t, pool, ctx, setupSQL)

	orderStats := sq.Select("o.user_id").
		Column(sq.Alias(sq.Sum(sq.Expr("o.amount")), "total_amount")).
		Column(sq.Alias(sq.Count(sq.Expr("o.id")), "paid_count")).
		From("orders o").
		Where(sq.Eq{"o.state": "paid"}).
		GroupBy("o.user_id")

	refundedExists := sq.Exists(
		sq.Select("1").
			From("orders r").
			Where(sq.And{
				sq.Expr("r.user_id = u.id"),
				sq.Eq{"r.state": "refunded"},
			}),
	)

	statusCase := sq.Case().
		When(sq.Expr("u.status = ?", "active"), "active").
		Else("inactive")

	selectQuery := sq.Select("u.id", "u.name").
		Column(sq.Alias(sq.Coalesce(0.0, sq.Expr("os.total_amount")), "total_amount")).
		Column(sq.Alias(sq.Coalesce(0, sq.Expr("os.paid_count")), "paid_count")).
		Column(sq.Alias(statusCase, "status_label")).
		Column(sq.Alias(refundedExists, "has_refunds")).
		Column(sq.Alias(sq.Count(sq.Expr("o.id")), "orders_count")).
		From("users u").
		LeftJoin("order_stats os ON os.user_id = u.id").
		LeftJoin("orders o ON o.user_id = u.id").
		Where(sq.And{
			sq.Range("u.age", 20, 45),
			sq.Or{
				sq.Eq{"u.status": "active"},
				sq.Like{"u.email": "%example.com"},
			},
		}).
		Search("example.com", "u.email").
		GroupBy("u.id", "u.name", "os.total_amount", "os.paid_count", "u.status").
		Having(sq.Expr("COUNT(o.id) >= ?", 1)).
		OrderBy("total_amount DESC", "u.id").
		Limit(3).
		Offset(0)

	query := sq.With("order_stats").As(orderStats).
		Select(selectQuery).
		PlaceholderFormat(sq.Dollar)

	sql, args, err := query.ToSql()
	require.NoError(t, err)

	type selectResult struct {
		ID          int64   `db:"id"`
		Name        string  `db:"name"`
		TotalAmount float64 `db:"total_amount"`
		PaidCount   int64   `db:"paid_count"`
		StatusLabel string  `db:"status_label"`
		HasRefunds  bool    `db:"has_refunds"`
		OrdersCount int64   `db:"orders_count"`
	}

	var results []selectResult
	err = pgxscan.Select(ctx, pool, &results, sql, args...)
	require.NoError(t, err)

	expected := []selectResult{
		{
			ID:          3,
			Name:        "Cara",
			TotalAmount: 250,
			PaidCount:   2,
			StatusLabel: "active",
			HasRefunds:  false,
			OrdersCount: 2,
		},
		{
			ID:          1,
			Name:        "Alice",
			TotalAmount: 120.5,
			PaidCount:   1,
			StatusLabel: "active",
			HasRefunds:  true,
			OrdersCount: 2,
		},
		{
			ID:          2,
			Name:        "Bob",
			TotalAmount: 0,
			PaidCount:   0,
			StatusLabel: "inactive",
			HasRefunds:  true,
			OrdersCount: 1,
		},
	}

	assert.Equal(t, expected, results)
}

func TestSelectBuilderAllConstructs(t *testing.T) {
	t.Parallel()

	pool, ctx := newTestPool(t)
	setupSQL := `
CREATE TABLE departments_all (
	id bigserial PRIMARY KEY,
	name text NOT NULL
);
CREATE TABLE users_all (
	id bigserial PRIMARY KEY,
	name text NOT NULL,
	email text NOT NULL,
	status text NOT NULL,
	age integer NOT NULL,
	department_id bigint NOT NULL REFERENCES departments_all(id)
);
CREATE TABLE emails_all (
	id bigserial PRIMARY KEY,
	user_id bigint NOT NULL REFERENCES users_all(id),
	address text NOT NULL,
	verified boolean NOT NULL
);
CREATE TABLE orders_all (
	id bigserial PRIMARY KEY,
	user_id bigint NOT NULL REFERENCES users_all(id),
	amount double precision NOT NULL,
	state text NOT NULL
);
CREATE TABLE groups_all (
	id bigserial PRIMARY KEY,
	name text NOT NULL
);
CREATE TABLE user_groups_all (
	user_id bigint NOT NULL REFERENCES users_all(id),
	group_id bigint NOT NULL REFERENCES groups_all(id)
);
INSERT INTO departments_all (id, name) VALUES
	(1, 'Engineering'),
	(2, 'Sales');
INSERT INTO users_all (id, name, email, status, age, department_id) VALUES
	(1, 'Alice', 'alice@example.com', 'active', 30, 1),
	(2, 'Bob', 'bob@example.com', 'inactive', 35, 1),
	(3, 'Cara', 'cara@example.com', 'active', 42, 2),
	(4, 'Dan', 'dan@example.com', 'pending', 25, 2);
INSERT INTO emails_all (user_id, address, verified) VALUES
	(1, 'alice@work.com', true),
	(3, 'cara@work.com', false);
INSERT INTO orders_all (user_id, amount, state) VALUES
	(1, 100, 'paid'),
	(1, 20, 'refunded'),
	(2, 15, 'refunded'),
	(3, 200, 'paid'),
	(3, 50, 'paid'),
	(4, 5, 'paid');
INSERT INTO groups_all (id, name) VALUES
	(1, 'core'),
	(2, 'banned');
INSERT INTO user_groups_all (user_id, group_id) VALUES
	(1, 1),
	(2, 1),
	(3, 1),
	(3, 2),
	(4, 1);
`
	execSetup(t, pool, ctx, setupSQL)

	activeUsers := sq.Select("id", "name", "email", "status", "age", "department_id").
		From("users_all").
		Where(sq.Eq{"status": []string{"active", "pending", "inactive"}})

	orderTotals := sq.Select("o.user_id").
		Column(sq.Alias(sq.Sum(sq.Expr("o.amount")), "sum_amount")).
		Column(sq.Alias(sq.Count(sq.Expr("o.id")), "orders_count")).
		Column(sq.Alias(sq.Min(sq.Expr("o.amount")), "min_amount")).
		Column(sq.Alias(sq.Max(sq.Expr("o.amount")), "max_amount")).
		Column(sq.Alias(sq.Avg(sq.Expr("o.amount")), "avg_amount")).
		From("orders_all o").
		GroupBy("o.user_id")

	groupSub := sq.Select("ug.user_id").
		From("user_groups_all ug").
		Where(sq.Eq{"ug.group_id": 1})

	bannedSub := sq.Select("ug.user_id").
		From("user_groups_all ug").
		Where(sq.Eq{"ug.group_id": 2})

	groupJoin := sq.Select("ug.user_id").
		From("user_groups_all ug").
		Where(sq.Eq{"ug.group_id": 1}).
		Prefix("JOIN (").
		Suffix(") ug_filter ON ug_filter.user_id = au.id")

	statusCase := sq.Case("au.status").
		When(sq.Expr("?", "active"), 1).
		When(sq.Expr("?", "pending"), 2).
		Else(0)

	displayName := sq.ConcatExpr(
		"CONCAT(",
		sq.Expr("au.name"),
		", ' <', ",
		sq.Expr("au.email"),
		", '>')",
	)

	refundedExists := sq.Exists(
		sq.Select("1").
			From("orders_all r").
			Where(sq.Expr("r.user_id = au.id")).
			Where(sq.Eq{"r.state": "refunded"}),
	)

	noChargebacks := sq.NotExists(
		sq.Select("1").
			From("orders_all cb").
			Where(sq.Expr("cb.user_id = au.id")).
			Where(sq.Eq{"cb.state": "chargeback"}),
	)

	deptSub := sq.Select("d.id").
		From("departments_all d").
		Where(sq.Expr("d.id = au.department_id"))

	orderByColumns := map[int]string{
		1: "au.status",
		2: "au.name",
		3: "au.id",
	}
	orderByConds := []sq.OrderCond{
		{ColumnID: 1, Direction: sq.Asc},
		{ColumnID: 2, Direction: sq.Asc},
		{ColumnID: 3, Direction: sq.Desc},
	}

	selectQuery := sq.Select().
		PrefixExpr(sq.Expr("/* select-all-constructs */")).
		Options("DISTINCT ON (au.status)").
		Alias("au", "pref").Columns("id", "name").
		Column(sq.Alias(displayName, "display_name")).
		Column(sq.Alias(sq.Coalesce("n/a", sq.Expr("e.address")), "email_label")).
		Column(sq.Alias(statusCase, "status_rank")).
		Column(sq.Alias(refundedExists, "has_refunds")).
		Column(sq.Alias(noChargebacks, "no_chargebacks")).
		Column(sq.Alias(sq.Coalesce(0.0, sq.Expr("ot.sum_amount")), "sum_amount")).
		Column(sq.Alias(sq.Coalesce(0, sq.Expr("ot.orders_count")), "orders_count")).
		Column(sq.Alias(sq.Expr("ot.min_amount"), "min_amount")).
		Column(sq.Alias(sq.Expr("ot.max_amount"), "max_amount")).
		Column(sq.Alias(sq.Expr("ot.avg_amount"), "avg_amount")).
		From("active_users au").
		Join("order_totals ot ON ot.user_id = au.id").
		InnerJoin("orders_all o ON o.user_id = au.id").
		LeftJoin("emails_all e ON e.user_id = au.id").
		RightJoin("departments_all d ON d.id = au.department_id").
		CrossJoin("groups_all g").
		JoinClause(groupJoin).
		Where(sq.And{
			sq.Range("au.age", 20, 35),
			sq.Or{
				sq.EqNotEmpty{"au.status": "active", "au.name": ""},
				sq.Eq{"au.status": "pending"},
			},
			sq.Not(sq.Expr("au.status = ?", "inactive")),
			sq.Eq{"au.id": []int64{1, 2, 3, 4}},
			sq.Eq{"g.id": 1},
			sq.NotEq{"au.email": "blocked@example.com"},
			sq.Eq{"au.department_id": deptSub},
			sq.In("au.id", groupSub),
			sq.NotIn("au.id", bannedSub),
			sq.Like{"au.email": "%@example.com"},
			sq.NotLike{"au.email": "%@spam.com"},
			sq.ILike{"au.name": "%a%"},
			sq.NotILike{"au.name": "%zzz%"},
			sq.Equal(sq.Select("1"), 1),
			sq.NotEqual(sq.Select("1"), 2),
			sq.Greater(sq.Select("2"), 1),
			sq.GreaterOrEqual(sq.Select("2"), 2),
			sq.Less(sq.Select("1"), 2),
			sq.LessOrEqual(sq.Select("1"), 1),
			refundedExists,
			noChargebacks,
		}).
		Search("example.com", "au.email", "au.name").
		GroupBy(
			"au.id",
			"au.name",
			"au.email",
			"au.status",
			"e.address",
			"ot.sum_amount",
			"ot.orders_count",
			"ot.min_amount",
			"ot.max_amount",
			"ot.avg_amount",
		).
		Having(sq.Expr("COUNT(o.id) >= ?", 1)).
		OrderByCond(
			orderByColumns,
			orderByConds,
			sq.OrderByCondOption{ColumnID: 2, NullsType: sq.OrderNullsLast},
		).
		Limit(10).
		Offset(0)

	query := sq.With("active_users").As(activeUsers).
		Cte("order_totals").As(orderTotals).
		Select(selectQuery).
		PlaceholderFormat(sq.Dollar)

	sql, args, err := query.ToSql()
	require.NoError(t, err)

	type selectResult struct {
		PrefID        int64   `db:"pref_id"`
		PrefName      string  `db:"pref_name"`
		DisplayName   string  `db:"display_name"`
		EmailLabel    string  `db:"email_label"`
		StatusRank    int     `db:"status_rank"`
		HasRefunds    bool    `db:"has_refunds"`
		NoChargebacks bool    `db:"no_chargebacks"`
		SumAmount     float64 `db:"sum_amount"`
		OrdersCount   int64   `db:"orders_count"`
		MinAmount     float64 `db:"min_amount"`
		MaxAmount     float64 `db:"max_amount"`
		AvgAmount     float64 `db:"avg_amount"`
	}

	var results []selectResult
	err = pgxscan.Select(ctx, pool, &results, sql, args...)
	require.NoError(t, err)
	require.Len(t, results, 1)

	got := results[0]

	assert.Equal(t, int64(1), got.PrefID)
	assert.Equal(t, "Alice", got.PrefName)
	assert.Equal(t, "Alice <alice@example.com>", got.DisplayName)
	assert.Equal(t, "alice@work.com", got.EmailLabel)
	assert.Equal(t, 1, got.StatusRank)
	assert.True(t, got.HasRefunds)
	assert.True(t, got.NoChargebacks)
	assert.InEpsilon(t, 120.0, got.SumAmount, 0.0001)
	assert.Equal(t, int64(2), got.OrdersCount)
	assert.InEpsilon(t, 20.0, got.MinAmount, 0.0001)
	assert.InEpsilon(t, 100.0, got.MaxAmount, 0.0001)
	assert.InEpsilon(t, 60.0, got.AvgAmount, 0.0001)

	builderResetQuery := sq.Select("id").
		Distinct().
		FromSelect(activeUsers, "au").
		RemoveColumns().
		Columns("au.id", "au.name").
		OrderBy("au.id").
		Limit(1).
		Offset(1).
		RemoveLimit().
		RemoveOffset().
		Prefix("/* cleanup */").
		PlaceholderFormat(sq.Dollar)

	cleanedIDs, _ := queryInt64StringPairs(t, pool, ctx, builderResetQuery)
	assert.Len(t, cleanedIDs, 4)

	paginateByID := sq.Select("id").
		From("users_all").
		OrderBy("id").
		PaginateByID(2, 1, "id").
		PlaceholderFormat(sq.Dollar)

	pageByID := queryInt64s(t, pool, ctx, paginateByID)
	assert.Equal(t, []int64{2, 3}, pageByID)

	paginateByPage := sq.Select("id").
		From("users_all").
		OrderBy("id").
		PaginateByPage(2, 2).
		PlaceholderFormat(sq.Dollar)

	pageByPage := queryInt64s(t, pool, ctx, paginateByPage)
	assert.Equal(t, []int64{3, 4}, pageByPage)

	paginateByPaginator := sq.Select("id").
		From("users_all").
		OrderBy("id").
		Paginate(sq.PaginatorByID(2, 1)).
		SetIDColumn("id").
		PlaceholderFormat(sq.Dollar)

	pageByPaginator := queryInt64s(t, pool, ctx, paginateByPaginator)
	assert.Equal(t, []int64{2, 3}, pageByPaginator)

	paginateByPagePaginator := sq.Select("id").
		From("users_all").
		OrderBy("id").
		Paginate(sq.PaginatorByPage(2, 2)).
		PlaceholderFormat(sq.Dollar)

	pageByPagePaginator := queryInt64s(t, pool, ctx, paginateByPagePaginator)
	assert.Equal(t, []int64{3, 4}, pageByPagePaginator)

	recursiveQuery := sq.WithRecursive("category_tree").As(
		sq.Select("id", "name").
			From("departments_all").
			Where(sq.Eq{"id": 1}),
	).Select(
		sq.Select("id", "name").
			From("category_tree"),
	).PlaceholderFormat(sq.Dollar)

	recursiveIDs, recursiveNames := queryInt64StringPairs(t, pool, ctx, recursiveQuery)
	assert.Equal(t, []int64{1}, recursiveIDs)
	assert.Equal(t, []string{"Engineering"}, recursiveNames)
}
