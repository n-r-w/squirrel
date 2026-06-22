---
name: squirrel
description: Guidelines for using `github.com/n-r-w/squirrel` package.
metadata:
  author: Roman Nikulenkov
---

<squirrel name="github.com/n-r-w/squirrel guidelines">
    <instructions>
        1. `github.com/n-r-w/squirrel` is a enhanced fork of the original `github.com/Masterminds/squirrel` package.
        2. Most of the API remains the same, but there are additional features and improvements with minimal breaking changes:
            1) **Breaking Changes:**
                * `Case` expression breaking changes:
                    - To pass an integer value to the `When` and `Else` methods, you need to pass it as an int, not as a string
                    - To pass a string value to the `When` and `Else` methods, you don't need to add quotes
            2) **New Features:**
                * Subquery support for `WHERE` clause
                * Support for integer values in `CASE THEN/ELSE` clause
                * Support for aggregate functions `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`
                * Support for using slice as argument for `Column` function
                * Support for `IN`, `NOT` and `NOT IN` clause
                * Equal, NotEqual, Greater, GreaterOrEqual, Less, LessOrEqual functions
                * Coalesce expression
                * Range function
                * EqNotEmpty function: ignores empty and zero values in Eq map. Useful for filtering
                * OrderByCond function: can be used to avoid hardcoding column names in the code
                * Search function: The search condition is a WHERE clause with LIKE expressions. All columns will be converted to text. Value can be a string or a number
                * Paginate: Allows you to use separated Paginator object to paginate the query
                * PaginateByID: Adds a LIMIT and start from ID condition to the query. WARNING: The columnID must be included in the ORDER BY clause to avoid unexpected results
                * PaginateByPage: Adds a LIMIT and OFFSET to the query. WARNING: The columnID must be included in the ORDER BY clause to avoid unexpected results
                * Alias for Select statement: allows to use table alias in the query for multiple columns and add prefix to the column names if needed
                * CTE support. `As` accepts any `Sqlizer`, including `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, and `DeleteBuilder`. Use this for PostgreSQL DML CTE bodies such as `UPDATE ... RETURNING` without raw `Expr`.
    </instructions>
    <examples>
        <example name="complex-select-with-cte">
            ```go
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
            ```
        </example>
        <example name="advanced-select-with-cte-and-joins">
            ```
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
            ```
        </example>
        <example name="insert-update-with-returning-and-cte">
            ```go
            insertProducts := sq.Insert("products").
                Columns("name", "stock", "price", "active").
                Values("Widget", 10, 25.5, true).
                Values("Gadget", 3, 15.0, true).
                Values("Legacy", 1, 5.0, false).
                Suffix("RETURNING id, name, stock").
                PlaceholderFormat(sq.Dollar)

            sql, args, err := insertProducts.ToSql()
            require.NoError(t, err)

            rows, err := pool.Query(ctx, sql, args...)
            require.NoError(t, err)
            t.Cleanup(rows.Close)

            productIDs := make(map[string]int64)
            for rows.Next() {
                var id int64
                var name string
                var stock int
                err := rows.Scan(&id, &name, &stock)
                require.NoError(t, err)
                productIDs[name] = id
            }
            require.NoError(t, rows.Err())
            require.Len(t, productIDs, 3)

            insertSales := sq.Insert("sales").
                Columns("product_id", "qty").
                Values(productIDs["Widget"], 4).
                Values(productIDs["Widget"], 2).
                Values(productIDs["Gadget"], 1).
                PlaceholderFormat(sq.Dollar)

            sql, args, err = insertSales.ToSql()
            ```
        </example>
        <example name="update-with-cte-and-from-select">
            ```go
            salesAgg := sq.Select("product_id", "SUM(qty) AS qty_sold").
                From("sales").
                GroupBy("product_id")

            updateQuery := sq.Update("products p").
                Set("stock", sq.Expr("p.stock - s.qty_sold")).
                FromSelect(salesAgg, "s").
                Where("s.product_id = p.id").
                Where(sq.Eq{"p.active": true}).
                Suffix("RETURNING p.id, p.stock").
                PlaceholderFormat(sq.Dollar)

            sql, args, err = updateQuery.ToSql()
            ```
        </example>
        <example name="update-returning-as-intermediate-cte">
            ```go
            candidate := sq.Select("id").
                From("delivery_messages").
                Where("state = ?", "ready").
                OrderBy("id").
                Limit(1).
                Suffix("FOR UPDATE SKIP LOCKED")

            picked := sq.Select("id").
                From("candidate")

            updated := sq.Update("delivery_messages dm").
                Set("state", "claimed").
                From("picked p").
                Where("dm.id = p.id").
                Suffix("RETURNING dm.id, dm.state")

            query := sq.With("candidate").As(candidate).
                Cte("picked").As(picked).
                Cte("updated").As(updated).
                Select(
                    sq.Select("id", "state").
                        From("updated").
                        OrderBy("id"),
                ).
                PlaceholderFormat(sq.Dollar)

            sql, args, err := query.ToSql()
            ```
        </example>
        <example>
            ```go
            insertArchive := sq.Insert("archived_products").
                Prefix("/* archive */").
                Columns("name", "stock", "price", "active").
                Select(
                    sq.Select("name", "stock", "price", "active").
                        From("products").
                        Where(sq.Eq{"active": false}),
                ).
                PlaceholderFormat(sq.Dollar)

            sql, args, err = insertArchive.ToSql()
            ```
        </example>
    </examples>
</squirrel>