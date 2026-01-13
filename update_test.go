package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateBuilderToSql(t *testing.T) {
	t.Parallel()
	b := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Set("c1", Case("status").When("1", 2).When("2", 1)).
		Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
		Set("c3", Select("a").From("b")).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.ToSql()
	require.NoError(t, err)

	expectedSql := "WITH prefix AS ? " +
		"UPDATE a SET b = ? + 1, c = ?, " +
		"c1 = CASE status WHEN 1 THEN CAST(? AS bigint) WHEN 2 THEN CAST(? AS bigint) END, " +
		"c2 = CASE WHEN a = 2 THEN ? WHEN a = 3 THEN ? END, " +
		"c3 = (SELECT a FROM b) " +
		"WHERE d = ? " +
		"ORDER BY e LIMIT 4 OFFSET 5 " +
		"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{0, 1, 2, 2, 1, "foo", "bar", 3, 6}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderToSqlErr(t *testing.T) {
	t.Parallel()
	_, _, err := Update("").Set("x", 1).ToSql()
	require.Error(t, err)

	_, _, err = Update("x").ToSql()
	require.Error(t, err)
}

func TestUpdateBuilderMustSql(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestUpdateBuilderMustSql should have panicked!")
		}
	}()
	Update("").MustSql()
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Update("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}

func TestUpdateBuilderFrom(t *testing.T) {
	t.Parallel()
	sql, _, err := Update("employees").Set("sales_count", 100).From("accounts").Where("accounts.name = ?", "ACME").ToSql()
	require.NoError(t, err)
	assert.Equal(t, "UPDATE employees SET sales_count = ? FROM accounts WHERE accounts.name = ?", sql)
}

func TestUpdateBuilderFromSelect(t *testing.T) {
	t.Parallel()
	sql, _, err := Update("employees").
		Set("sales_count", 100).
		FromSelect(Select("id").
			From("accounts").
			Where("accounts.name = ?", "ACME"), "subquery").
		Where("employees.account_id = subquery.id").ToSql()
	require.NoError(t, err)

	expectedSql := "UPDATE employees " +
		"SET sales_count = ? " +
		"FROM (SELECT id FROM accounts WHERE accounts.name = ?) AS subquery " +
		"WHERE employees.account_id = subquery.id"
	assert.Equal(t, expectedSql, sql)
}

func TestUpdateSetWithNestedSelect_DollarPlaceholderNumberingConflict(t *testing.T) {
	t.Parallel()
	b := StatementBuilder.PlaceholderFormat(Dollar)

	sub := b.Select("max(val)").From("t2").Where("id = ?", 11)

	q := b.Update("t1").
		Set("col", sub).
		Where("id = ?", 12)

	sql, args, err := q.ToSql()
	require.NoError(t, err)

	expectedSQL := "UPDATE t1 SET col = (SELECT max(val) FROM t2 WHERE id = $1) WHERE id = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{11, 12}, args)
}
