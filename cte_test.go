package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithAsQuery_OneSubquery(t *testing.T) {
	t.Parallel()
	w := With("lab").As(
		Select("col").From("tab").
			Where("simple AND NOT hard"),
	).Select(
		Select("col").
			From("lab"),
	)
	q, _, err := w.ToSql()
	require.NoError(t, err)

	expectedSql := "WITH lab AS (SELECT col FROM tab WHERE simple AND NOT hard) SELECT col FROM lab"
	assert.Equal(t, expectedSql, q)

	w = WithRecursive("lab").As(
		Select("col").From("tab").
			Where("simple").
			Where("NOT hard"),
	).Select(Select("col").From("lab"))
	q, _, err = w.ToSql()
	require.NoError(t, err)

	expectedSql = "WITH RECURSIVE lab AS (" +
		"SELECT col FROM tab WHERE simple AND NOT hard" +
		") " +
		"SELECT col FROM lab"
	assert.Equal(t, expectedSql, q)
}

func TestWithAsQuery_TwoSubqueries(t *testing.T) {
	t.Parallel()
	w := With("lab_1").As(
		Select("col_1", "col_common").From("tab_1").
			Where("simple").
			Where("NOT hard"),
	).Cte("lab_2").As(
		Select("col_2", "col_common").From("tab_2"),
	).Select(Select("col_1", "col_2", "col_common").
		From("lab_1").Join("lab_2 ON lab_1.col_common = lab_2.col_common"),
	)
	q, _, err := w.ToSql()
	require.NoError(t, err)

	expectedSql := "WITH lab_1 AS (" +
		"SELECT col_1, col_common FROM tab_1 WHERE simple AND NOT hard" +
		"), lab_2 AS (" +
		"SELECT col_2, col_common FROM tab_2" +
		") " +
		"SELECT col_1, col_2, col_common FROM lab_1 JOIN lab_2 ON lab_1.col_common = lab_2.col_common"
	assert.Equal(t, expectedSql, q)
}

func TestWithAsQuery_ManySubqueries(t *testing.T) {
	t.Parallel()
	w := With("lab_1").As(
		Select("col_1", "col_common").From("tab_1").
			Where("simple").
			Where("NOT hard"),
	).Cte("lab_2").As(
		Select("col_2", "col_common").From("tab_2"),
	).Cte("lab_3").As(
		Select("col_3", "col_common").From("tab_3"),
	).Cte("lab_4").As(
		Select("col_4", "col_common").From("tab_4"),
	).Select(
		Select("col_1", "col_2", "col_3", "col_4", "col_common").
			From("lab_1").Join("lab_2 ON lab_1.col_common = lab_2.col_common").
			Join("lab_3 ON lab_1.col_common = lab_3.col_common").
			Join("lab_4 ON lab_1.col_common = lab_4.col_common"))
	q, _, err := w.ToSql()
	require.NoError(t, err)

	expectedSql := "WITH lab_1 AS (" +
		"SELECT col_1, col_common FROM tab_1 WHERE simple AND NOT hard" +
		"), lab_2 AS (" +
		"SELECT col_2, col_common FROM tab_2" +
		"), lab_3 AS (" +
		"SELECT col_3, col_common FROM tab_3" +
		"), lab_4 AS (" +
		"SELECT col_4, col_common FROM tab_4" +
		") " +
		"SELECT col_1, col_2, col_3, col_4, col_common FROM lab_1 JOIN lab_2 ON lab_1.col_common = lab_2.col_common JOIN lab_3 ON lab_1.col_common = lab_3.col_common JOIN lab_4 ON lab_1.col_common = lab_4.col_common"
	assert.Equal(t, expectedSql, q)
}

func TestWithAsQuery_Insert(t *testing.T) {
	t.Parallel()
	w := With("lab").As(
		Select("col").From("tab").
			Where("simple").
			Where("NOT hard"),
	).Insert(Insert("ins_tab").Columns("ins_col").Select(Select("col").From("lab")))
	q, _, err := w.ToSql()
	require.NoError(t, err)

	expectedSql := "WITH lab AS (" +
		"SELECT col FROM tab WHERE simple AND NOT hard" +
		") " +
		"INSERT INTO ins_tab (ins_col) SELECT col FROM lab"
	assert.Equal(t, expectedSql, q)
}

func TestWithAsQuery_Update(t *testing.T) {
	t.Parallel()
	w := With("lab").As(
		Select("col", "common_col").From("tab").
			Where("simple").
			Where("NOT hard"),
	).Update(
		Update("upd_tab, lab").
			Set("upd_col", Expr("lab.col")).
			Where("common_col = lab.common_col"))

	q, _, err := w.ToSql()
	require.NoError(t, err)

	expectedSql := "WITH lab AS (" +
		"SELECT col, common_col FROM tab WHERE simple AND NOT hard" +
		") " +
		"UPDATE upd_tab, lab SET upd_col = lab.col WHERE common_col = lab.common_col"

	assert.Equal(t, expectedSql, q)
}

func TestCTEPlaceholderFormat(t *testing.T) {
	t.Parallel()
	q := With("table1").As(
		Select("col1", "col2").
			From("table1").
			Where(Eq{"col1": 1})).
		Update(
			Update("table2").
				Set("col3", 2))

	sql, _, err := q.PlaceholderFormat(Question).ToSql()
	require.NoError(t, err)

	expectedSql := "WITH table1 AS (SELECT col1, col2 FROM table1 WHERE col1 = ?) UPDATE table2 SET col3 = ?"
	assert.Equal(t, expectedSql, sql)

	sql, _, err = q.PlaceholderFormat(Dollar).ToSql()
	require.NoError(t, err)

	expectedSql = "WITH table1 AS (SELECT col1, col2 FROM table1 WHERE col1 = $1) UPDATE table2 SET col3 = $2"
	assert.Equal(t, expectedSql, sql)
}

func TestCTEWithNestedSelects_DollarPlaceholderFormat(t *testing.T) {
	t.Parallel()
	b := StatementBuilder.PlaceholderFormat(Dollar)

	sub := b.Select("col1", "col2").
		From("table1").
		Where("col1 = ?", 1)

	sub = sub.Where("col2 = ?", "123")

	q := b.With("table1").
		As(sub).
		Cte("table2").
		As(
			b.Select("col3", "col4").
				From("table2").
				Where("col3 = ?", "345").
				Where("col4 = ?", 2),
		).
		Select(
			b.Select("col1", "col2", "col3", "col4").
				From("table1").
				Where("col1 = ?", 3).
				Join("table2 ON col3 = col4"),
		)

	sql, args, err := q.ToSql()
	require.NoError(t, err)

	expectedSQL := "" +
		"WITH table1 AS (SELECT col1, col2 FROM table1 WHERE col1 = $1 AND col2 = $2), " +
		"table2 AS (SELECT col3, col4 FROM table2 WHERE col3 = $3 AND col4 = $4) " +
		"SELECT col1, col2, col3, col4 FROM table1 JOIN table2 ON col3 = col4 WHERE col1 = $5"

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, "123", "345", 2, 3}, args)
}

func TestCTEFinalUpdate_DollarPlaceholderNumberingConflict(t *testing.T) {
	t.Parallel()
	b := StatementBuilder.PlaceholderFormat(Dollar)

	q := b.With("w1").
		As(
			b.Select("c").From("t1").Where("a = ?", 1),
		).
		Update(
			b.Update("t2").Set("x", 2).Where("y = ?", 3),
		)

	sql, args, err := q.ToSql()
	require.NoError(t, err)

	expectedSQL := "WITH w1 AS (SELECT c FROM t1 WHERE a = $1) UPDATE t2 SET x = $2 WHERE y = $3"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 2, 3}, args)
}
