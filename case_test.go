package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCaseWithVal(t *testing.T) {
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	qb := Select().
		Column(caseStmt).
		From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE number " +
		"WHEN 1 THEN ? " +
		"WHEN 2 THEN ? " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"one", "two", "big number"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithComplexVal(t *testing.T) {
	caseStmt := Case("? > ?", 10, 5).
		When("true", "T")

	qb := Select().
		Column(Alias(caseStmt, "complexCase")).
		From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT (CASE ? > ? " +
		"WHEN true THEN ? " +
		"END) AS complexCase " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{10, 5, "T"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoVal(t *testing.T) {
	caseStmt := Case().
		When(Eq{"x": 0}, Expr("x is zero")).
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE " +
		"WHEN x = ? THEN x is zero " +
		"WHEN x > ? THEN CONCAT('x is greater than ', ?) " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{0, 1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithExpr(t *testing.T) {
	caseStmt := Case(Expr("x = ?", true)).
		When("1 > 0", Expr("?", "it's true!")).
		When("1 > 0", "test").
		When("1 > 0", 42).
		When("1 > 0", 42.1).
		When("1 > 0", true).
		Else(42)

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE x = ? " +
		"WHEN 1 > 0 THEN ? " +
		"WHEN 1 > 0 THEN ? " +
		"WHEN 1 > 0 THEN ? " +
		"WHEN 1 > 0 THEN ? " +
		"WHEN 1 > 0 THEN ? " +
		"ELSE ? " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{
		true,
		"it's true!",
		"test",
		42,
		42.1,
		true,
		42,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestMultipleCase(t *testing.T) {
	caseStmtNoval := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else(42)
	caseStmtExpr := Case().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().
		Column(Alias(caseStmtNoval, "case_noval")).
		Column(Alias(caseStmtExpr, "case_expr")).
		From("table")

	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT " +
		"(CASE x = ? WHEN true THEN ? ELSE ? END) AS case_noval, " +
		"(CASE WHEN x = ? THEN ? WHEN x > ? THEN CONCAT('x is greater than ', ?) END) AS case_expr " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{
		true, "it's true!",
		42, 0, "x is zero", 1, 2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoWhenClause(t *testing.T) {
	caseStmt := Case("something").
		Else("42")

	qb := Select().Column(caseStmt).From("table")

	_, _, err := qb.ToSql()

	assert.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}

func TestCaseBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCaseBuilderMustSql should have panicked!")
		}
	}()
	Case("").MustSql()
}

func TestCaseNull(t *testing.T) {
	caseStmt := Case().
		When("1", nil).
		Else(nil)

	qb := Select().
		Column(caseStmt).
		From("table")

	sql, args, err := qb.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT CASE " +
		"WHEN 1 THEN ? " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []any{nil, nil}, args)
}
