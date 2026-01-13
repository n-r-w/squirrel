package squirrel

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCaseWithVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	qb := Select().
		Column(caseStmt).
		From("table")
	sql, args, err := qb.ToSql()

	require.NoError(t, err)

	expectedSql := "SELECT CASE number " +
		"WHEN 1 THEN CAST(? AS text) " +
		"WHEN 2 THEN CAST(? AS text) " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"one", "two", "big number"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithComplexVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case("? > ?", 10, 5).
		When("true", "T")

	qb := Select().
		Column(Alias(caseStmt, "complexCase")).
		From("table")
	sql, args, err := qb.ToSql()

	require.NoError(t, err)

	expectedSql := "SELECT (CASE ? > ? " +
		"WHEN true THEN CAST(? AS text) " +
		"END) AS complexCase " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{10, 5, "T"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case().
		When(Eq{"x": 0}, Expr("x is zero")).
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	require.NoError(t, err)

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
	t.Parallel()
	caseStmt := Case(Expr("x = ?", true)).
		When("1 > 0", Expr("?::text", "it's true!")).
		When("1 > 0", "test").
		When("1 > 0", 42).
		When("1 > 0", 42.1).
		When("1 > 0", true).
		Else(42)

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	require.NoError(t, err)

	expectedSql := "SELECT CASE x = ? " +
		"WHEN 1 > 0 THEN ?::text " +
		"WHEN 1 > 0 THEN CAST(? AS text) " +
		"WHEN 1 > 0 THEN CAST(? AS bigint) " +
		"WHEN 1 > 0 THEN CAST(? AS double precision) " +
		"WHEN 1 > 0 THEN CAST(? AS boolean) " +
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
	t.Parallel()
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

	require.NoError(t, err)

	expectedSql := "SELECT " +
		"(CASE x = ? WHEN true THEN ? ELSE ? END) AS case_noval, " +
		"(CASE WHEN x = ? THEN CAST(? AS text) WHEN x > ? THEN CONCAT('x is greater than ', ?) END) AS case_expr " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{
		true, "it's true!",
		42, 0, "x is zero", 1, 2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoWhenClause(t *testing.T) {
	t.Parallel()
	caseStmt := Case("something").
		Else("42")

	qb := Select().Column(caseStmt).From("table")

	_, _, err := qb.ToSql()

	require.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}

func TestCaseBuilderMustSql(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCaseBuilderMustSql should have panicked!")
		}
	}()
	Case("").MustSql()
}

func TestCaseNull(t *testing.T) {
	t.Parallel()
	caseStmt := Case().
		When("1", nil).
		Else(nil)

	qb := Select().
		Column(caseStmt).
		From("table")

	sql, args, err := qb.ToSql()
	require.NoError(t, err)

	expectedSql := "SELECT CASE " +
		"WHEN 1 THEN ? " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []any{nil, nil}, args)
}

func TestSqlTypeNameHelper(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		arg     reflect.Type
		want    string
		wantErr bool
	}{
		{"Bool", reflect.TypeOf(true), "boolean", false},
		{"Int64", reflect.TypeOf(int64(1)), "bigint", false},
		{"Uint64", reflect.TypeOf(uint64(1)), "bigint", false},
		{"Int", reflect.TypeOf(int(1)), "bigint", false},
		{"Uint", reflect.TypeOf(uint(1)), "bigint", false},
		{"Int32", reflect.TypeOf(int32(1)), "integer", false},
		{"Uint32", reflect.TypeOf(uint32(1)), "integer", false},
		{"Int16", reflect.TypeOf(int16(1)), "smallint", false},
		{"Uint16", reflect.TypeOf(uint16(1)), "smallint", false},
		{"Int8", reflect.TypeOf(int8(1)), "smallint", false},
		{"Uint8", reflect.TypeOf(uint8(1)), "smallint", false},
		{"Float32", reflect.TypeOf(float32(1.0)), "double precision", false},
		{"Float64", reflect.TypeOf(float64(1.0)), "double precision", false},
		{"String", reflect.TypeOf(string("test")), "text", false},
		{"Time", reflect.TypeOf(time.Time{}), "timestamp with time zone", false},
		{"Slice", reflect.TypeOf([]int{1, 2, 3}), "bigint[]", false},
		{"Unsupported", reflect.TypeOf(struct{}{}), "", true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t1 *testing.T) {
			t1.Parallel()
			got, err := sqlTypeNameHelper(tt.arg)
			if (err != nil) != tt.wantErr {
				t1.Errorf("sqlTypeNameHelper() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t1.Errorf("sqlTypeNameHelper() = %v, want %v", got, tt.want)
			}
		})
	}
}
