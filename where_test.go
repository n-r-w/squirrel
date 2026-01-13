package squirrel

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWherePartsAppendToSql(t *testing.T) {
	t.Parallel()
	parts := []Sqlizer{
		newWherePart("x = ?", 1),
		newWherePart(nil),
		newWherePart(Eq{"y": 2}),
	}
	sql := &bytes.Buffer{}
	args, _ := appendToSql(parts, sql, " AND ", []any{})
	assert.Equal(t, "x = ? AND y = ?", sql.String())
	assert.Equal(t, []any{1, 2}, args)
}

func TestWherePartsAppendToSqlErr(t *testing.T) {
	t.Parallel()
	parts := []Sqlizer{newWherePart(1)}
	_, err := appendToSql(parts, &bytes.Buffer{}, "", []any{})
	assert.Error(t, err)
}

func TestWherePartNil(t *testing.T) {
	t.Parallel()
	sql, _, _ := newWherePart(nil).ToSql()
	assert.Equal(t, "", sql)
}

func TestWherePartErr(t *testing.T) {
	t.Parallel()
	_, _, err := newWherePart(1).ToSql()
	assert.Error(t, err)
}

func TestWherePartString(t *testing.T) {
	t.Parallel()
	sql, args, _ := newWherePart("x = ?", 1).ToSql()
	assert.Equal(t, "x = ?", sql)
	assert.Equal(t, []any{1}, args)
}

func TestWherePartMap(t *testing.T) {
	t.Parallel()
	test := func(pred any) {
		sql, _, _ := newWherePart(pred).ToSql()
		expect := []string{"x = ? AND y = ?", "y = ? AND x = ?"}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]any{"x": 1, "y": 2}
	test(m)
	test(Eq(m))
}
