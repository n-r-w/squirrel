package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteBuilderToSql(t *testing.T) {
	t.Parallel()
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToSql()
	require.NoError(t, err)

	expectedSql := "WITH prefix AS ? " +
		"DELETE FROM a WHERE b = ? ORDER BY c LIMIT 2 OFFSET 3 " +
		"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{0, 1, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderToSqlErr(t *testing.T) {
	t.Parallel()
	_, _, err := Delete("").ToSql()
	assert.Error(t, err)
}

func TestDeleteBuilderMustSql(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestDeleteBuilderMustSql should have panicked!")
		}
	}()
	Delete("").MustSql()
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}
