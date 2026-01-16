//go:build itest

package itests

import (
	"context"
	"testing"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgxpool"
	sq "github.com/n-r-w/squirrel"
	"github.com/n-r-w/testdock/v2"
	"github.com/stretchr/testify/require"
)

func newTestPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()

	ctx := context.Background()
	pool, _ := testdock.GetPgxPool(t, testdock.DefaultPostgresDSN)

	return pool, ctx
}

func execSetup(t *testing.T, pool *pgxpool.Pool, ctx context.Context, setupSQL string) {
	t.Helper()

	_, err := pool.Exec(ctx, setupSQL)
	require.NoError(t, err)
}

func queryInt64s(t *testing.T, pool *pgxpool.Pool, ctx context.Context, q sq.Sqlizer) []int64 {
	t.Helper()

	sql, args, err := q.ToSql()
	require.NoError(t, err)

	var ids []int64
	err = pgxscan.Select(ctx, pool, &ids, sql, args...)
	require.NoError(t, err)

	return ids
}

func queryInt64StringPairs(t *testing.T, pool *pgxpool.Pool, ctx context.Context, q sq.Sqlizer) ([]int64, []string) {
	t.Helper()

	sql, args, err := q.ToSql()
	require.NoError(t, err)

	type idName struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}

	var rows []idName
	err = pgxscan.Select(ctx, pool, &rows, sql, args...)
	require.NoError(t, err)

	ids := make([]int64, 0, len(rows))
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
		names = append(names, row.Name)
	}

	return ids, names
}
