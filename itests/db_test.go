//go:build itest

package itests

import (
	"context"
	"testing"

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

	rows, err := pool.Query(ctx, sql, args...)
	require.NoError(t, err)
	t.Cleanup(rows.Close)

	var ids []int64
	for rows.Next() {
		var id int64
		err := rows.Scan(&id)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	return ids
}

func queryInt64StringPairs(t *testing.T, pool *pgxpool.Pool, ctx context.Context, q sq.Sqlizer) ([]int64, []string) {
	t.Helper()

	sql, args, err := q.ToSql()
	require.NoError(t, err)

	rows, err := pool.Query(ctx, sql, args...)
	require.NoError(t, err)
	t.Cleanup(rows.Close)

	var ids []int64
	var names []string
	for rows.Next() {
		var id int64
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)

		ids = append(ids, id)
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	return ids, names
}
