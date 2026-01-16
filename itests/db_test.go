//go:build itest

package itests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/n-r-w/testdock/v2"
	"github.com/stretchr/testify/require"
)

func newTestPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()

	ctx := context.Background()
	pool, _ := testdock.GetPgxPool(t, testdock.DefaultPostgresDSN)
	t.Cleanup(pool.Close)

	return pool, ctx
}

func execSetup(t *testing.T, pool *pgxpool.Pool, ctx context.Context, setupSQL string) {
	t.Helper()

	_, err := pool.Exec(ctx, setupSQL)
	require.NoError(t, err)
}
