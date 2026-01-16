//go:build itest

package itests

import (
	"testing"

	sq "github.com/n-r-w/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteBuildersWithReturning(t *testing.T) {
	t.Parallel()

	pool, ctx := newTestPool(t)
	setupSQL := `
CREATE TABLE products (
	id bigserial PRIMARY KEY,
	name text NOT NULL,
	stock integer NOT NULL,
	price double precision NOT NULL,
	active boolean NOT NULL
);
CREATE TABLE sales (
	id bigserial PRIMARY KEY,
	product_id bigint NOT NULL REFERENCES products(id),
	qty integer NOT NULL
);
CREATE TABLE archived_products (
	id bigserial PRIMARY KEY,
	name text NOT NULL,
	stock integer NOT NULL,
	price double precision NOT NULL,
	active boolean NOT NULL
);
`
	execSetup(t, pool, ctx, setupSQL)

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
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sql, args...)
	require.NoError(t, err)

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
	require.NoError(t, err)

	rows, err = pool.Query(ctx, sql, args...)
	require.NoError(t, err)
	t.Cleanup(rows.Close)

	updated := make(map[int64]int)
	for rows.Next() {
		var id int64
		var stock int
		err := rows.Scan(&id, &stock)
		require.NoError(t, err)
		updated[id] = stock
	}
	require.NoError(t, rows.Err())
	assert.Len(t, updated, 2)

	assert.Equal(t, 4, updated[productIDs["Widget"]])
	assert.Equal(t, 2, updated[productIDs["Gadget"]])
	_, found := updated[productIDs["Legacy"]]
	assert.False(t, found)

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
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sql, args...)
	require.NoError(t, err)

	var archivedCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM archived_products").Scan(&archivedCount)
	require.NoError(t, err)
	assert.Equal(t, 1, archivedCount)

	deleteTarget := sq.Select("id").
		From("products").
		Where(sq.Lt{"stock": 5}).
		OrderBy("stock ASC").
		Limit(1)

	deleteQuery := sq.Delete("products").
		Where(sq.In("id", deleteTarget)).
		Suffix("RETURNING id").
		PlaceholderFormat(sq.Dollar)

	sql, args, err = deleteQuery.ToSql()
	require.NoError(t, err)

	var deletedID int64
	err = pool.QueryRow(ctx, sql, args...).Scan(&deletedID)
	require.NoError(t, err)

	assert.Equal(t, productIDs["Legacy"], deletedID)

	var remaining int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM products WHERE id = $1", deletedID).Scan(&remaining)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)

	insertMapped := sq.Insert("products").
		SetMap(map[string]any{
			"name":   "Refill",
			"stock":  7,
			"price":  9.5,
			"active": true,
		}).
		Suffix("RETURNING id, name, stock").
		PlaceholderFormat(sq.Dollar)

	sql, args, err = insertMapped.ToSql()
	require.NoError(t, err)

	var mappedID int64
	var mappedName string
	var mappedStock int
	err = pool.QueryRow(ctx, sql, args...).Scan(&mappedID, &mappedName, &mappedStock)
	require.NoError(t, err)

	assert.NotZero(t, mappedID)
	assert.Equal(t, "Refill", mappedName)
	assert.Equal(t, 7, mappedStock)
}
