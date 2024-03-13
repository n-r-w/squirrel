[![Go Reference](https://pkg.go.dev/badge/github.com/n-r-w/squirrel.svg)](https://pkg.go.dev/github.com/n-r-w/squirrel)
[![Go Coverage](https://github.com/n-r-w/squirrel/wiki/coverage.svg)](https://raw.githack.com/wiki/n-r-w/squirrel/coverage.html)
![CI Status](https://github.com/n-r-w/squirrel/actions/workflows/go.yml/badge.svg)
[![Stability](http://badges.github.io/stability-badges/dist/stable.svg)](http://github.com/badges/stability-badges)
[![Go Report](https://goreportcard.com/badge/github.com/n-r-w/squirrel)](https://goreportcard.com/badge/github.com/n-r-w/squirrel)

# Evolution of [github.com/Masterminds/squirrel](https://github.com/Masterminds/squirrel), which unfortunately has not been updated by the author for a long time

Contains breaking changes and new features (see below).

# Squirrel - fluent SQL generator for Go

```go
import "github.com/n-r-w/squirrel"
```

**Squirrel is not an ORM.**

Squirrel helps you build SQL queries from composable parts:

```go
import sq "github.com/n-r-w/squirrel"

users := sq.Select("*").From("users").Join("emails USING (email_id)")

active := users.Where(sq.Eq{"deleted_at": nil})

sql, args, err := active.ToSql()

sql == "SELECT * FROM users JOIN emails USING (email_id) WHERE deleted_at IS NULL"
```

```go
sql, args, err := sq.
    Insert("users").Columns("name", "age").
    Values("moe", 13).Values("larry", sq.Expr("? + 5", 12)).
    ToSql()

sql == "INSERT INTO users (name,age) VALUES (?,?),(?,? + 5)"
```

Squirrel makes conditional query building a breeze:

```go
if len(q) > 0 {
    users = users.Where("name LIKE ?", fmt.Sprint("%", q, "%"))
}
```

Squirrel loves PostgreSQL:

```go
psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// You use question marks for placeholders...
sql, _, _ := psql.Select("*").From("elephants").Where("name IN (?,?)", "Dumbo", "Verna").ToSql()

/// ...squirrel replaces them using PlaceholderFormat.
sql == "SELECT * FROM elephants WHERE name IN ($1,$2)"
```

You can escape question marks by inserting two question marks:

```sql
SELECT * FROM nodes WHERE meta->'format' ??| array[?,?]
```

will generate with the Dollar Placeholder:

```sql
SELECT * FROM nodes WHERE meta->'format' ?| array[$1,$2]
```

## FAQ

- **How can I build an IN query on composite keys / tuples, e.g. `WHERE (col1, col2) IN ((1,2),(3,4))`?**

    Squirrel does not explicitly support tuples, but you can get the same effect with e.g.:

    ```go
    sq.Or{
      sq.Eq{"col1": 1, "col2": 2},
      sq.Eq{"col1": 3, "col2": 4}}
    ```

    ```sql
    WHERE (col1 = 1 AND col2 = 2) OR (col1 = 3 AND col2 = 4)
    ```

    (which should produce the same query plan as the tuple version)

## Breaking changes in comparison to the original [github.com/Masterminds/squirrel](https://github.com/Masterminds/squirrel)

### Removed all database interaction methods. Only query building functions are left

Squirrel is now a pure SQL query builder. For database interaction, use:

- Sqlizer.ToSql() to get the SQL query and arguments.
- `database/sql`, <https://github.com/jackc/pgx>, etc. for executing queries.
- <https://github.com/georgysavva/scany> for scanning rows into structs.

### Changes in the `Case` method

- To pass an integer value to the `When` and `Else` methods, you need to pass it as an int, not as a string.
- To pass a string value to the `When` and `Else` methods, you don't need to add quotes.

Before:

```go
sq.Case("id").When(1, "2").When(2, "'text'").Else("4")
```

After:

```go
sq.Case("id").When(1, 2).When(2, "text").Else(4)
```

## New features

### Subquery support for `WHERE` clause

```go
Select("id", "name").From("users").Where(Eq{"id": Select("id").From("other_table")}).ToSql()
// SELECT id, name FROM users WHERE id IN (SELECT id1 FROM other_table)
```

### Support for integer values in `CASE THEN/ELSE` clause

```go
Select("id", "name").From("users").Where(Case("id").When(1, 2).When(2, 3).Else(4))
// SELECT id, name FROM users WHERE CASE id WHEN 1 THEN 2 WHEN 2 THEN 3 ELSE 4 END
```

### Support for aggregate functions `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`

```go
sq.Sum(subQuery)
```

### Support for using slice as argument for `Column` function

```go
Column(sq.Expr("id = ANY(?)", []int{1,2,3}))
```

### Support for `IN` and `NOT IN` clause

```go
In("id", []int{1, 2, 3})
NotIn("id", subQuery)
```

### Range function

```go
sq.Range("id", 1, 10) // id BETWEEN 1 AND 10
sq.Range("id", 1, nil) //id >= 1
sq.Range("id", nil, 10) // id <= 10
```

### EqNotEmpty function: ignores empty and zero values in Eq map. Useful for filtering

```go
EqNotEmpty{"id1": 1, "name": nil, id2: 0, "desc": ""} // id1 = 1
```

### OrderByCond function: can be used to avoid hardcoding column names in the code

```go
columns := map[int]string{1: "id", 2: "created"}
orderConds := []OrderCond{{1, Asc}, {2, Desc}, {1, Desc}} // duplicate should be ignored

Select("id").From("users").OrderByCond(columns, orderConds)
// SELECT id FROM users ORDER BY id ASC, created DESC
```

### Search function

The search condition is a WHERE clause with LIKE expressions. All columns will be converted to text. Value can be a string or a number.

```go
Select("id", "name").From("users").Search("John", "name", "email")
// SELECT id, name FROM users WHERE (name::text LIKE ? OR email::text LIKE ?)  
// args = ["%John%", "%John%"]
```

### PaginateByID: adds a LIMIT and start from ID condition to the query. WARNING: The columnID must be included in the ORDER BY clause to avoid unexpected results

```go
Select("id", "name").From("users").PaginateByID(10, 20, "id").OrderBy("id ASC")
// SELECT id, name FROM users WHERE id > ? ORDER BY id ASC LIMIT 10
// args = [20]
```

### PaginateByPage: adds a LIMIT and OFFSET to the query. WARNING: The columnID must be included in the ORDER BY clause to avoid unexpected results

```go
Select("id", "name").From("users").PaginateByPage(10, 3).OrderBy("id ASC")
// SELECT id, name FROM users ORDER BY id ASC LIMIT 10 OFFSET 20
```

### Paginate: allows you to use separated Paginator object to paginate the query

It's useful when you want to use the same Paginator object in different application layers.
In following example, SetIDColumn method is used to specify the column name that will be used to paginate the query. If not set, error will be returned. It's required for combination with Paginate and PaginatorByID methods.

```go
Select("id", "name").From("users").Paginate(PaginatorByID(10, 20)).SetIDColumn("id").OrderBy("id ASC")
// SELECT id, name FROM users WHERE id > ? ORDER BY id ASC LIMIT 10
```

```go
Select("id", "name").From("users").Paginate(PaginatorByPage(10, 3)).OrderBy("id ASC")
// SELECT id, name FROM users ORDER BY id ASC LIMIT 10 OFFSET 20
```

### Alias for Select statement: allows to use table alias in the query for multiple columns and add prefix to the column names if needed

```go
Select().
Alias("u").Columns("id", "name").
From("users u")
Alias("u").GroupBy("id", "name").
Alias("u").OrderBy("id").
// SELECT u.id, u.name FROM users u GROUP BY u.id, u.name ORDER BY u.id

Select().
Alias("u", "pref").Columns("id", "name").
From("users u")
Alias("u", "pref").GroupBy("id", "name").
Alias("u", "pref").OrderBy("id").
// SELECT SELECT u.id AS pref_id, u.name AS pref_name FROM users u GROUP BY u.id AS pref_id, u.name AS pref_name ORDER BY u.id AS pref_id
```

## Miscellaneous

- Added a linter and fixed all warnings.

## License

Squirrel is released under the
[MIT License](http://www.opensource.org/licenses/MIT).
