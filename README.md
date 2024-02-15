# Fork of github.com/Masterminds/squirrel

Changes:

- Added a linter and fixed all warnings.
- Removed all database interaction methods. Only query building functions are left.
- Add subquery support for WHERE clause (e.g. `sq.Eq{"id": sq.Select("id").From("other_table")}`).

# Squirrel - fluent SQL generator for Go

```go
import "github.com/n-r-w/squirrel"
```

**Squirrel is not an ORM.** For an application of Squirrel, check out
[structable, a table-struct mapper](https://github.com/Masterminds/structable)

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

- **How can I build an IN query on composite keys / tuples, e.g. `WHERE (col1, col2) IN ((1,2),(3,4))`? ([#104](https://github.com/n-r-w/squirrel/issues/104))**

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

- **Why doesn't `Eq{"mynumber": []uint8{1,2,3}}` turn into an `IN` query? ([#114](https://github.com/n-r-w/squirrel/issues/114))**

    Values of type `[]byte` are handled specially by `database/sql`. In Go, [`byte` is just an alias of `uint8`](https://golang.org/pkg/builtin/#byte), so there is no way to distinguish `[]uint8` from `[]byte`.

- **Some features are poorly documented!**

    This isn't a frequent complaints section!

- **Some features are poorly documented?**

    Yes. The tests should be considered a part of the documentation; take a look at those for ideas on how to express more complex queries.

## License

Squirrel is released under the
[MIT License](http://www.opensource.org/licenses/MIT).
