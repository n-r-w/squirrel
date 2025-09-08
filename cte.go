package squirrel

import (
	"bytes"
	"fmt"

	"github.com/lann/builder"
)

// Common Table Expressions helper
// e.g.
// WITH cte AS (
// ...
// ), cte_2 AS (
// ...
// )
// SELECT ... FROM cte ... cte_2;

type commonTableExpressionsData struct {
	PlaceholderFormat PlaceholderFormat
	Recursive         bool
	CurrentCteName    string
	Ctes              []Sqlizer
	Statement         Sqlizer
}

func (d *commonTableExpressionsData) toSql() (sqlStr string, args []any, err error) {
	if len(d.Ctes) == 0 {
		err = fmt.Errorf("common table expressions statements must have at least one label and subquery")
		return "", nil, err
	}

	if d.Statement == nil {
		err = fmt.Errorf("common table expressions must one of the following final statement: (select, insert, replace, update, delete)")
		return "", nil, err
	}

	sql := &bytes.Buffer{}

	_, _ = sql.WriteString("WITH ")
	if d.Recursive {
		_, _ = sql.WriteString("RECURSIVE ")
	}

	args, err = appendToSql(d.Ctes, sql, ", ", args)
	if err != nil {
		return "", nil, err
	}

	_, _ = sql.WriteString(" ")
	args, err = appendToSql([]Sqlizer{d.Statement}, sql, "", args)
	if err != nil {
		return "", nil, err
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return sqlStr, args, err
}

func (d *commonTableExpressionsData) ToSql() (sql string, args []any, err error) {
	return d.toSql()
}

// Builder

// CommonTableExpressionsBuilder builds CTE (Common Table Expressions) SQL statements.
type CommonTableExpressionsBuilder builder.Builder

func init() {
	builder.Register(CommonTableExpressionsBuilder{}, commonTableExpressionsData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b CommonTableExpressionsBuilder) PlaceholderFormat(f PlaceholderFormat) CommonTableExpressionsBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(CommonTableExpressionsBuilder)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b CommonTableExpressionsBuilder) ToSql() (string, []any, error) {
	data := builder.GetStruct(b).(commonTableExpressionsData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b CommonTableExpressionsBuilder) MustSql() (string, []any) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

func (b CommonTableExpressionsBuilder) Recursive(recursive bool) CommonTableExpressionsBuilder {
	return builder.Set(b, "Recursive", recursive).(CommonTableExpressionsBuilder)
}

// Cte starts a new cte
func (b CommonTableExpressionsBuilder) Cte(cte string) CommonTableExpressionsBuilder {
	return builder.Set(b, "CurrentCteName", cte).(CommonTableExpressionsBuilder)
}

// As sets the expression for the Cte
func (b CommonTableExpressionsBuilder) As(as SelectBuilder) CommonTableExpressionsBuilder {
	data := builder.GetStruct(b).(commonTableExpressionsData)
	// Prevent misnumbered parameters in nested selects similar to #183.
	as = as.PlaceholderFormat(Question)
	return builder.Append(b, "Ctes", cteExpr{as, data.CurrentCteName}).(CommonTableExpressionsBuilder)
}

// Select finalizes the CommonTableExpressionsBuilder with a SELECT
func (b CommonTableExpressionsBuilder) Select(statement SelectBuilder) CommonTableExpressionsBuilder {
	return builder.Set(b, "Statement", statement).(CommonTableExpressionsBuilder)
}

// Insert finalizes the CommonTableExpressionsBuilder with an INSERT
func (b CommonTableExpressionsBuilder) Insert(statement InsertBuilder) CommonTableExpressionsBuilder {
	return builder.Set(b, "Statement", statement).(CommonTableExpressionsBuilder)
}

// Replace finalizes the CommonTableExpressionsBuilder with a REPLACE
func (b CommonTableExpressionsBuilder) Replace(statement InsertBuilder) CommonTableExpressionsBuilder {
	return b.Insert(statement)
}

// Update finalizes the CommonTableExpressionsBuilder with an UPDATE
func (b CommonTableExpressionsBuilder) Update(statement UpdateBuilder) CommonTableExpressionsBuilder {
	return builder.Set(b, "Statement", statement).(CommonTableExpressionsBuilder)
}

// Delete finalizes the CommonTableExpressionsBuilder with a DELETE
func (b CommonTableExpressionsBuilder) Delete(statement DeleteBuilder) CommonTableExpressionsBuilder {
	return builder.Set(b, "Statement", statement).(CommonTableExpressionsBuilder)
}
