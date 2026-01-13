package squirrel

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/lann/builder"
)

type deleteData struct {
	PlaceholderFormat PlaceholderFormat
	Prefixes          []Sqlizer
	From              string
	WhereParts        []Sqlizer
	OrderBys          []string
	Limit             string
	Offset            string
	Suffixes          []Sqlizer
}

func (d *deleteData) toSqlRaw() (sqlStr string, args []any, err error) {
	if d.From == "" {
		err = errors.New("delete statements must specify a From table")
		return "", nil, err
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, err = appendToSql(d.Prefixes, sql, " ", args)
		if err != nil {
			return "", nil, err
		}

		_, _ = sql.WriteString(" ")
	}

	_, _ = sql.WriteString("DELETE FROM ")
	_, _ = sql.WriteString(d.From)

	if len(d.WhereParts) > 0 {
		_, _ = sql.WriteString(" WHERE ")
		args, err = appendToSql(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return "", nil, err
		}
	}

	if len(d.OrderBys) > 0 {
		_, _ = sql.WriteString(" ORDER BY ")
		_, _ = sql.WriteString(strings.Join(d.OrderBys, ", "))
	}

	if d.Limit != "" {
		_, _ = sql.WriteString(" LIMIT ")
		_, _ = sql.WriteString(d.Limit)
	}

	if d.Offset != "" {
		_, _ = sql.WriteString(" OFFSET ")
		_, _ = sql.WriteString(d.Offset)
	}

	if len(d.Suffixes) > 0 {
		_, _ = sql.WriteString(" ")
		args, err = appendToSql(d.Suffixes, sql, " ", args)
		if err != nil {
			return "", nil, err
		}
	}

	return sql.String(), args, nil
}

func (d *deleteData) ToSql() (sqlStr string, args []any, err error) {
	s, a, e := d.toSqlRaw()
	if e != nil {
		return "", nil, e
	}
	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(s)
	return sqlStr, a, err
}

// Builder

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder builder.Builder

func init() {
	builder.Register(DeleteBuilder{}, deleteData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b DeleteBuilder) PlaceholderFormat(f PlaceholderFormat) DeleteBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(DeleteBuilder)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b DeleteBuilder) ToSql() (sql string, args []any, err error) {
	data := builder.GetStruct(b).(deleteData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b DeleteBuilder) MustSql() (sql string, args []any) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b DeleteBuilder) Prefix(sql string, args ...any) DeleteBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query.
func (b DeleteBuilder) PrefixExpr(e Sqlizer) DeleteBuilder {
	return builder.Append(b, "Prefixes", e).(DeleteBuilder)
}

// From sets the table to be deleted from.
func (b DeleteBuilder) From(from string) DeleteBuilder {
	return builder.Set(b, "From", from).(DeleteBuilder)
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b DeleteBuilder) Where(pred any, args ...any) DeleteBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(DeleteBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
func (b DeleteBuilder) OrderBy(orderBys ...string) DeleteBuilder {
	return builder.Extend(b, "OrderBys", orderBys).(DeleteBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b DeleteBuilder) Limit(limit uint64) DeleteBuilder {
	return builder.Set(b, "Limit", strconv.FormatUint(limit, 10)).(DeleteBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b DeleteBuilder) Offset(offset uint64) DeleteBuilder {
	return builder.Set(b, "Offset", strconv.FormatUint(offset, 10)).(DeleteBuilder)
}

// toSqlRaw builds SQL with raw placeholders ("?") without applying PlaceholderFormat.
func (b DeleteBuilder) toSqlRaw() (sql string, args []any, err error) {
	data := builder.GetStruct(b).(deleteData)
	return data.toSqlRaw()
}

// Suffix adds an expression to the end of the query.
func (b DeleteBuilder) Suffix(sql string, args ...any) DeleteBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b DeleteBuilder) SuffixExpr(e Sqlizer) DeleteBuilder {
	return builder.Append(b, "Suffixes", e).(DeleteBuilder)
}
