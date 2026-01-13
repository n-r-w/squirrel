package squirrel

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lann/builder"
)

type updateData struct {
	PlaceholderFormat PlaceholderFormat
	Prefixes          []Sqlizer
	Table             string
	SetClauses        []setClause
	From              Sqlizer
	WhereParts        []Sqlizer
	OrderBys          []string
	Limit             string
	Offset            string
	Suffixes          []Sqlizer
}

type setClause struct {
	column string
	value  any
}

func (d *updateData) writePrefixes(sql *bytes.Buffer, args []any) ([]any, error) {
	if len(d.Prefixes) == 0 {
		return args, nil
	}

	args, err := appendToSql(d.Prefixes, sql, " ", args)
	if err != nil {
		return nil, err
	}

	_, _ = sql.WriteString(" ")
	return args, nil
}

func buildSetClauseSQL(sc setClause) (sql string, args []any, err error) {
	vs, ok := sc.value.(Sqlizer)
	if !ok {
		return sc.column + " = ?", []any{sc.value}, nil
	}

	vsql, vargs, err := nestedToSql(vs)
	if err != nil {
		return "", nil, err
	}

	if _, isSelect := vs.(SelectBuilder); isSelect {
		return fmt.Sprintf("%s = (%s)", sc.column, vsql), vargs, nil
	}

	return fmt.Sprintf("%s = %s", sc.column, vsql), vargs, nil
}

func (d *updateData) writeSetClauses(sql *bytes.Buffer, args []any) ([]any, error) {
	_, _ = sql.WriteString(" SET ")

	setSqls := make([]string, len(d.SetClauses))
	for i, sc := range d.SetClauses {
		setSql, setArgs, err := buildSetClauseSQL(sc)
		if err != nil {
			return nil, err
		}
		setSqls[i] = setSql
		args = append(args, setArgs...)
	}

	_, _ = sql.WriteString(strings.Join(setSqls, ", "))
	return args, nil
}

func (d *updateData) writeFromClause(sql *bytes.Buffer, args []any) ([]any, error) {
	if d.From == nil {
		return args, nil
	}

	_, _ = sql.WriteString(" FROM ")
	return appendToSql([]Sqlizer{d.From}, sql, "", args)
}

func (d *updateData) writeWhereClause(sql *bytes.Buffer, args []any) ([]any, error) {
	if len(d.WhereParts) == 0 {
		return args, nil
	}

	_, _ = sql.WriteString(" WHERE ")
	return appendToSql(d.WhereParts, sql, " AND ", args)
}

func (d *updateData) writeOrderByClause(sql *bytes.Buffer) {
	if len(d.OrderBys) > 0 {
		_, _ = sql.WriteString(" ORDER BY ")
		_, _ = sql.WriteString(strings.Join(d.OrderBys, ", "))
	}
}

func (d *updateData) writeLimitOffset(sql *bytes.Buffer) {
	if d.Limit != "" {
		_, _ = sql.WriteString(" LIMIT ")
		_, _ = sql.WriteString(d.Limit)
	}

	if d.Offset != "" {
		_, _ = sql.WriteString(" OFFSET ")
		_, _ = sql.WriteString(d.Offset)
	}
}

func (d *updateData) writeSuffixes(sql *bytes.Buffer, args []any) ([]any, error) {
	if len(d.Suffixes) == 0 {
		return args, nil
	}

	_, _ = sql.WriteString(" ")
	return appendToSql(d.Suffixes, sql, " ", args)
}

func (d *updateData) toSqlRaw() (sqlStr string, args []any, err error) {
	if d.Table == "" {
		return "", nil, errors.New("update statements must specify a table")
	}
	if len(d.SetClauses) == 0 {
		return "", nil, errors.New("update statements must have at least one Set clause")
	}

	sql := &bytes.Buffer{}

	if args, err = d.writePrefixes(sql, args); err != nil {
		return "", nil, err
	}

	_, _ = sql.WriteString("UPDATE ")
	_, _ = sql.WriteString(d.Table)

	if args, err = d.writeSetClauses(sql, args); err != nil {
		return "", nil, err
	}

	if args, err = d.writeFromClause(sql, args); err != nil {
		return "", nil, err
	}

	if args, err = d.writeWhereClause(sql, args); err != nil {
		return "", nil, err
	}

	d.writeOrderByClause(sql)
	d.writeLimitOffset(sql)

	if args, err = d.writeSuffixes(sql, args); err != nil {
		return "", nil, err
	}

	return sql.String(), args, nil
}

func (d *updateData) ToSql() (sqlStr string, args []any, err error) {
	s, a, e := d.toSqlRaw()
	if e != nil {
		return "", nil, e
	}
	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(s)
	return sqlStr, a, err
}

// Builder

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder builder.Builder

func init() { //nolint:gochecknoinits // required to register UpdateBuilder
	builder.Register(UpdateBuilder{}, updateData{}) //nolint:exhaustruct // empty struct is fine
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) UpdateBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(UpdateBuilder)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b UpdateBuilder) ToSql() (sql string, args []any, err error) {
	data := builder.GetStruct(b).(updateData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b UpdateBuilder) MustSql() (sql string, args []any) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b UpdateBuilder) Prefix(sql string, args ...any) UpdateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query.
func (b UpdateBuilder) PrefixExpr(e Sqlizer) UpdateBuilder {
	return builder.Append(b, "Prefixes", e).(UpdateBuilder)
}

// Table sets the table to be updated.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	return builder.Set(b, "Table", table).(UpdateBuilder)
}

// Set adds SET clauses to the query.
func (b UpdateBuilder) Set(column string, value any) UpdateBuilder {
	return builder.Append(b, "SetClauses", setClause{column: column, value: value}).(UpdateBuilder)
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b UpdateBuilder) SetMap(clauses map[string]any) UpdateBuilder {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		val := clauses[key]
		b = b.Set(key, val)
	}
	return b
}

// From adds FROM clause to the query
// FROM is valid construct in postgresql only.
func (b UpdateBuilder) From(from string) UpdateBuilder {
	return builder.Set(b, "From", newPart(from)).(UpdateBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b UpdateBuilder) FromSelect(from SelectBuilder, alias string) UpdateBuilder {
	return builder.Set(b, "From", Alias(from, alias)).(UpdateBuilder)
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b UpdateBuilder) Where(pred any, args ...any) UpdateBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(UpdateBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
func (b UpdateBuilder) OrderBy(orderBys ...string) UpdateBuilder {
	return builder.Extend(b, "OrderBys", orderBys).(UpdateBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b UpdateBuilder) Limit(limit uint64) UpdateBuilder {
	return builder.Set(b, "Limit", strconv.FormatUint(limit, 10)).(UpdateBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b UpdateBuilder) Offset(offset uint64) UpdateBuilder {
	return builder.Set(b, "Offset", strconv.FormatUint(offset, 10)).(UpdateBuilder)
}

// Suffix adds an expression to the end of the query.
func (b UpdateBuilder) Suffix(sql string, args ...any) UpdateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// toSqlRaw builds SQL with raw placeholders ("?") without applying PlaceholderFormat.
func (b UpdateBuilder) toSqlRaw() (sql string, args []any, err error) {
	data := builder.GetStruct(b).(updateData)
	return data.toSqlRaw()
}

// SuffixExpr adds an expression to the end of the query.
func (b UpdateBuilder) SuffixExpr(e Sqlizer) UpdateBuilder {
	return builder.Append(b, "Suffixes", e).(UpdateBuilder)
}
