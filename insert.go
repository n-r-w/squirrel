package squirrel

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/lann/builder"
)

type insertData struct {
	PlaceholderFormat PlaceholderFormat
	Prefixes          []Sqlizer
	StatementKeyword  string
	Options           []string
	Into              string
	Columns           []string
	Values            [][]any
	Suffixes          []Sqlizer
	Select            *SelectBuilder
}

func (d *insertData) toSqlRaw() (sqlStr string, args []any, err error) {
	if d.Into == "" {
		err = errors.New("insert statements must specify a table")
		return "", nil, err
	}
	if len(d.Values) == 0 && d.Select == nil {
		err = errors.New("insert statements must have at least one set of values or select clause")
		return "", nil, err
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, err = appendToSql(d.Prefixes, sql, " ", args)
		if err != nil {
			return "", nil, err
		}

		sql.WriteString(" ")
	}

	if d.StatementKeyword == "" {
		_, _ = sql.WriteString("INSERT ")
	} else {
		_, _ = sql.WriteString(d.StatementKeyword)
		_, _ = sql.WriteString(" ")
	}

	if len(d.Options) > 0 {
		_, _ = sql.WriteString(strings.Join(d.Options, " "))
		_, _ = sql.WriteString(" ")
	}

	_, _ = sql.WriteString("INTO ")
	_, _ = sql.WriteString(d.Into)
	_, _ = sql.WriteString(" ")

	if len(d.Columns) > 0 {
		_, _ = sql.WriteString("(")
		_, _ = sql.WriteString(strings.Join(d.Columns, ","))
		_, _ = sql.WriteString(") ")
	}

	if d.Select != nil {
		args, err = d.appendSelectToSQL(sql, args)
	} else {
		args, err = d.appendValuesToSQL(sql, args)
	}
	if err != nil {
		return "", nil, err
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(d.Suffixes, sql, " ", args)
		if err != nil {
			return "", nil, err
		}
	}

	return sql.String(), args, nil
}

func (d *insertData) ToSql() (sqlStr string, args []any, err error) {
	s, a, e := d.toSqlRaw()
	if e != nil {
		return "", nil, e
	}
	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(s)
	return sqlStr, a, err
}

func (d *insertData) appendValuesToSQL(w io.Writer, args []any) ([]any, error) {
	if len(d.Values) == 0 {
		return args, errors.New("values for insert statements are not set")
	}

	_, _ = io.WriteString(w, "VALUES ")

	valuesStrings := make([]string, len(d.Values))
	for r, row := range d.Values {
		valueStrings := make([]string, len(row))
		for v, val := range row {
			if vs, ok := val.(Sqlizer); ok {
				vsql, vargs, err := nestedToSql(vs)
				if err != nil {
					return nil, err
				}
				valueStrings[v] = vsql
				args = append(args, vargs...)
			} else {
				valueStrings[v] = "?"
				args = append(args, val)
			}
		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}

	_, _ = io.WriteString(w, strings.Join(valuesStrings, ","))

	return args, nil
}

func (d *insertData) appendSelectToSQL(w io.Writer, args []any) ([]any, error) {
	if d.Select == nil {
		return args, errors.New("select clause for insert statements are not set")
	}

	selectClause, sArgs, err := d.Select.toSqlRaw()
	if err != nil {
		return args, err
	}

	_, _ = io.WriteString(w, selectClause)
	args = append(args, sArgs...)

	return args, nil
}

// Builder

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder builder.Builder

func init() {
	builder.Register(InsertBuilder{}, insertData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b InsertBuilder) PlaceholderFormat(f PlaceholderFormat) InsertBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(InsertBuilder)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b InsertBuilder) ToSql() (sql string, args []any, err error) {
	data := builder.GetStruct(b).(insertData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b InsertBuilder) MustSql() (sql string, args []any) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b InsertBuilder) Prefix(sql string, args ...any) InsertBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query.
func (b InsertBuilder) PrefixExpr(e Sqlizer) InsertBuilder {
	return builder.Append(b, "Prefixes", e).(InsertBuilder)
}

// Options adds keyword options before the INTO clause of the query.
func (b InsertBuilder) Options(options ...string) InsertBuilder {
	return builder.Extend(b, "Options", options).(InsertBuilder)
}

// Into sets the INTO clause of the query.
func (b InsertBuilder) Into(from string) InsertBuilder {
	return builder.Set(b, "Into", from).(InsertBuilder)
}

// Columns adds insert columns to the query.
func (b InsertBuilder) Columns(columns ...string) InsertBuilder {
	return builder.Extend(b, "Columns", columns).(InsertBuilder)
}

// Values adds a single row's values to the query.
func (b InsertBuilder) Values(values ...any) InsertBuilder {
	return builder.Append(b, "Values", values).(InsertBuilder)
}

// Suffix adds an expression to the end of the query.
func (b InsertBuilder) Suffix(sql string, args ...any) InsertBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b InsertBuilder) SuffixExpr(e Sqlizer) InsertBuilder {
	return builder.Append(b, "Suffixes", e).(InsertBuilder)
}

// SetMap set columns and values for insert builder from a map of column name and value.
// Note that it will reset all previous columns and values was set if any.
func (b InsertBuilder) SetMap(clauses map[string]any) InsertBuilder {
	// Keep the columns in a consistent order by sorting the column key string.

	cols := make([]string, 0, len(clauses))
	for col := range clauses {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	vals := make([]any, 0, len(clauses))
	for _, col := range cols {
		vals = append(vals, clauses[col])
	}

	b = builder.Set(b, "Columns", cols).(InsertBuilder)
	b = builder.Set(b, "Values", [][]any{vals}).(InsertBuilder)

	return b
}

// Select set Select clause for insert query.
// If Values and Select are used, then Select has higher priority.
func (b InsertBuilder) Select(sb SelectBuilder) InsertBuilder {
	return builder.Set(b, "Select", &sb).(InsertBuilder)
}

func (b InsertBuilder) statementKeyword(keyword string) InsertBuilder {
	return builder.Set(b, "StatementKeyword", keyword).(InsertBuilder)
}

// toSqlRaw builds SQL with raw placeholders ("?") without applying PlaceholderFormat.
func (b InsertBuilder) toSqlRaw() (sql string, args []any, err error) {
	data := builder.GetStruct(b).(insertData)
	return data.toSqlRaw()
}
