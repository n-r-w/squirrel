package squirrel

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/lann/builder"
)

func init() {
	builder.Register(CaseBuilder{}, caseData{})
}

// sqlizerBuffer is a helper that allows to write many Sqlizers one by one
// without constant checks for errors that may come from Sqlizer.
type sqlizerBuffer struct {
	bytes.Buffer
	args []any
	err  error
}

// WriteSql converts Sqlizer to SQL strings and writes it to buffer.
func (b *sqlizerBuffer) WriteSql(item Sqlizer) {
	if b.err != nil {
		return
	}

	var str string
	var args []any
	str, args, b.err = nestedToSql(item)

	if b.err != nil {
		return
	}

	_, _ = b.WriteString(str)
	_ = b.WriteByte(' ')
	b.args = append(b.args, args...)
}

func (b *sqlizerBuffer) ToSql() (sql string, args []any, err error) {
	return b.String(), b.args, b.err
}

// whenPart is a helper structure to describe SQLs "WHEN ... THEN ..." expression.
type whenPart struct {
	when Sqlizer

	then      Sqlizer
	thenValue any
	nullThen  bool
}

func newWhenPart(when, then any) whenPart {
	wp := whenPart{
		when:      newPart(when),
		then:      nil,
		thenValue: nil,
		nullThen:  false,
	}

	switch t := then.(type) {
	case Sqlizer:
		wp.then = newPart(then)
	default:
		if t == nil {
			wp.nullThen = true
		} else {
			sqlName, err := sqlTypeNameHelper(reflect.TypeOf(then))
			if err != nil {
				wp.thenValue = t
			} else {
				wp.then = newPart(Expr(fmt.Sprintf("CAST(? AS %s)", sqlName), t))
			}
		}
	}

	return wp
}

func sqlTypeNameHelper(t reflect.Type) (string, error) {
	switch t.Kind() { //nolint:exhaustive // only specific kinds are supported for SQL type names
	case reflect.Bool:
		return "boolean", nil
	case reflect.Int64, reflect.Uint64, reflect.Int, reflect.Uint:
		return "bigint", nil
	case reflect.Int32, reflect.Uint32:
		return "integer", nil
	case reflect.Int16, reflect.Uint16, reflect.Int8, reflect.Uint8:
		return "smallint", nil
	case reflect.Float32, reflect.Float64:
		return "double precision", nil
	case reflect.String:
		return "text", nil
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "timestamp with time zone", nil
		}
	case reflect.Slice, reflect.Array:
		sqlType, err := sqlTypeNameHelper(t.Elem())
		if err != nil {
			return "", err
		}
		return sqlType + "[]", nil
	}

	return "", fmt.Errorf("unsupported type %s", t.Name())
}

// caseData holds all the data required to build a CASE SQL construct.
type caseData struct {
	What      Sqlizer
	WhenParts []whenPart

	Else      Sqlizer
	ElseValue any
	ElseNull  bool
}

// ToSql implements Sqlizer.
func (d *caseData) ToSql() (sqlStr string, args []any, err error) {
	if len(d.WhenParts) == 0 {
		return "", nil, errors.New("case expression must contain at lease one WHEN clause")
	}

	sql := sqlizerBuffer{
		Buffer: bytes.Buffer{},
		args:   nil,
		err:    nil,
	}

	_, _ = sql.WriteString("CASE ")
	if d.What != nil {
		sql.WriteSql(d.What)
	}

	for _, p := range d.WhenParts {
		_, _ = sql.WriteString("WHEN ")
		sql.WriteSql(p.when)

		if p.then == nil && p.thenValue == nil && !p.nullThen {
			return "", nil, errors.New("When clause must have Then part")
		}

		_, _ = sql.WriteString("THEN ")

		if p.then != nil {
			sql.WriteSql(p.then)
		} else {
			_, _ = sql.WriteString(Placeholders(1) + " ")
			sql.args = append(sql.args, p.thenValue)
		}
	}

	if d.Else != nil || d.ElseValue != nil || d.ElseNull {
		_, _ = sql.WriteString("ELSE ")
	}

	if d.Else != nil {
		sql.WriteSql(d.Else)
	} else if d.ElseValue != nil || d.ElseNull {
		_, _ = sql.WriteString(Placeholders(1) + " ")
		sql.args = append(sql.args, d.ElseValue)
	}

	_, _ = sql.WriteString("END")

	return sql.ToSql()
}

// CaseBuilder builds SQL CASE construct which could be used as parts of queries.
type CaseBuilder builder.Builder

// ToSql builds the query into a SQL string and bound args.
func (b CaseBuilder) ToSql() (sql string, args []any, err error) {
	data := builder.GetStruct(b).(caseData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b CaseBuilder) MustSql() (sql string, args []any) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// what sets optional value for CASE construct "CASE [value] ...".
func (b CaseBuilder) what(e any) CaseBuilder {
	return builder.Set(b, "What", newPart(e)).(CaseBuilder)
}

// When adds "WHEN ... THEN ..." part to CASE construct.
func (b CaseBuilder) When(when, then any) CaseBuilder {
	// TODO: performance hint: replace slice of WhenPart with just slice of parts
	// where even indices of the slice belong to "when"s and odd indices belong to "then"s
	return builder.Append(b, "WhenParts", newWhenPart(when, then)).(CaseBuilder)
}

// Else What sets optional "ELSE ..." part for CASE construct.
func (b CaseBuilder) Else(e any) CaseBuilder {
	switch e.(type) {
	case Sqlizer:
		return builder.Set(b, "Else", newPart(e)).(CaseBuilder)
	default:
		if e == nil {
			return builder.Set(b, "ElseNull", true).(CaseBuilder)
		}
		return builder.Set(b, "ElseValue", e).(CaseBuilder)
	}
}
