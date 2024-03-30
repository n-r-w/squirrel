package squirrel

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const (
	// Portable true/false literals.
	sqlTrue  = "(1=1)"
	sqlFalse = "(1=0)"
)

type expr struct {
	sql  string
	args []any
}

// Expr builds an expression from a SQL fragment and arguments.
//
// Ex:
//
//	Expr("FROM_UNIXTIME(?)", t)
func Expr(sql string, args ...any) Sqlizer {
	return expr{sql: sql, args: args}
}

func (e expr) ToSql() (sql string, args []any, err error) {
	simple := true
	for _, arg := range e.args {
		if _, ok := arg.(Sqlizer); ok {
			simple = false
		}
		if isListType(arg) {
			simple = false
		}
	}
	if simple {
		return e.sql, e.args, nil
	}

	buf := &bytes.Buffer{}
	ap := e.args
	sp := e.sql

	var isql string
	var iargs []any

	for err == nil && len(ap) > 0 && len(sp) > 0 {
		i := strings.Index(sp, "?")
		if i < 0 {
			// no more placeholders
			break
		}
		if len(sp) > i+1 && sp[i+1:i+2] == "?" {
			// escaped "??"; append it and step past
			buf.WriteString(sp[:i+2])
			sp = sp[i+2:]
			continue
		}

		if as, ok := ap[0].(Sqlizer); ok {
			// sqlizer argument; expand it and append the result
			isql, iargs, err = as.ToSql()
			buf.WriteString(sp[:i])
			buf.WriteString(isql)
			args = append(args, iargs...)
		} else {
			// normal argument; append it and the placeholder
			buf.WriteString(sp[:i+1])
			args = append(args, ap[0])
		}

		// step past the argument and placeholder
		ap = ap[1:]
		sp = sp[i+1:]
	}

	// append the remaining sql and arguments
	buf.WriteString(sp)
	return buf.String(), append(args, ap...), err
}

type concatExpr []any

func (ce concatExpr) ToSql() (sql string, args []any, err error) {
	for _, part := range ce {
		switch p := part.(type) {
		case string:
			sql += p
		case Sqlizer:
			pSql, pArgs, err := p.ToSql()
			if err != nil {
				return "", nil, err
			}
			sql += pSql
			args = append(args, pArgs...)
		default:
			return "", nil, fmt.Errorf("%#v is not a string or Sqlizer", part)
		}
	}
	return
}

// ConcatExpr builds an expression by concatenating strings and other expressions.
//
// Ex:
//
//	name_expr := Expr("CONCAT(?, ' ', ?)", firstName, lastName)
//	ConcatExpr("COALESCE(full_name,", name_expr, ")")
func ConcatExpr(parts ...any) concatExpr {
	return concatExpr(parts)
}

// aliasExpr helps to alias part of SQL query generated with underlying "expr"
type aliasExpr struct {
	expr  Sqlizer
	alias string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//
//	.Column(Alias(caseStmt, "case_column"))
func Alias(e Sqlizer, a string) aliasExpr {
	return aliasExpr{e, a}
}

func (e aliasExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("(%s) AS %s", sql, e.alias)
	}
	return
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
type Eq map[string]any

func (eq Eq) toSQL(useNotOpr bool) (sql string, args []any, err error) {
	if len(eq) == 0 {
		// Empty Sql{} evaluates to true.
		sql = sqlTrue
		return sql, args, nil
	}

	var (
		exprs       = make([]string, 0, len(eq))
		equalOpr    = "="
		inOpr       = "IN"
		nullOpr     = "IS"
		inEmptyExpr = sqlFalse
	)

	if useNotOpr {
		equalOpr = "<>"
		inOpr = "NOT IN"
		nullOpr = "IS NOT"
		inEmptyExpr = sqlTrue
	}

	sortedKeys := getSortedKeys(eq)
	for _, key := range sortedKeys {
		var expr1 string
		val := eq[key]

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return "", nil, err
			}
		}

		r := reflect.ValueOf(val)
		if r.Kind() == reflect.Ptr {
			if r.IsNil() {
				val = nil
			} else {
				val = r.Elem().Interface()
			}
		}

		if val == nil {
			expr1 = fmt.Sprintf("%s %s NULL", key, nullOpr)
		} else {
			if isListType(val) {
				valVal := reflect.ValueOf(val)
				if valVal.Len() == 0 {
					expr1 = inEmptyExpr
					if args == nil {
						args = []any{}
					}
				} else {
					for i := 0; i < valVal.Len(); i++ {
						args = append(args, valVal.Index(i).Interface())
					}
					expr1 = fmt.Sprintf("%s %s (%s)", key, inOpr, Placeholders(valVal.Len()))
				}
			} else if sb, ok := val.(SelectBuilder); ok {
				var (
					subSql  string
					subArgs []any
				)
				subSql, subArgs, err = sb.toSqlRaw()
				if err != nil {
					return "", nil, err
				}
				expr1 = fmt.Sprintf("%s %s (%s)", key, inOpr, subSql)
				args = append(args, subArgs...)
			} else {
				expr1 = fmt.Sprintf("%s %s ?", key, equalOpr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr1)
	}
	sql = strings.Join(exprs, " AND ")
	return sql, args, nil
}

func (eq Eq) ToSql() (sql string, args []any, err error) {
	return eq.toSQL(false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NotEq{"id": 1}) == "id <> 1"
type NotEq Eq

func (neq NotEq) ToSql() (sql string, args []any, err error) {
	return Eq(neq).toSQL(true)
}

// Like is syntactic sugar for use with LIKE conditions.
// Ex:
//
//	.Where(Like{"name": "%irrel"})
type Like map[string]any

func (lk Like) toSql(opr string) (sql string, args []any, err error) {
	exprs := make([]string, 0, len(lk))
	for key, val := range lk {
		var expr1 string

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with like operators")
			return
		} else {
			if isListType(val) {
				err = fmt.Errorf("cannot use array or slice with like operators")
				return
			} else {
				expr1 = fmt.Sprintf("%s %s ?", key, opr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr1)
	}
	sql = strings.Join(exprs, " AND ")
	return
}

func (lk Like) ToSql() (sql string, args []any, err error) {
	return lk.toSql("LIKE")
}

// NotLike is syntactic sugar for use with LIKE conditions.
// Ex:
//
//	.Where(NotLike{"name": "%irrel"})
type NotLike Like

func (nlk NotLike) ToSql() (sql string, args []any, err error) {
	return Like(nlk).toSql("NOT LIKE")
}

// ILike is syntactic sugar for use with ILIKE conditions.
// Ex:
//
//	.Where(ILike{"name": "sq%"})
type ILike Like

func (ilk ILike) ToSql() (sql string, args []any, err error) {
	return Like(ilk).toSql("ILIKE")
}

// NotILike is syntactic sugar for use with ILIKE conditions.
// Ex:
//
//	.Where(NotILike{"name": "sq%"})
type NotILike Like

func (nilk NotILike) ToSql() (sql string, args []any, err error) {
	return Like(nilk).toSql("NOT ILIKE")
}

// Lt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Lt{"id": 1})
type Lt map[string]any

func (lt Lt) toSql(opposite, orEq bool) (sql string, args []any, err error) {
	var (
		exprs = make([]string, 0, len(lt))
		opr   = "<"
	)

	if opposite {
		opr = ">"
	}

	if orEq {
		opr = fmt.Sprintf("%s%s", opr, "=")
	}

	sortedKeys := getSortedKeys(lt)
	for _, key := range sortedKeys {
		var expr1 string
		val := lt[key]

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return "", nil, err
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with less than or greater than operators")
			return "", nil, err
		}
		if isListType(val) {
			err = fmt.Errorf("cannot use array or slice with less than or greater than operators")
			return "", nil, err
		}
		expr1 = fmt.Sprintf("%s %s ?", key, opr)
		args = append(args, val)

		exprs = append(exprs, expr1)
	}
	sql = strings.Join(exprs, " AND ")
	return sql, args, nil
}

func (lt Lt) ToSql() (sql string, args []any, err error) {
	return lt.toSql(false, false)
}

// LtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LtOrEq{"id": 1}) == "id <= 1"
type LtOrEq Lt

func (ltOrEq LtOrEq) ToSql() (sql string, args []any, err error) {
	return Lt(ltOrEq).toSql(false, true)
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Gt{"id": 1}) == "id > 1"
type Gt Lt

func (gt Gt) ToSql() (sql string, args []any, err error) {
	return Lt(gt).toSql(true, false)
}

// GtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(GtOrEq{"id": 1}) == "id >= 1"
type GtOrEq Lt

func (gtOrEq GtOrEq) ToSql() (sql string, args []any, err error) {
	return Lt(gtOrEq).toSql(true, true)
}

type conj []Sqlizer

func (c conj) join(sep, defaultExpr string) (sql string, args []any, err error) {
	if len(c) == 0 {
		return defaultExpr, []any{}, nil
	}
	var sqlParts []string
	for _, sqlizer := range c {
		partSQL, partArgs, err := nestedToSql(sqlizer)
		if err != nil {
			return "", nil, err
		}
		if partSQL != "" {
			sqlParts = append(sqlParts, partSQL)
			args = append(args, partArgs...)
		}
	}
	if len(sqlParts) > 0 {
		sql = fmt.Sprintf("(%s)", strings.Join(sqlParts, sep))
	}
	return
}

// And conjunction Sqlizers
type And conj

func (a And) ToSql() (string, []any, error) {
	return conj(a).join(" AND ", sqlTrue)
}

// Or conjunction Sqlizers
type Or conj

func (o Or) ToSql() (string, []any, error) {
	return conj(o).join(" OR ", sqlFalse)
}

func getSortedKeys(exp map[string]any) []string {
	sortedKeys := make([]string, 0, len(exp))
	for k := range exp {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	return sortedKeys
}

func isListType(val any) bool {
	if driver.IsValue(val) {
		return false
	}
	valVal := reflect.ValueOf(val)
	return valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice
}

// sumExpr helps to use aggregate function SUM in SQL query
type sumExpr struct {
	expr Sqlizer
}

// Sum allows to use SUM function in SQL query
// Ex: SelectBuilder.Select("id", Sum("amount"))
func Sum(e Sqlizer) sumExpr {
	return sumExpr{e}
}

func (e sumExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("SUM(%s)", sql)
	}
	return
}

// countExpr helps to use aggregate function COUNT in SQL query
type countExpr struct {
	expr Sqlizer
}

// Count allows to use COUNT function in SQL query
// Ex: SelectBuilder.Select("id", Count("amount"))
func Count(e Sqlizer) countExpr {
	return countExpr{e}
}

func (e countExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("COUNT(%s)", sql)
	}
	return
}

// minExpr helps to use aggregate function MIN in SQL query
type minExpr struct {
	expr Sqlizer
}

// Min allows to use MIN function in SQL query
// Ex: SelectBuilder.Select("id", Min("amount"))
func Min(e Sqlizer) minExpr {
	return minExpr{e}
}

func (e minExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("MIN(%s)", sql)
	}
	return
}

// maxExpr helps to use aggregate function MAX in SQL query
type maxExpr struct {
	expr Sqlizer
}

// Max allows to use MAX function in SQL query
// Ex: SelectBuilder.Select("id", Max("amount"))
func Max(e Sqlizer) maxExpr {
	return maxExpr{e}
}

func (e maxExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("MAX(%s)", sql)
	}
	return
}

// avgExpr helps to use aggregate function AVG in SQL query
type avgExpr struct {
	expr Sqlizer
}

// Avg allows to use AVG function in SQL query
// Ex: SelectBuilder.Select("id", Avg("amount"))
func Avg(e Sqlizer) avgExpr {
	return avgExpr{e}
}

func (e avgExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("AVG(%s)", sql)
	}
	return
}

// ExistsExpr helps to use EXISTS in SQL query
type existsExpr struct {
	expr Sqlizer
}

// Exists allows to use EXISTS in SQL query
// Ex: SelectBuilder.Where(Exists(Select("id").From("accounts").Where(Eq{"id": 1})))
func Exists(e Sqlizer) existsExpr {
	return existsExpr{e}
}

func (e existsExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("EXISTS (%s)", sql)
	}
	return
}

// NotExistsExpr helps to use NOT EXISTS in SQL query
type notExistsExpr struct {
	expr Sqlizer
}

// NotExists allows to use NOT EXISTS in SQL query
// Ex: SelectBuilder.Where(NotExists(Select("id").From("accounts").Where(Eq{"id": 1})))
func NotExists(e Sqlizer) notExistsExpr {
	return notExistsExpr{e}
}

func (e notExistsExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("NOT EXISTS (%s)", sql)
	}
	return
}

// InExpr helps to use IN in SQL query
type inExpr struct {
	column string
	expr   any
}

// In allows to use IN in SQL query
// Ex: SelectBuilder.Where(In("id", 1, 2, 3))
func In(column string, e any) inExpr {
	return inExpr{column, e}
}

func (e inExpr) ToSql() (sql string, args []any, err error) {
	switch v := e.expr.(type) {
	case Sqlizer:
		sql, args, err = v.ToSql()
		if err == nil && sql != "" {
			sql = fmt.Sprintf("%s IN (%s)", e.column, sql)
		}
	default:
		if isListType(v) {
			if reflect.ValueOf(v).Len() == 0 {
				return "", nil, nil
			}

			if reflect.ValueOf(v).Len() == 1 {
				args = []any{reflect.ValueOf(v).Index(0).Interface()}
				sql = fmt.Sprintf("%s=?", e.column)
			} else {
				args = []any{v}
				sql = fmt.Sprintf("%s=ANY(?)", e.column)
			}
		} else {
			args = []any{v}
			sql = fmt.Sprintf("%s=?", e.column)
		}
	}

	return sql, args, err
}

// NotInExpr helps to use NOT IN in SQL query
type notInExpr inExpr

// NotIn allows to use NOT IN in SQL query
// Ex: SelectBuilder.Where(NotIn("id", 1, 2, 3))
func NotIn(column string, e any) notInExpr {
	return notInExpr{column, e}
}

func (e notInExpr) ToSql() (sql string, args []any, err error) {
	switch v := e.expr.(type) {
	case Sqlizer:
		sql, args, err = v.ToSql()
		if err == nil && sql != "" {
			sql = fmt.Sprintf("%s NOT IN (%s)", e.column, sql)
		}
	default:
		if isListType(v) {
			if reflect.ValueOf(v).Len() == 0 {
				return "", nil, nil
			}

			if reflect.ValueOf(v).Len() == 1 {
				args = []any{reflect.ValueOf(v).Index(0).Interface()}
				sql = fmt.Sprintf("%s<>?", e.column)
			} else {
				args = []any{v}
				sql = fmt.Sprintf("%s<>ALL(?)", e.column)
			}
		} else {
			args = []any{v}
			sql = fmt.Sprintf("%s<>?", e.column)
		}
	}

	return sql, args, err
}

// rangeExpr helps to use BETWEEN in SQL query
type rangeExpr struct {
	column string
	start  any
	end    any
}

// Range allows to use range in SQL query
// Ex: SelectBuilder.Where(Range("id", 1, 3)) -> "id BETWEEN 1 AND 3"
// If start or end is nil, it will be omitted from the query.
// Ex: SelectBuilder.Where(Range("id", 1, nil)) -> "id >= 1"
// Ex: SelectBuilder.Where(Range("id", nil, 3)) -> "id <= 3"
func Range(column string, start, end any) rangeExpr {
	return rangeExpr{column, start, end}
}

// ToSql builds the query into a SQL string and bound args.
func (e rangeExpr) ToSql() (sql string, args []any, err error) {
	hasStart := e.start != nil && !reflect.ValueOf(e.start).IsZero()
	hasEnd := e.end != nil && !reflect.ValueOf(e.end).IsZero()

	if !hasStart && !hasEnd {
		return "", nil, nil
	}

	var s Sqlizer
	if hasStart && hasEnd {
		s = Expr(fmt.Sprintf("%s BETWEEN ? AND ?", e.column), e.start, e.end)
	} else if hasStart {
		s = GtOrEq{e.column: e.start}
	} else {
		s = LtOrEq{e.column: e.end}
	}

	return s.ToSql()
}

// EqNotEmpty ignores empty and zero values in Eq map.
// Ex: EqNotEmpty{"id1": 1, "name": nil, id2: 0, "desc": ""} -> "id1 = 1".
type EqNotEmpty map[string]any

// ToSql builds the query into a SQL string and bound args.
func (eq EqNotEmpty) ToSql() (sql string, args []any, err error) {
	vals := make(Eq, len(eq))
	for k, v := range eq {
		v = clearEmptyValue(v)
		if v != nil {
			vals[k] = v
		}
	}

	return vals.ToSql()
}

// clearEmptyValue recursively clears empty and zero values in any type.
func clearEmptyValue(v any) any {
	if v == nil {
		return nil
	}

	t := reflect.ValueOf(v)
	switch t.Kind() { //nolint:exhaustive
	case reflect.Array, reflect.Slice:
		if t.Len() != 0 {
			newSlice := reflect.MakeSlice(t.Type(), 0, t.Len())
			for i := 0; i < t.Len(); i++ {
				itemVal := clearEmptyValue(t.Index(i).Interface())
				if itemVal != nil {
					newSlice = reflect.Append(newSlice, t.Index(i))
				}
			}

			if newSlice.Len() != 0 {
				return newSlice.Interface()
			}
		}

	default:
		if !t.IsZero() {
			return v
		}
	}

	return nil
}

type cteExpr struct {
	expr Sqlizer
	cte  string
}

// Cte allows to define CTE (Common Table Expressions) in SQL query
func Cte(e Sqlizer, cte string) cteExpr {
	return cteExpr{e, cte}
}

// ToSql builds the query into a SQL string and bound args.
func (e cteExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("%s AS (%s)", e.cte, sql)
	}
	return
}

type notExpr struct {
	expr Sqlizer
}

// ToSql builds the query into a SQL string and bound args.
func (e notExpr) ToSql() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("NOT (%s)", sql)
	}
	return
}

// Not is a helper function to negate a condition.
func Not(e Sqlizer) Sqlizer {
	// check nested NOT
	if n, ok := e.(notExpr); ok {
		return n.expr
	}

	return notExpr{e}
}
