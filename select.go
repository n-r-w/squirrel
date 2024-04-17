package squirrel

import (
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/lann/builder"
)

// Direction is used in OrderByDir to specify the direction of the ordering.
type Direction int

const (
	Asc Direction = iota
	Desc
)

type PaginatorType int

const (
	PaginatorTypeUndefined PaginatorType = iota
	PaginatorTypeByPage
	PaginatorTypeByID
)

// Paginator is a helper object to paginate results.
type Paginator struct {
	limit  uint64
	page   uint64
	lastID int64
	pType  PaginatorType
}

// PaginatorByPage creates a new Paginator for pagination by page.
func PaginatorByPage(pageSize, pageNum uint64) Paginator {
	return Paginator{
		limit: pageSize,
		page:  pageNum,
		pType: PaginatorTypeByPage,
	}
}

// PaginatorByID creates a new Paginator for pagination by ID.
func PaginatorByID(limit uint64, lastID int64) Paginator {
	return Paginator{
		limit:  limit,
		lastID: lastID,
		pType:  PaginatorTypeByID,
	}
}

// PageSize returns the page size for PaginatorTypeByPage
func (p Paginator) PageSize() uint64 {
	return p.limit
}

// PageNumber returns the page number for PaginatorTypeByPage
func (p Paginator) PageNumber() uint64 {
	return p.page
}

// Limit returns the limit for PaginatorTypeByID
func (p Paginator) Limit() uint64 {
	return p.limit
}

// LastID returns the last ID for PaginatorTypeByID
func (p Paginator) LastID() int64 {
	return p.lastID
}

// Type returns the type of the paginator.
func (p Paginator) Type() PaginatorType {
	return p.pType
}

// String returns the string representation of the direction.
func (d Direction) String() string {
	if d == Asc {
		return "ASC"
	}
	return "DESC"
}

// OrderCond is used in OrderByDir to specify the condition of the ordering.
type OrderCond struct {
	ColumnID  int
	Direction Direction
}

type selectData struct {
	PlaceholderFormat PlaceholderFormat
	Prefixes          []Sqlizer
	Options           []string
	Columns           []Sqlizer
	From              Sqlizer
	Joins             []Sqlizer
	WhereParts        []Sqlizer
	GroupBys          []string
	HavingParts       []Sqlizer
	OrderByParts      []Sqlizer
	Limit             string
	Offset            string
	Suffixes          []Sqlizer
	Paginator         Paginator
	IDColumn          string // ID column name. Required for pagination by ID.
}

func (d *selectData) ToSql() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toSqlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *selectData) toSqlRaw() (sqlStr string, args []any, err error) {
	if len(d.Columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
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

	_, _ = sql.WriteString("SELECT ")

	if len(d.Options) > 0 {
		_, _ = sql.WriteString(strings.Join(d.Options, " "))
		_, _ = sql.WriteString(" ")
	}

	if len(d.Columns) > 0 {
		args, err = appendToSql(d.Columns, sql, ", ", args)
		if err != nil {
			return "", nil, err
		}
	}

	if d.From != nil {
		_, _ = sql.WriteString(" FROM ")
		args, err = appendToSql([]Sqlizer{d.From}, sql, "", args)
		if err != nil {
			return "", nil, err
		}
	}

	if len(d.Joins) > 0 {
		_, _ = sql.WriteString(" ")
		args, err = appendToSql(d.Joins, sql, " ", args)
		if err != nil {
			return "", nil, err
		}
	}

	whereParts := make([]Sqlizer, len(d.WhereParts))
	copy(whereParts, d.WhereParts)

	if d.Paginator.pType == PaginatorTypeByID {
		if d.IDColumn == "" {
			return "", nil, fmt.Errorf("IDColumn is required for pagination by ID")
		}

		whereParts = append(whereParts, Gt{d.IDColumn: d.Paginator.lastID})
	}

	if len(whereParts) > 0 {
		_, _ = sql.WriteString(" WHERE ")
		args, err = appendToSql(whereParts, sql, " AND ", args)
		if err != nil {
			return "", nil, err
		}
	}

	if len(d.GroupBys) > 0 {
		_, _ = sql.WriteString(" GROUP BY ")
		_, _ = sql.WriteString(strings.Join(d.GroupBys, ", "))
	}

	if len(d.HavingParts) > 0 {
		_, _ = sql.WriteString(" HAVING ")
		args, err = appendToSql(d.HavingParts, sql, " AND ", args)
		if err != nil {
			return "", nil, err
		}
	}

	if len(d.OrderByParts) > 0 {
		_, _ = sql.WriteString(" ORDER BY ")
		args, err = appendToSql(d.OrderByParts, sql, ", ", args)
		if err != nil {
			return "", nil, err
		}
	}

	if len(d.Limit) > 0 {
		if d.Paginator.pType != PaginatorTypeUndefined {
			return "", nil, fmt.Errorf("limit and paginator cannot be used together")
		}

		_, _ = sql.WriteString(" LIMIT ")
		_, _ = sql.WriteString(d.Limit)
	}

	if len(d.Offset) > 0 {
		if d.Paginator.pType != PaginatorTypeUndefined {
			return "", nil, fmt.Errorf("offset and paginator cannot be used together")
		}

		_, _ = sql.WriteString(" OFFSET ")
		_, _ = sql.WriteString(d.Offset)
	}

	if d.Paginator.pType == PaginatorTypeByPage {
		_, _ = sql.WriteString(fmt.Sprintf(" LIMIT %d", d.Paginator.limit))
		if d.Paginator.page > 1 {
			_, _ = sql.WriteString(fmt.Sprintf(" OFFSET %d", d.Paginator.limit*(d.Paginator.page-1)))
		}
	} else if d.Paginator.pType == PaginatorTypeByID {
		_, _ = sql.WriteString(fmt.Sprintf(" LIMIT %d", d.Paginator.limit))
	}

	if len(d.Suffixes) > 0 {
		_, _ = sql.WriteString(" ")

		args, err = appendToSql(d.Suffixes, sql, " ", args)
		if err != nil {
			return "", nil, err
		}
	}

	sqlStr = sql.String()
	return sqlStr, args, nil
}

// Builder

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder builder.Builder

func init() {
	builder.Register(SelectBuilder{}, selectData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b SelectBuilder) PlaceholderFormat(f PlaceholderFormat) SelectBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(SelectBuilder)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b SelectBuilder) ToSql() (string, []any, error) {
	data := builder.GetStruct(b).(selectData)
	return data.ToSql()
}

func (b SelectBuilder) toSqlRaw() (string, []any, error) {
	data := builder.GetStruct(b).(selectData)
	return data.toSqlRaw()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b SelectBuilder) MustSql() (string, []any) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b SelectBuilder) Prefix(sql string, args ...any) SelectBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b SelectBuilder) PrefixExpr(e Sqlizer) SelectBuilder {
	return builder.Append(b, "Prefixes", e).(SelectBuilder)
}

// Distinct adds a DISTINCT clause to the query.
func (b SelectBuilder) Distinct() SelectBuilder {
	return b.Options("DISTINCT")
}

// Options adds select option to the query
func (b SelectBuilder) Options(options ...string) SelectBuilder {
	return builder.Extend(b, "Options", options).(SelectBuilder)
}

// Columns adds result columns to the query.
func (b SelectBuilder) Columns(columns ...string) SelectBuilder {
	parts := make([]any, 0, len(columns))
	for _, str := range columns {
		parts = append(parts, newPart(str))
	}
	return builder.Extend(b, "Columns", parts).(SelectBuilder)
}

// RemoveColumns remove all columns from query.
// Must add a new column with Column or Columns methods, otherwise
// return a error.
func (b SelectBuilder) RemoveColumns() SelectBuilder {
	return builder.Delete(b, "Columns").(SelectBuilder)
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//
//	Column("IF(col IN ("+squirrel.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b SelectBuilder) Column(column any, args ...any) SelectBuilder {
	return builder.Append(b, "Columns", newPart(column, args...)).(SelectBuilder)
}

// From sets the FROM clause of the query.
func (b SelectBuilder) From(from string) SelectBuilder {
	return builder.Set(b, "From", newPart(from)).(SelectBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilder) FromSelect(from SelectBuilder, alias string) SelectBuilder {
	// Prevent misnumbered parameters in nested selects (#183).
	from = from.PlaceholderFormat(Question)
	return builder.Set(b, "From", Alias(from, alias)).(SelectBuilder)
}

// JoinClause adds a join clause to the query.
func (b SelectBuilder) JoinClause(pred any, args ...any) SelectBuilder {
	return builder.Append(b, "Joins", newPart(pred, args...)).(SelectBuilder)
}

// Join adds a JOIN clause to the query.
func (b SelectBuilder) Join(join string, rest ...any) SelectBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b SelectBuilder) LeftJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b SelectBuilder) RightJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// InnerJoin adds a INNER JOIN clause to the query.
func (b SelectBuilder) InnerJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// CrossJoin adds a CROSS JOIN clause to the query.
func (b SelectBuilder) CrossJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("CROSS JOIN "+join, rest...)
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]any OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> IN
// (?,?,...)", with one placeholder for each item in the value. These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b SelectBuilder) Where(pred any, args ...any) SelectBuilder {
	if pred == nil || pred == "" {
		return b
	}
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(SelectBuilder)
}

// GroupBy adds GROUP BY expressions to the query.
func (b SelectBuilder) GroupBy(groupBys ...string) SelectBuilder {
	return builder.Extend(b, "GroupBys", groupBys).(SelectBuilder)
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b SelectBuilder) Having(pred any, rest ...any) SelectBuilder {
	return builder.Append(b, "HavingParts", newWherePart(pred, rest...)).(SelectBuilder)
}

// OrderByClause adds ORDER BY clause to the query.
func (b SelectBuilder) OrderByClause(pred any, args ...any) SelectBuilder {
	return builder.Append(b, "OrderByParts", newPart(pred, args...)).(SelectBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
func (b SelectBuilder) OrderBy(orderBys ...string) SelectBuilder {
	for _, orderBy := range orderBys {
		b = b.OrderByClause(orderBy)
	}

	return b
}

// OrderByCond adds ORDER BY expressions with direction to the query.
// The columns map is used to map OrderCond.ColumnID to the column name.
// Can be used to avoid hardcoding column names in the code.
func (b SelectBuilder) OrderByCond(columns map[int]string, conds []OrderCond) SelectBuilder {
	for i, cond := range conds {
		if pos := slices.IndexFunc(conds[:i], func(c OrderCond) bool {
			return c.ColumnID == cond.ColumnID
		}); pos >= 0 && pos < i {
			continue
		}

		column, ok := columns[cond.ColumnID]
		if !ok {
			panic(fmt.Sprintf("column id %d not found in columns map %v", cond.ColumnID, columns))
		}

		b = b.OrderByClause(fmt.Sprintf("%s %s", column, cond.Direction.String()))
	}

	return b
}

// Search adds a search condition to the query.
// The search condition is a WHERE clause with LIKE expressions. All columns will be converted to text.
// value can be a string or a number.
func (b SelectBuilder) Search(value any, columns ...string) SelectBuilder {
	if len(columns) == 0 {
		return b
	}

	search := Or{}
	for _, column := range columns {
		search = append(search, Like{column + "::text": fmt.Sprintf("%%%v%%", value)})
	}

	return b.Where(search)
}

// PaginateByID adds a LIMIT and start from ID condition to the query.
// WARNING: The columnID must be included in the ORDER BY clause to avoid unexpected results!
func (b SelectBuilder) PaginateByID(limit uint64, startID int64, columnID string) SelectBuilder {
	return b.Limit(limit).Where(Gt{columnID: startID})
}

// PaginateByPage adds a LIMIT and OFFSET condition to the query.
// WARNING: query must be ordered to avoid unexpected results!
func (b SelectBuilder) PaginateByPage(limit uint64, page uint64) SelectBuilder {
	sb := b.Limit(limit)
	if page > 1 {
		sb = sb.Offset(limit * (page - 1))
	}

	return sb
}

// Paginate adds pagination conditions to the query.
func (b SelectBuilder) Paginate(p Paginator) SelectBuilder {
	return builder.Set(b, "Paginator", p).(SelectBuilder)
}

// SetIDColumn sets the column name to be used for pagination by ID.
// Required in special cases when Paginate function combined with PaginatorByID.
func (b SelectBuilder) SetIDColumn(column string) SelectBuilder {
	return builder.Set(b, "IDColumn", column).(SelectBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b SelectBuilder) Limit(limit uint64) SelectBuilder {
	return builder.Set(b, "Limit", fmt.Sprintf("%d", limit)).(SelectBuilder)
}

// RemoveLimit Limit ALL allows to access all records with limit
func (b SelectBuilder) RemoveLimit() SelectBuilder {
	return builder.Delete(b, "Limit").(SelectBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b SelectBuilder) Offset(offset uint64) SelectBuilder {
	return builder.Set(b, "Offset", fmt.Sprintf("%d", offset)).(SelectBuilder)
}

// RemoveOffset removes OFFSET clause.
func (b SelectBuilder) RemoveOffset() SelectBuilder {
	return builder.Delete(b, "Offset").(SelectBuilder)
}

// Suffix adds an expression to the end of the query
func (b SelectBuilder) Suffix(sql string, args ...any) SelectBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b SelectBuilder) SuffixExpr(e Sqlizer) SelectBuilder {
	return builder.Append(b, "Suffixes", e).(SelectBuilder)
}

type alias struct {
	builder SelectBuilder
	table   string
	prefix  []string
}

// Columns sets the columns for the table alias.
func (a alias) Columns(columns ...string) SelectBuilder {
	if len(columns) == 0 {
		return a.builder
	}

	return a.builder.Columns(prepareAliasColumns(a.table, a.prefix, columns...)...)
}

// GroupBy sets the group by for the table alias.
func (a alias) GroupBy(groupBys ...string) SelectBuilder {
	if len(groupBys) == 0 {
		return a.builder
	}

	return a.builder.GroupBy(prepareAliasColumns(a.table, a.prefix, groupBys...)...)
}

// OrderBy sets the order by for the table alias.
func (a alias) OrderBy(orderBys ...string) SelectBuilder {
	if len(orderBys) == 0 {
		return a.builder
	}

	return a.builder.OrderBy(prepareAliasColumns(a.table, a.prefix, orderBys...)...)
}

func prepareAliasColumns(table string, prefix []string, columns ...string) []string {
	columnsPrepared := make([]string, 0, len(columns))

	for _, column := range columns {
		if len(prefix) == 0 {
			if table == "" {
				columnsPrepared = append(columnsPrepared, column)
			} else {
				columnsPrepared = append(columnsPrepared, fmt.Sprintf("%s.%s", table, column))
			}
		} else {
			if table == "" {
				columnsPrepared = append(columnsPrepared, fmt.Sprintf("%s AS %s_%s", column, prefix[0], column))
			} else {
				columnsPrepared = append(columnsPrepared, fmt.Sprintf("%s.%s AS %s_%s", table, column, prefix[0], column))
			}
		}
	}

	return columnsPrepared
}

// Alias creates a new table alias for the select builder.
// Prefix is used to add a prefix to the beginning of the column names. If no prefix, the column name will be used.
// All prefixes except the first will be ignored.
func (b SelectBuilder) Alias(table string, prefix ...string) alias {
	return alias{
		builder: b,
		table:   table,
		prefix:  prefix,
	}
}

// With adds a CTE (Common Table Expression) to the query.
func (b SelectBuilder) With(cteName string, cte SelectBuilder) SelectBuilder {
	return b.PrefixExpr(cte.Prefix(fmt.Sprintf("WITH %s AS (", cteName)).Suffix(")"))
}
