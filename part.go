package squirrel

import (
	"fmt"
	"io"
	"reflect"
)

type part struct {
	pred any
	args []any
}

func newPart(pred any, args ...any) Sqlizer {
	return &part{pred, args}
}

func (p part) ToSql() (sql string, args []any, err error) {
	switch pred := p.pred.(type) {
	case nil:
		// no-op
	case Sqlizer:
		sql, args, err = nestedToSql(pred)
	case string:
		sql = pred
		args = p.args
	default:
		v := reflect.ValueOf(pred)
		switch v.Kind() { // nolint:exhaustive
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			sql = fmt.Sprintf("%d", v.Int())
			args = p.args
		case reflect.Float32, reflect.Float64:
			sql = fmt.Sprintf("%f", v.Float())
			args = p.args
		default:
			err = fmt.Errorf("expected value or Sqlizer, not %T", pred)
		}
	}
	return
}

func nestedToSql(s Sqlizer) (string, []any, error) {
	if raw, ok := s.(rawSqlizer); ok {
		return raw.toSqlRaw()
	} else {
		return s.ToSql()
	}
}

func appendToSql(parts []Sqlizer, w io.Writer, sep string, args []any) ([]any, error) {
	for i, p := range parts {
		partSql, partArgs, err := nestedToSql(p)
		if err != nil {
			return nil, err
		} else if len(partSql) == 0 {
			continue
		}

		if i > 0 {
			_, err = io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}

		_, err = io.WriteString(w, partSql)
		if err != nil {
			return nil, err
		}
		args = append(args, partArgs...)
	}
	return args, nil
}
