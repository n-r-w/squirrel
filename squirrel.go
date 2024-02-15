// Package squirrel provides a fluent SQL generator.
//
// See https://github.com/Masterminds/squirrel for examples.
package squirrel

import (
	"bytes"
	"fmt"
	"strings"
)

// Sqlizer is the interface that wraps the ToSql method.
//
// ToSql returns a SQL representation of the Sqlizer, along with a slice of args
// as passed to e.g. database/sql.Exec. It can also return an error.
type Sqlizer interface {
	ToSql() (string, []any, error)
}

// rawSqlizer is expected to do what Sqlizer does, but without finalizing placeholders.
// This is useful for nested queries.
type rawSqlizer interface {
	toSqlRaw() (string, []any, error)
}

// DebugSqlizer calls ToSql on s and shows the approximate SQL to be executed
//
// If ToSql returns an error, the result of this method will look like:
// "[ToSql error: %s]" or "[DebugSqlizer error: %s]"
//
// IMPORTANT: As its name suggests, this function should only be used for
// debugging. While the string result *might* be valid SQL, this function does
// not try very hard to ensure it. Additionally, executing the output of this
// function with any untrusted user input is certainly insecure.
func DebugSqlizer(s Sqlizer) string {
	sql, args, err := s.ToSql()
	if err != nil {
		return fmt.Sprintf("[ToSql error: %s]", err)
	}

	var placeholder string
	downCast, ok := s.(placeholderDebugger)
	if !ok {
		placeholder = "?"
	} else {
		placeholder = downCast.debugPlaceholder()
	}
	// TODO: dedupe this with placeholder.go
	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, placeholder)
		if p == -1 {
			break
		}
		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			if i+1 > len(args) {
				return fmt.Sprintf(
					"[DebugSqlizer error: too many placeholders in %#v for %d args]",
					sql, len(args))
			}
			buf.WriteString(sql[:p])
			fmt.Fprintf(buf, "'%v'", args[i])
			// advance our sql string "cursor" beyond the arg we placed
			sql = sql[p+1:]
			i++
		}
	}
	if i < len(args) {
		return fmt.Sprintf(
			"[DebugSqlizer error: not enough placeholders in %#v for %d args]",
			sql, len(args))
	}
	// "append" any remaning sql that won't need interpolating
	buf.WriteString(sql)
	return buf.String()
}
