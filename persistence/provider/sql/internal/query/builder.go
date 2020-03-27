package query

import (
	"fmt"
	"strings"
)

// Builder is a primitive SQL query builder.
type Builder struct {
	// Numeric indicates whether question-mark place-holders (?) should be
	// rewritten to numeric place-holders ($1, $2, ...).
	Numeric bool

	// Parameters is the set of parameters to the query.
	Parameters []interface{}

	query strings.Builder
}

// Write appends a query fragment to the query.
func (b *Builder) Write(
	fragment string,
	parameters ...interface{},
) {
	n := len(b.Parameters)

	for {
		pos := strings.IndexByte(fragment, '?')
		if pos == -1 {
			break
		}

		n++

		b.query.WriteString(fragment[:pos])

		if b.Numeric {
			fmt.Fprintf(&b.query, "$%d", n)
		} else {
			b.query.WriteByte('?')
		}

		fragment = fragment[pos+1:]
	}

	b.query.WriteString(fragment)
	b.query.WriteByte(' ')

	expect := n - len(b.Parameters)
	given := len(parameters)

	if given != expect {
		panic(fmt.Sprintf(
			"parameter count mismatch, expected %d, got %d",
			expect,
			given,
		))
	}

	b.Parameters = append(b.Parameters, parameters...)
}

// String returns the final query string.
func (b *Builder) String() string {
	return b.query.String()
}
