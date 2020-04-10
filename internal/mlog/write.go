package mlog

import (
	"io"
	"strings"

	"github.com/dogmatiq/iago/must"
)

// String returns a log line as a string.
func String(
	ids []IconWithLabel,
	icons []Icon,
	text ...string,
) string {
	w := &strings.Builder{}
	mustWrite(w, ids, icons, text)
	return w.String()
}

// Write writes a log line to w.
func Write(
	w io.Writer,
	ids []IconWithLabel,
	icons []Icon,
	text ...string,
) (n int, err error) {
	defer must.Recover(&err)
	n = mustWrite(w, ids, icons, text)
	return
}

func mustWrite(
	w io.Writer,
	ids []IconWithLabel,
	icons []Icon,
	text []string,
) (n int) {
	for _, v := range ids {
		n += must.WriteTo(w, v)
		n += must.Write(w, space2)
	}

	for _, v := range icons {
		n += must.WriteTo(w, v)
		n += must.Write(w, space1)
	}

	i := 0
	for _, v := range text {
		if v == "" {
			continue
		}

		n += must.Write(w, space1)

		if i > 0 {
			n += must.WriteTo(w, SeparatorIcon)
			n += must.Write(w, space1)
		}

		n += must.WriteString(w, v)
		i++
	}

	return
}

var (
	space1 = []byte{' '}
	space2 = []byte{' ', ' '}
)
