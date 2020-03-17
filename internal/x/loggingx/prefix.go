package loggingx

import (
	"fmt"
	"strings"

	"github.com/dogmatiq/dodeca/logging"
)

// WithPrefix returns a logger that adds a prefix to log messages.
func WithPrefix(target logging.Logger, f string, v ...interface{}) logging.Logger {
	prefix := fmt.Sprintf(f, v...)

	return &prefixer{
		target,
		prefix,
		strings.ReplaceAll(prefix, "%", "%%"),
	}
}

type prefixer struct {
	target logging.Logger
	prefix string
	format string
}

func (p *prefixer) Log(fmt string, v ...interface{}) {
	p.target.Log(p.format+fmt, v...)
}

func (p *prefixer) LogString(s string) {
	p.target.LogString(p.prefix + s)
}

func (p *prefixer) Debug(fmt string, v ...interface{}) {
	p.target.Debug(p.format+fmt, v...)
}

func (p *prefixer) DebugString(s string) {
	p.target.DebugString(p.prefix + s)
}

func (p *prefixer) IsDebug() bool {
	return p.target.IsDebug()
}
