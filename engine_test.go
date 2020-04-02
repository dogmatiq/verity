package infix_test

import (
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/infix"
)

var _ dogma.CommandExecutor = (*Engine)(nil)
