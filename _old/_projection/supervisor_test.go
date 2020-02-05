package projection_test

import (
	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/projection"
)

var _ eventstream.Observer = (*Supervisor)(nil)
