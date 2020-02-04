package eventstream_test

import (
	. "github.com/dogmatiq/infix/eventstream"
)

var _ Publisher = (*Registry)(nil)
