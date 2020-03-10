package memory_test

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/streamtest"
	. "github.com/dogmatiq/infix/persistence/memory"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("type Stream (standard test suite)", func() {
	var stream *Stream

	streamtest.Declare(
		func(ctx context.Context) persistence.Stream {
			stream = &Stream{}
			return stream
		},
		nil,
		func(ctx context.Context, envelopes ...*envelope.Envelope) {
			stream.Append(envelopes...)
		},
	)
})
