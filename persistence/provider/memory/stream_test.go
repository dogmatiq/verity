package memory_test

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/testing/streamtest"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("type Stream (standard test suite)", func() {
	streamtest.Declare(
		func(ctx context.Context, _ streamtest.In) streamtest.Out {
			stream := &Stream{}

			return streamtest.Out{
				Stream: stream,
				Append: func(_ context.Context, envelopes ...*envelope.Envelope) {
					stream.Append(envelopes...)
				},
			}
		},
		nil,
	)
})
