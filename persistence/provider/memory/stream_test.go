package memory_test

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence/internal/streamtest"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/marshalkit"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("type Stream (standard test suite)", func() {
	streamtest.Declare(
		func(ctx context.Context, _ marshalkit.Marshaler) streamtest.Config {
			stream := &Stream{}

			return streamtest.Config{
				Stream: stream,
				Append: func(_ context.Context, envelopes ...*envelope.Envelope) {
					stream.Append(envelopes...)
				},
			}
		},
		nil,
	)
})
