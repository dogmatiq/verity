package memory_test

import (
	"context"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/fixtures" // can't dot-import due to conflicts
	"github.com/dogmatiq/infix/internal/testing/streamtest"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Stream", func() {
	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			stream := &Stream{
				App:   in.Application.Identity(),
				Types: in.MessageTypes,
			}

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

var _ = Describe("type Stream", func() {
	Describe("func Append()", func() {
		It("panics if the message type is not supported", func() {
			env := fixtures.NewEnvelope("<id>", MessageA1)

			stream := &Stream{
				Types: message.NewTypeSet(
					MessageBType,
				),
			}

			Expect(func() {
				stream.Append(env)
			}).To(Panic())
		})
	})
})
