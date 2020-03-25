package eventstream_test

import (
	"context"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/internal/streamtest"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type MemoryStream", func() {
	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			stream := &MemoryStream{
				App:   in.Application.Identity(),
				Types: in.EventTypes,
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

var _ = Describe("type MemoryStream", func() {
	Describe("func Append()", func() {
		It("panics if the message type is not supported", func() {
			env := NewEnvelope("<id>", MessageA1)

			stream := &MemoryStream{
				App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
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
