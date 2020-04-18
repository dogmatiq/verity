package memorystream_test

import (
	"context"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/eventstream/internal/streamtest"
	. "github.com/dogmatiq/infix/eventstream/memorystream"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Stream", func() {
	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			stream := &Stream{
				App:   in.Application.Identity(),
				Types: in.EventTypes,
			}

			return streamtest.Out{
				Stream: stream,
				Append: func(_ context.Context, parcels ...*parcel.Parcel) {
					stream.Append(parcels...)
				},
			}
		},
		nil,
	)
})

var _ = Describe("type Stream", func() {
	Describe("func Append()", func() {
		It("panics if the message type is not supported", func() {
			p := NewParcel("<id>", MessageA1)

			stream := &Stream{
				App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
				Types: message.NewTypeSet(
					MessageBType,
				),
			}

			Expect(func() {
				stream.Append(p)
			}).To(Panic())
		})
	})
})
