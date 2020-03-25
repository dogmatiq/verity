package eventstream_test

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/internal/streamtest"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = XDescribe("type EventStoreStream", func() {
	var dataStore persistence.DataStore

	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			provider := &memory.Provider{}

			var err error
			dataStore, err = provider.Open(ctx, in.Application.Identity().Key)
			Expect(err).ShouldNot(HaveOccurred())

			stream := &EventStoreStream{
				App:        in.Application.Identity(),
				Types:      in.EventTypes,
				Repository: dataStore.EventStoreRepository(),
			}

			return streamtest.Out{
				Stream: stream,
				Append: func(ctx context.Context, envelopes ...*envelope.Envelope) {
					tx, err := dataStore.Begin(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer tx.Rollback()

					// TODO:
					// _, err = tx.SaveEvents(ctx, envelopes...)
					// Expect(err).ShouldNot(HaveOccurred())

					err = tx.Commit(ctx)
					Expect(err).ShouldNot(HaveOccurred())
				},
			}
		},
		func() {
			if dataStore != nil {
				dataStore.Close()
			}
		},
	)
})