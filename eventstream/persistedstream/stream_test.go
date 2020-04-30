package persistedstream_test

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/internal/streamtest"
	"github.com/dogmatiq/infix/eventstream/memorystream"
	. "github.com/dogmatiq/infix/eventstream/persistedstream"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Stream", func() {
	var dataStore persistence.DataStore

	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			provider := &memory.Provider{}

			var err error
			dataStore, err = provider.Open(ctx, in.Application.Identity().Key)
			Expect(err).ShouldNot(HaveOccurred())

			cache := &memorystream.Stream{
				// don't cache the first event from our tests so we force use of
				// both the cache and the event store.
				FirstOffset: 1,
			}

			stream := &Stream{
				App:        in.Application.Identity(),
				Types:      in.EventTypes,
				Repository: dataStore.EventStoreRepository(),
				Marshaler:  in.Marshaler,
				Cache:      cache,
			}

			return streamtest.Out{
				Stream: stream,
				Append: func(ctx context.Context, parcels ...*parcel.Parcel) {
					err := persistence.WithTransaction(
						ctx,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							for _, p := range parcels {
								o, err := tx.SaveEvent(ctx, p.Envelope)
								if err != nil {
									return err
								}

								cache.Add([]*eventstream.Event{
									{
										Offset: o,
										Parcel: p,
									},
								})
							}

							return nil
						},
					)
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
