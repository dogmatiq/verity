package persistedstream_test

import (
	"context"

	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/eventstream/internal/streamtest"
	"github.com/dogmatiq/verity/eventstream/memorystream"
	. "github.com/dogmatiq/verity/eventstream/persistedstream"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/provider/memory"
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
				// both the cache and the repository.
				FirstOffset: 1,
			}

			stream := &Stream{
				App:        in.Application.Identity(),
				Types:      in.EventTypes,
				Repository: dataStore,
				Marshaler:  in.Marshaler,
				Cache:      cache,
			}

			return streamtest.Out{
				Stream: stream,
				Append: func(ctx context.Context, parcels ...parcel.Parcel) {
					var batch persistence.Batch
					for _, p := range parcels {
						batch = append(batch, persistence.SaveEvent{
							Envelope: p.Envelope,
						})
					}

					res, err := dataStore.Persist(ctx, batch)
					Expect(err).ShouldNot(HaveOccurred())

					var events []eventstream.Event
					for _, p := range parcels {
						events = append(events, eventstream.Event{
							Offset: res.EventOffsets[p.ID()],
							Parcel: p,
						})
					}

					cache.Add(events)
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
