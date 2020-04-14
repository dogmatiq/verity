package eventstream_test

import (
	"context"

	. "github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/internal/streamtest"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type PersistedStream", func() {
	var dataStore persistence.DataStore

	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			provider := &memory.Provider{}

			var err error
			dataStore, err = provider.Open(ctx, in.Application.Identity().Key)
			Expect(err).ShouldNot(HaveOccurred())

			stream := &PersistedStream{
				App:        in.Application.Identity(),
				Types:      in.EventTypes,
				Repository: dataStore.EventStoreRepository(),
				Marshaler:  in.Marshaler,
			}

			return streamtest.Out{
				Stream: stream,
				Append: func(ctx context.Context, parcels ...*parcel.Parcel) {
					tx, err := dataStore.Begin(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer tx.Rollback()

					for _, p := range parcels {
						_, err = tx.SaveEvent(
							ctx,
							p.Envelope,
						)
						Expect(err).ShouldNot(HaveOccurred())
					}

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
