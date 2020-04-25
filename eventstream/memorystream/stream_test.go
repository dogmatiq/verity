package memorystream_test

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/eventstream/internal/streamtest"
	. "github.com/dogmatiq/infix/eventstream/memorystream"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("type Stream", func() {
	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			stream := &Stream{
				App:   in.Application.Identity(),
				Types: in.EventTypes,
			}

			var (
				m    sync.Mutex
				next uint64
			)

			return streamtest.Out{
				Stream: stream,
				Append: func(_ context.Context, parcels ...*parcel.Parcel) {
					m.Lock()
					defer m.Unlock()

					stream.Add(next, parcels)
					next += uint64(len(parcels))
				},
			}
		},
		nil,
	)
})
