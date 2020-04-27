package memorystream_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/eventstream"
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

var _ = Describe("type Stream", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		stream *Stream
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		stream = &Stream{
			App: configkit.MustNewIdentity("<app-key>", "<app-name>"),
			Types: message.NewTypeSet(
				MessageEType,
			),
			// For the purposes of our test, we assume there are already 100
			// events in the store.
			FirstOffset: 100,
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Add()", func() {
		When("the events are the next expected events", func() {
			It("makes the events available immediately", func() {
				addEvents(stream, 100, 101)
				expectEventsToBeAvailable(ctx, stream, 100, 101)
			})
		})

		When("the events are added out of order", func() {
			It("does not skip past the 'missing' events", func() {
				addEvents(stream, 102, 103)

				ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()

				cur, err := stream.Open(ctx, 100, stream.Types)
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				_, err = cur.Next(ctx)
				Expect(err).To(Equal(context.DeadlineExceeded))
			})

			It("reorders the events", func() {
				addEvents(stream, 102, 103)
				addEvents(stream, 104, 105)
				addEvents(stream, 100, 101)
				expectEventsToBeAvailable(ctx, stream, 100, 105)
			})
		})

		When("the events are older than the oldest events already in the stream", func() {
			It("discards the events", func() {
				addEvents(stream, 100, 101)
				addEvents(stream, 98, 99)

				expectEventsToBeTruncated(ctx, stream, 98, 99)
				expectEventsToBeAvailable(ctx, stream, 100, 101)
			})
		})

		It("drops the oldest batch of events when the max buffer size is reached", func() {
			stream.BufferSize = 3

			addEvents(stream, 100, 101)
			addEvents(stream, 102, 103)

			expectEventsToBeTruncated(ctx, stream, 100, 101)
			expectEventsToBeAvailable(ctx, stream, 102, 103)
		})

		It("does not drop the latest batch even if it exceeds the max buffer size", func() {
			stream.BufferSize = 3

			addEvents(stream, 100, 104)
			expectEventsToBeAvailable(ctx, stream, 100, 104)
		})
	})
})

// addEvents adds events in the range [begin, end] in a single call to s.Add().
func addEvents(
	s *Stream,
	begin, end uint64,
) {
	var parcels []*parcel.Parcel

	for o := begin; o <= end; o++ {
		id := fmt.Sprintf("<event-%d>", o)
		parcels = append(
			parcels,
			NewParcel(
				id,
				MessageE{
					Value: id,
				},
			),
		)
	}

	s.Add(begin, parcels)
}

// expectEventsToBeAvailable asserts that the events in the range [begin, end]
// are available in the stream.
func expectEventsToBeAvailable(
	ctx context.Context,
	s *Stream,
	begin, end uint64,
) {
	cur, err := s.Open(ctx, begin, s.Types)
	Expect(err).ShouldNot(HaveOccurred())
	defer cur.Close()

	for o := begin; o < end; o++ {
		ev, err := cur.Next(ctx)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(ev.Parcel.Message).To(
			Equal(
				MessageE{
					Value: fmt.Sprintf("<event-%d>", o),
				},
			),
			fmt.Sprintf("unexpected event at offset %d", o),
		)
	}
}

// expectEventsToBeTruncated asserts that the events in the range [begin, end]
// have been truncated from the stream.
func expectEventsToBeTruncated(
	ctx context.Context,
	s *Stream,
	begin, end uint64,
) {
	for o := begin; o < end; o++ {
		cur, err := s.Open(ctx, o, s.Types)
		Expect(err).ShouldNot(HaveOccurred())
		defer cur.Close()

		_, err = cur.Next(ctx)
		Expect(err).To(
			Equal(eventstream.ErrTruncated),
			fmt.Sprintf("expected event at offset %d to be truncated", o),
		)
	}
}
