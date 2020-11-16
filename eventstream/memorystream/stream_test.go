package memorystream_test

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/eventstream/internal/streamtest"
	. "github.com/dogmatiq/verity/eventstream/memorystream"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/parcel"
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
				Append: func(_ context.Context, parcels ...parcel.Parcel) {
					stream.Append(parcels...)
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
			// persisted events.
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

		When("all of the events are older than the oldest events already in the stream", func() {
			It("discards the events", func() {
				addEvents(stream, 100, 101)
				addEvents(stream, 98, 99)

				expectEventsToBeTruncated(ctx, stream, 98, 99)
				expectEventsToBeAvailable(ctx, stream, 100, 101)
			})
		})

		When("some of the events are older than the oldest events already in the stream", func() {
			It("discards only the older events", func() {
				addEvents(stream, 99, 101)

				expectEventsToBeAvailable(ctx, stream, 100, 101)
			})
		})

		When("the maximum buffer size is exceeded", func() {
			It("drops old events when the max buffer size is reached", func() {
				stream.BufferSize = 3

				addEvents(stream, 100, 101)
				addEvents(stream, 102, 103)

				expectEventsToBeTruncated(ctx, stream, 100, 100)
				expectEventsToBeAvailable(ctx, stream, 101, 103)
			})

			It("drops the oldest events if a single call exceeds the buffer size", func() {
				stream.BufferSize = 3

				addEvents(stream, 100, 103)
				expectEventsToBeTruncated(ctx, stream, 100, 100)
				expectEventsToBeAvailable(ctx, stream, 101, 103)
			})
		})
	})
})

// addEvents adds events in the range [begin, end] in a single call to s.Add().
func addEvents(
	s *Stream,
	begin, end uint64,
) {
	var events []eventstream.Event

	for o := begin; o <= end; o++ {
		id := fmt.Sprintf("<event-%d>", o)
		events = append(
			events,
			eventstream.Event{
				Offset: o,
				Parcel: NewParcel(
					id,
					MessageE{
						Value: id,
					},
				),
			},
		)
	}

	s.Add(events)
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
