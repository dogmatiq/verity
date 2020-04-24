package eventbuffer_test

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/eventstream/eventbuffer"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type EventBuffer", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		buffer *Buffer
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		buffer = &Buffer{
			// For the purposes of our test, we assume there are already 100
			// events in the store.
			NextOffset: 100,
			Logger:     logging.DebugLogger,
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Add()", func() {
		When("the events are the next expected events", func() {
			It("adds the events to the buffer immediately", func() {
				addEvents(ctx, buffer, 100, 101)

				expectEventToBeBuffered(buffer, 100)
				expectEventToBeBuffered(buffer, 101)
			})
		})

		When("the events are not the next expected events", func() {
			It("add the events to the buffer once the 'missing' events are added", func() {
				addEvents(ctx, buffer, 102, 103)
				addEvents(ctx, buffer, 104, 105)
				addEvents(ctx, buffer, 100, 101)

				expectEventToBeBuffered(buffer, 100)
				expectEventToBeBuffered(buffer, 101)
				expectEventToBeBuffered(buffer, 102)
				expectEventToBeBuffered(buffer, 103)
				expectEventToBeBuffered(buffer, 104)
				expectEventToBeBuffered(buffer, 105)
			})
		})

		When("the events are older than the oldest events already in the buffer", func() {
			It("discards the events", func() {
				addEvents(ctx, buffer, 100, 101)
				addEvents(ctx, buffer, 98, 99)

				expectEventNotToBeBuffered(buffer, 98)
				expectEventNotToBeBuffered(buffer, 99)
			})
		})

		It("drops the oldest events when the buffer capacity is reached", func() {
			buffer.Size = 3

			addEvents(ctx, buffer, 100, 101)
			addEvents(ctx, buffer, 102, 103)

			expectEventNotToBeBuffered(buffer, 100)
			expectEventToBeBuffered(buffer, 101)
			expectEventToBeBuffered(buffer, 102)
			expectEventToBeBuffered(buffer, 103)
		})

		It("drops the oldest events when a single call fills the entire buffer", func() {
			buffer.Size = 3

			addEvents(ctx, buffer, 100, 103)

			expectEventNotToBeBuffered(buffer, 100)
			expectEventToBeBuffered(buffer, 101)
			expectEventToBeBuffered(buffer, 102)
			expectEventToBeBuffered(buffer, 103)
		})
	})

	Describe("func Get()", func() {
		When("the buffer is empty", func() {
			When("the requested offset is before the next expected offset", func() {
				It("returns false", func() {
					_, ok, err := buffer.Get(ctx, 99)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(ok).To(BeFalse())
				})
			})
		})

		When("the buffer is not empty", func() {
			BeforeEach(func() {
				addEvents(ctx, buffer, 100, 102)
			})

			When("the requested offset is before the oldest event in the buffer", func() {
				It("returns false", func() {
					_, ok, err := buffer.Get(ctx, 99)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(ok).To(BeFalse())
				})
			})

			When("the requested offset is before the next expected offset", func() {
				It("returns the parcel from the buffer", func() {
					p, ok, err := buffer.Get(ctx, 101)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(ok).To(BeTrue())
					Expect(p.Message).To(Equal(
						MessageE{Value: "<event-101>"},
					))
				})
			})
		})

		When("the requested offset is equal to the next offset", func() {
			It("blocks until the next event is added", func() {
				go func() {
					defer GinkgoRecover()

					time.Sleep(10 * time.Millisecond)

					// Add out-of-order to ensure Get() doesn't unblock on
					// the "future" events.
					addEvents(ctx, buffer, 103, 104)
					addEvents(ctx, buffer, 100, 102)
				}()

				p, ok, err := buffer.Get(ctx, 100)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(ok).To(BeTrue())
				Expect(p.Message).To(Equal(
					MessageE{Value: "<event-100>"},
				))
			})

			It("returns an error if the deadline is exceeded", func() {
				ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()

				_, _, err := buffer.Get(ctx, 100)
				Expect(err).To(Equal(context.DeadlineExceeded))
			})
		})

		When("the requested offset is after the next expected offset", func() {
			It("returns an error", func() {
				_, _, err := buffer.Get(ctx, 101)
				Expect(err).To(MatchError("requested offset is in the future"))
			})
		})
	})
})

// addEvents adds events in the range [begin, end] in a single call to
// b.Add().
func addEvents(
	ctx context.Context,
	b *Buffer,
	begin, end eventstore.Offset,
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

	b.Add(begin, parcels)
}

// expectEventToBeBuffered asserts that the event with the given offset is in
// the buffer.
func expectEventToBeBuffered(
	b *Buffer,
	o eventstore.Offset,
) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	p, ok, err := b.Get(ctx, o)
	Expect(err).ShouldNot(HaveOccurred())

	Expect(ok).To(
		BeTrue(),
		fmt.Sprintf("expected event at offset %d to be in the buffer", o),
	)

	Expect(p.Message).To(Equal(
		MessageE{
			Value: fmt.Sprintf("<event-%d>", o),
		},
	))
}

// expectEventNotToBeBuffered asserts that the event with the given offset is
// not in the buffer.
func expectEventNotToBeBuffered(
	b *Buffer,
	o eventstore.Offset,
) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, ok, err := b.Get(ctx, o)

	Expect(ok).To(
		BeFalse(),
		fmt.Sprintf("did not expect event at offset %d to be in the buffer", o),
	)

	if err != nil {
		Expect(err).To(Equal(context.Canceled))
	}
}
