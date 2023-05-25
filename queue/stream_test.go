package queue_test

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/dogmatiq/verity/eventstream"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
	. "github.com/dogmatiq/verity/queue"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type StreamAdaptor", func() {
	var (
		ctx       context.Context
		dataStore *DataStoreStub
		queue     *Queue
		parcel    parcel.Parcel
		event     eventstream.Event
		adaptor   *StreamAdaptor
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		parcel = NewParcel("<message-0>", MessageE1)
		event = eventstream.Event{
			Offset: 123,
			Parcel: parcel,
		}

		dataStore = NewDataStoreStub()
		DeferCleanup(dataStore.Close)

		queue = &Queue{
			Repository: dataStore,
			Marshaler:  Marshaler,
		}

		adaptor = &StreamAdaptor{
			Queue:            queue,
			OffsetRepository: dataStore,
			Persister:        dataStore,
		}

		q := queue
		go func() {
			defer GinkgoRecover()
			err := q.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		}()
	})

	Describe("func NextOffset()", func() {
		It("loads the next offset from the offset repository", func() {
			_, err := dataStore.Persist(
				ctx,
				persistence.Batch{
					persistence.SaveOffset{
						ApplicationKey: DefaultAppKey,
						CurrentOffset:  0,
						NextOffset:     123,
					},
				},
			)
			Expect(err).ShouldNot(HaveOccurred())

			o, err := adaptor.NextOffset(
				ctx,
				configkit.MustNewIdentity(
					"<app-name>",
					DefaultAppKey,
				),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(o).To(BeNumerically("==", 123))
		})
	})

	Describe("func HandleEvent()", func() {
		It("persists the event to the queue", func() {
			err := adaptor.HandleEvent(
				ctx,
				0,
				event,
			)
			Expect(err).ShouldNot(HaveOccurred())

			messages, err := dataStore.LoadQueueMessages(ctx, 1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(messages).To(HaveLen(1))
			Expect(messages[0].Envelope).To(EqualX(parcel.Envelope))
		})

		It("updates the next offset", func() {
			err := adaptor.HandleEvent(
				ctx,
				0,
				event,
			)
			Expect(err).ShouldNot(HaveOccurred())

			o, err := dataStore.LoadOffset(ctx, DefaultAppKey)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(o).To(BeNumerically("==", 124))
		})

		It("pushes the event into the in-memory queue", func() {
			err := adaptor.HandleEvent(
				ctx,
				0,
				event,
			)
			Expect(err).ShouldNot(HaveOccurred())

			m, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m.Revision).To(BeNumerically("==", 1))
			Expect(m.Parcel).To(EqualX(parcel))
		})

		It("returns an error if the current offset is incorrect", func() {
			err := adaptor.HandleEvent(
				ctx,
				1,
				event,
			)
			Expect(err).To(Equal(
				persistence.ConflictError{
					Cause: persistence.SaveOffset{
						ApplicationKey: DefaultAppKey,
						CurrentOffset:  1,
						NextOffset:     124,
					},
				},
			))
		})
	})
})
