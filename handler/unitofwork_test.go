package handler_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/infix/queue"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Resolver", func() {
	var (
		dataStore     *DataStoreStub
		command       *parcel.Parcel
		timeout       *parcel.Parcel
		unqueuedEvent *parcel.Parcel
		queuedEvent   *parcel.Parcel
		oper          persistence.Operation
		resolver      *Resolver
	)

	BeforeEach(func() {
		dataStore = NewDataStoreStub()

		command = NewParcel("<command>", MessageC1)
		timeout = NewParcel(
			"<timeout>",
			MessageT1,
			time.Now(),
			time.Now().Add(1*time.Hour),
		)
		unqueuedEvent = NewParcel("<unqueued-event>", MessageU1)
		queuedEvent = NewParcel("<queued-event>", MessageQ1)

		oper = persistence.SaveOffset{
			ApplicationKey: "<app-key>",
		}

		resolver = &Resolver{
			QueueEvents: message.TypesOf(MessageQ{}),
			UnitOfWork: &UnitOfWork{
				Commands: []*parcel.Parcel{command},
				Timeouts: []*parcel.Parcel{timeout},
				Events:   []*parcel.Parcel{unqueuedEvent, queuedEvent},
				Batch:    persistence.Batch{oper},
			},
		}
	})

	AfterEach(func() {
		dataStore.Close()
	})

	Describe("func ResolveBatch()", func() {
		It("contains the expected operations", func() {
			batch := resolver.ResolveBatch()

			Expect(batch).To(EqualX(
				persistence.Batch{
					// commands ...
					persistence.SaveQueueItem{
						Item: queuestore.Item{
							NextAttemptAt: command.CreatedAt,
							Envelope:      command.Envelope,
						},
					},
					// timeouts ...
					persistence.SaveQueueItem{
						Item: queuestore.Item{
							NextAttemptAt: timeout.ScheduledFor,
							Envelope:      timeout.Envelope,
						},
					},
					// events ...
					persistence.SaveEvent{
						Envelope: unqueuedEvent.Envelope,
					},
					persistence.SaveQueueItem{
						Item: queuestore.Item{
							NextAttemptAt: queuedEvent.CreatedAt,
							Envelope:      queuedEvent.Envelope,
						},
					},
					persistence.SaveEvent{
						Envelope: queuedEvent.Envelope,
					},
					// other ...
					oper,
				},
			))
		})
	})

	Describe("func ResolveResult()", func() {
		It("returns the expected result", func() {
			batch := resolver.ResolveBatch()

			pr, err := dataStore.Persist(context.Background(), batch)
			Expect(err).ShouldNot(HaveOccurred())

			res := resolver.ResolveResult(pr)
			Expect(res).To(EqualX(
				Result{
					Queued: []queue.Message{
						{
							Parcel: command,
							Item: &queuestore.Item{
								Revision:      1,
								NextAttemptAt: command.CreatedAt,
								Envelope:      command.Envelope,
							},
						},
						{
							Parcel: timeout,
							Item: &queuestore.Item{
								Revision:      1,
								NextAttemptAt: timeout.ScheduledFor,
								Envelope:      timeout.Envelope,
							},
						},
						{
							Parcel: queuedEvent,
							Item: &queuestore.Item{
								Revision:      1,
								NextAttemptAt: queuedEvent.CreatedAt,
								Envelope:      queuedEvent.Envelope,
							},
						},
					},
					Events: []eventstream.Event{
						{
							Offset: 0,
							Parcel: unqueuedEvent,
						},
						{
							Offset: 1,
							Parcel: queuedEvent,
						},
					},
				},
			))
		})
	})
})

var _ = Describe("func NotifyObservers()", func() {
	It("calls each of the observers in the unit-of-work", func() {
		count := 0

		res := Result{
			Events: []eventstream.Event{
				{
					Offset: 123,
				},
			},
		}

		fn := func(r Result, err error) {
			count++
			Expect(r).To(Equal(res))
			Expect(err).To(MatchError("<error>"))
		}

		work := &UnitOfWork{}
		work.Observers = append(work.Observers, fn)
		work.Observers = append(work.Observers, fn)

		NotifyObservers(
			work,
			res,
			errors.New("<error>"),
		)
	})
})
