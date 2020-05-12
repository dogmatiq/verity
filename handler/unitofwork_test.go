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

var _ = Describe("func Persist()", func() {
	var (
		dataStore *DataStoreStub
		work      *UnitOfWork
	)

	BeforeEach(func() {
		dataStore = NewDataStoreStub()

		work = &UnitOfWork{}
	})

	AfterEach(func() {
		dataStore.Close()
	})

	When("the unit-of-work is persisted successfully", func() {
		var (
			result Result
			batch  persistence.Batch
		)

		BeforeEach(func() {
			work.Observe(func(r Result, err error) {
				Expect(err).ShouldNot(HaveOccurred())
				result = r
			})

			dataStore.PersistFunc = func(
				ctx context.Context,
				b persistence.Batch,
			) (persistence.Result, error) {
				batch = b
				return dataStore.DataStore.Persist(ctx, b)
			}
		})

		JustBeforeEach(func() {
			err := Persist(context.Background(), dataStore, work)
			Expect(err).ShouldNot(HaveOccurred())
		})

		When("the unit-of-work contains a command", func() {
			var command *parcel.Parcel

			BeforeEach(func() {
				command = NewParcel(
					"<command>",
					MessageC1,
				)

				work.ExecuteCommand(command)
			})

			It("includes the command in the result", func() {
				Expect(result.Queued).To(EqualX(
					[]queue.Message{
						{
							Parcel: command,
							Item: &queuestore.Item{
								Revision:      1,
								NextAttemptAt: command.CreatedAt,
								Envelope:      command.Envelope,
							},
						},
					},
				))
			})

			It("includes the command in the batch", func() {
				Expect(batch).To(EqualX(
					persistence.Batch{
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: command.CreatedAt,
								Envelope:      command.Envelope,
							},
						},
					},
				))
			})
		})

		When("the unit-of-work contains a timeout", func() {
			var timeout *parcel.Parcel

			BeforeEach(func() {
				timeout = NewParcel(
					"<timeout>",
					MessageT1,
					time.Now(),
					time.Now().Add(1*time.Hour),
				)

				work.ScheduleTimeout(timeout)
			})

			It("includes the timeout in the result", func() {
				Expect(result.Queued).To(EqualX(
					[]queue.Message{
						{
							Parcel: timeout,
							Item: &queuestore.Item{
								Revision:      1,
								NextAttemptAt: timeout.ScheduledFor,
								Envelope:      timeout.Envelope,
							},
						},
					},
				))
			})

			It("includes the timeout in the batch", func() {
				Expect(batch).To(EqualX(
					persistence.Batch{
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: timeout.ScheduledFor,
								Envelope:      timeout.Envelope,
							},
						},
					},
				))
			})
		})

		When("the unit-of-work contains an event", func() {
			var (
				unqueuedEvent *parcel.Parcel
				queuedEvent   *parcel.Parcel
			)

			BeforeEach(func() {
				unqueuedEvent = NewParcel(
					"<unqueued-event>",
					MessageU1,
				)

				queuedEvent = NewParcel(
					"<queued-event>",
					MessageQ1,
				)

				work.QueueEvents = message.TypesOf(MessageQ{})
				work.RecordEvent(unqueuedEvent)
				work.RecordEvent(queuedEvent)
			})

			It("includes the events in the result", func() {
				// Note that we're also testing that the offets from the
				// persistence result are correlated with their original parcel
				// in the handler result.
				//
				// We're relying on the fact that the memory persistence driver
				// used in the data-store stub always gives us a contiguous
				// block of offsets in the order we record the events.
				Expect(result.Events).To(EqualX(
					[]eventstream.Event{
						{
							Offset: 0,
							Parcel: unqueuedEvent,
						},
						{
							Offset: 1,
							Parcel: queuedEvent,
						},
					},
				))

				Expect(result.Queued).To(EqualX(
					[]queue.Message{
						{
							Parcel: queuedEvent,
							Item: &queuestore.Item{
								Revision:      1,
								NextAttemptAt: queuedEvent.CreatedAt,
								Envelope:      queuedEvent.Envelope,
							},
						},
					},
				))
			})

			It("includes the events in the batch", func() {
				Expect(batch).To(EqualX(
					persistence.Batch{
						persistence.SaveEvent{
							Envelope: unqueuedEvent.Envelope,
						},
						persistence.SaveEvent{
							Envelope: queuedEvent.Envelope,
						},
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: queuedEvent.CreatedAt,
								Envelope:      queuedEvent.Envelope,
							},
						},
					},
				))
			})
		})

		When("the unit-of-work contains arbitrary persistence operations", func() {
			var oper persistence.Operation

			BeforeEach(func() {
				oper = persistence.SaveOffset{
					ApplicationKey: "<app-key>",
				}

				work.Do(oper)
			})

			It("includes the operations in the batch", func() {
				Expect(batch).To(EqualX(
					persistence.Batch{oper},
				))
			})
		})
	})

	When("the unit-of-work is empty", func() {
		It("notifies the observers", func() {
			called := false
			work.Observe(func(r Result, err error) {
				called = true
				Expect(r).To(Equal(Result{}))
				Expect(err).ShouldNot(HaveOccurred())
			})

			err := Persist(context.Background(), dataStore, work)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})
	})

	When("the unit-of-work can not be persisted", func() {
		BeforeEach(func() {
			dataStore.PersistFunc = func(
				context.Context,
				persistence.Batch,
			) (persistence.Result, error) {
				return persistence.Result{}, errors.New("<error>")
			}
		})

		It("notifies the observers", func() {
			called := false
			work.Observe(func(r Result, err error) {
				called = true
				Expect(err).To(MatchError("<error>"))
			})

			err := Persist(context.Background(), dataStore, work)
			Expect(err).To(MatchError("<error>"))
			Expect(called).To(BeTrue())
		})
	})
})
