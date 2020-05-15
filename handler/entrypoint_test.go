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

var _ = Describe("type EntryPoint", func() {
	var (
		dataStore  *DataStoreStub
		handler    *HandlerStub
		cause      *parcel.Parcel
		entryPoint *EntryPoint
	)

	BeforeEach(func() {
		dataStore = NewDataStoreStub()

		handler = &HandlerStub{}

		cause = NewParcel("<consume>", MessageC1)

		entryPoint = &EntryPoint{
			QueueEvents: message.TypesOf(MessageQ{}),
			Persister:   dataStore,
			Handler:     handler,
		}
	})

	AfterEach(func() {
		dataStore.Close()
	})

	Describe("func HandleMessage()", func() {
		It("forwards to the handler", func() {
			handler.HandleMessageFunc = func(
				_ context.Context,
				_ *UnitOfWork,
				p *parcel.Parcel,
			) error {
				Expect(p).To(Equal(cause))
				return errors.New("<error>")
			}

			err := entryPoint.HandleMessage(context.Background(), cause, nil)
			Expect(err).To(MatchError("<error>"))
		})

		When("the message is handled successfully", func() {
			var (
				command        *parcel.Parcel
				timeout        *parcel.Parcel
				unqueuedEvent  *parcel.Parcel
				queuedEvent    *parcel.Parcel
				oper           persistence.Operation
				expectedResult Result
			)

			BeforeEach(func() {
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

				handler.HandleMessageFunc = func(
					ctx context.Context,
					w *UnitOfWork,
					p *parcel.Parcel,
				) error {
					w.ExecuteCommand(command)
					w.ScheduleTimeout(timeout)
					w.RecordEvent(unqueuedEvent)
					w.RecordEvent(queuedEvent)
					w.Do(oper)
					return nil
				}

				expectedResult = Result{
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
				}
			})

			It("persists the batch from the unit-of-work", func() {
				called := false
				dataStore.PersistFunc = func(
					_ context.Context,
					b persistence.Batch,
				) (persistence.Result, error) {
					called = true
					Expect(b).To(EqualX(
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
							persistence.SaveEvent{
								Envelope: queuedEvent.Envelope,
							},
							persistence.SaveQueueItem{
								Item: queuestore.Item{
									NextAttemptAt: queuedEvent.CreatedAt,
									Envelope:      queuedEvent.Envelope,
								},
							},
							// other ...
							oper,
						},
					))

					return persistence.Result{}, nil
				}

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(called).To(BeTrue())
			})

			It("notifies the entry point's observers", func() {
				called := false
				entryPoint.Observers = append(
					entryPoint.Observers,
					func(res Result, err error) {
						called = true
						Expect(err).ShouldNot(HaveOccurred())
						Expect(res).To(EqualX(expectedResult))
					},
				)

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(called).To(BeTrue())
			})

			It("notifies the unit-of-work's observers", func() {
				called := false
				next := handler.HandleMessageFunc
				handler.HandleMessageFunc = func(
					ctx context.Context,
					w *UnitOfWork,
					p *parcel.Parcel,
				) error {
					w.Observe(func(res Result, err error) {
						called = true
						Expect(err).ShouldNot(HaveOccurred())
						Expect(res).To(EqualX(expectedResult))
					})
					return next(ctx, w, p)
				}

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(called).To(BeTrue())
			})
		})

		When("the handler returns an error", func() {
			BeforeEach(func() {
				handler.HandleMessageFunc = func(
					context.Context,
					*UnitOfWork,
					*parcel.Parcel,
				) error {
					return errors.New("<error>")
				}
			})

			It("notifies the entry point's observers", func() {
				called := false
				entryPoint.Observers = append(
					entryPoint.Observers,
					func(_ Result, err error) {
						called = true
						Expect(err).To(MatchError("<error>"))
					},
				)

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).To(MatchError("<error>"))
				Expect(called).To(BeTrue())
			})

			It("notifies the unit-of-work's observers", func() {
				called := false
				handler.HandleMessageFunc = func(
					_ context.Context,
					w *UnitOfWork,
					_ *parcel.Parcel,
				) error {
					w.Observe(func(_ Result, err error) {
						called = true
						Expect(err).To(MatchError("<error>"))
					})

					return errors.New("<error>")
				}

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).To(MatchError("<error>"))
				Expect(called).To(BeTrue())
			})

			It("does not persist anything", func() {
				dataStore.PersistFunc = func(
					context.Context,
					persistence.Batch,
				) (persistence.Result, error) {
					Fail("unexpected call")
					return persistence.Result{}, nil
				}

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).To(MatchError("<error>"))
			})
		})

		When("the persister returns an error", func() {
			BeforeEach(func() {
				dataStore.PersistFunc = func(
					context.Context,
					persistence.Batch,
				) (persistence.Result, error) {
					return persistence.Result{}, errors.New("<error>")
				}
			})

			It("notifies the entry point's observers", func() {
				called := false
				entryPoint.Observers = append(
					entryPoint.Observers,
					func(_ Result, err error) {
						called = true
						Expect(err).To(MatchError("<error>"))
					},
				)

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).To(MatchError("<error>"))
				Expect(called).To(BeTrue())
			})

			It("notifies the unit-of-work's observers", func() {
				called := false
				handler.HandleMessageFunc = func(
					_ context.Context,
					w *UnitOfWork,
					_ *parcel.Parcel,
				) error {
					w.Observe(func(_ Result, err error) {
						called = true
						Expect(err).To(MatchError("<error>"))
					})

					return errors.New("<error>")
				}

				err := entryPoint.HandleMessage(context.Background(), cause, nil)
				Expect(err).To(MatchError("<error>"))
				Expect(called).To(BeTrue())
			})
		})
	})
})
