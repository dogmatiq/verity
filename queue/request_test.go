package queue_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Request", func() {
	var (
		ctx              context.Context
		cancel           context.CancelFunc
		dataStore        *DataStoreStub
		queue            *Queue
		parcel0, parcel1 *parcel.Parcel
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		parcel0 = NewParcel("<message-0>", MessageA1)
		parcel1 = NewParcel("<message-1>", MessageA2)

		dataStore = NewDataStoreStub()

		queue = &Queue{
			DataStore: dataStore,
			Marshaler: Marshaler,
		}
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
	})

	When("the queue is running", func() {
		var req *Request

		JustBeforeEach(func() {
			go queue.Run(ctx)

			push(ctx, queue, parcel0)

			var err error
			req, err = queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})

		AfterEach(func() {
			req.Close()
		})

		Describe("func MessageID()", func() {
			It("returns the message ID", func() {
				Expect(req.MessageID()).To(Equal("<message-0>"))
			})
		})

		Describe("func FailureCount()", func() {
			It("returns the number of times the message has failed handling", func() {
				err := req.Nack(ctx, time.Now())
				Expect(err).ShouldNot(HaveOccurred())
				req.Close()

				req, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer req.Close()

				Expect(req.FailureCount()).To(BeEquivalentTo(1))
			})
		})

		Describe("func Envelope()", func() {
			It("returns the message envelope", func() {
				e := req.Envelope()
				Expect(e).To(EqualX(parcel0.Envelope))
			})
		})

		Describe("func Parcel()", func() {
			It("returns the parcel", func() {
				p, err := req.Parcel()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(p).To(EqualX(parcel0))
			})

			It("unmarshals the parcel if necessary", func() {
				// Push a new message, which will get discarded from memory due to
				// the buffer size limit.
				queue.BufferSize = 1
				push(ctx, queue, parcel1)

				// Acknowledge and close the existing request for parcel0, freeing
				// us to load again.
				err := req.Ack(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				err = req.Close()
				Expect(err).ShouldNot(HaveOccurred())

				// Start the request for parcel1.
				req, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer req.Close()

				// Finally, verify that the parcel is unpacked.
				p, err := req.Parcel()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(p).To(EqualX(parcel1))
			})
		})

		Describe("func Transaction()", func() {
			It("begins a transaction", func() {
				tx, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(tx).ShouldNot(BeNil())
			})

			It("returns the same transaction on each call", func() {
				tx1, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				tx2, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(tx1).To(BeIdenticalTo(tx2))
			})

			It("returns an error if the transaction cannot be begun", func() {
				dataStore.BeginFunc = func(
					ctx context.Context,
				) (persistence.Transaction, error) {
					return nil, errors.New("<error>")
				}

				_, err := req.Tx(ctx)
				Expect(err).To(MatchError("<error>"))
			})
		})

		Describe("func Ack()", func() {
			It("commits the underlying transaction", func() {
				tx, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				_, err = tx.SaveEvent(ctx, NewEnvelope("<event>", MessageE1))
				Expect(err).ShouldNot(HaveOccurred())

				err = req.Ack(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				repo := dataStore.EventStoreRepository()
				res, err := repo.QueryEvents(ctx, eventstore.Query{})
				Expect(err).ShouldNot(HaveOccurred())

				_, ok, err := res.Next(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(ok).To(BeTrue())
			})

			It("removes the message from the queue store within the same transaction", func() {
				// Make sure the transaction is started.
				_, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				// Ensure no new transactions can be started.
				dataStore.BeginFunc = func(
					ctx context.Context,
				) (persistence.Transaction, error) {
					return nil, errors.New("<error>")
				}

				err = req.Ack(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				items, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(items).To(BeEmpty())
			})

			It("returns an error if the transaction cannot be begun", func() {
				dataStore.BeginFunc = func(
					ctx context.Context,
				) (persistence.Transaction, error) {
					return nil, errors.New("<error>")
				}

				err := req.Ack(ctx)
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if the message can not be removed from the queue store", func() {
				tx, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				tx.(*TransactionStub).RemoveMessageFromQueueFunc = func(
					context.Context,
					*queuestore.Item,
				) error {
					return errors.New("<error>")
				}

				err = req.Ack(ctx)
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if the transaction can not be committed", func() {
				tx, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				tx.(*TransactionStub).CommitFunc = func(
					context.Context,
				) (persistence.TransactionResult, error) {
					return persistence.TransactionResult{}, errors.New("<error>")
				}

				err = req.Ack(ctx)
				Expect(err).To(MatchError("<error>"))
			})
		})

		Describe("func Nack()", func() {
			It("rolls the underlying transaction back", func() {
				tx, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				_, err = tx.SaveEvent(ctx, NewEnvelope("<event>", MessageE1))
				Expect(err).ShouldNot(HaveOccurred())

				err = req.Nack(ctx, time.Now().Add(1*time.Hour))
				Expect(err).ShouldNot(HaveOccurred())

				repo := dataStore.EventStoreRepository()
				res, err := repo.QueryEvents(ctx, eventstore.Query{})
				Expect(err).ShouldNot(HaveOccurred())

				_, ok, err := res.Next(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(ok).To(BeFalse())
			})

			It("updates the failure count and next-attempt time in the queue store", func() {
				next := time.Now().Add(1 * time.Hour)
				err := req.Nack(ctx, next)
				Expect(err).ShouldNot(HaveOccurred())

				items, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(items).To(HaveLen(1))

				i := items[0]
				Expect(i.FailureCount).To(BeEquivalentTo(1))
				Expect(i.NextAttemptAt).To(BeTemporally("~", next))
			})

			It("returns the message to the in-memory queue when closed", func() {
				next := time.Now().Add(10 * time.Millisecond)

				err := req.Nack(ctx, next)
				Expect(err).ShouldNot(HaveOccurred())

				err = req.Close()
				Expect(err).ShouldNot(HaveOccurred())

				req, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer req.Close()

				Expect(time.Now()).To(BeTemporally(">=", next))
			})

			It("returns an error if the message can not be updated in the queue store", func() {
				// Stop the begin of the transaction that is started to update the
				// message.
				dataStore.BeginFunc = func(
					ctx context.Context,
				) (persistence.Transaction, error) {
					return nil, errors.New("<error>")
				}

				err := req.Nack(ctx, time.Now())
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if the transaction can not be rolled-back", func() {
				tx, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				tx.(*TransactionStub).RollbackFunc = func() error {
					return errors.New("<error>")
				}

				err = req.Nack(ctx, time.Now())
				Expect(err).To(MatchError("<error>"))
			})
		})

		Describe("func Close()", func() {
			It("rolls the transaction back", func() {
				tx, err := req.Tx(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				err = req.Close()
				Expect(err).ShouldNot(HaveOccurred())

				err = tx.(persistence.Transaction).Rollback()
				Expect(err).To(Equal(persistence.ErrTransactionClosed))
			})

			It("returns the message to the in-memory queue for immediate handling", func() {
				err := req.Close()
				Expect(err).ShouldNot(HaveOccurred())

				req, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer req.Close()
			})
		})
	})

	When("the queue has stopped", func() {
		var req *Request

		BeforeEach(func() {
			// It's an implementation detail, but the internal channel used to
			// start tracking is buffered at the same size as the overall buffer
			// size limit.
			//
			// We can't set it to zero, because that will fallback to the
			// default. We also can't start the queue, otherwise it'll start
			// reading from this channel and nothing will block.
			//
			// Instead, we set it to one, and "fill" the channel with a request
			// to ensure that it will block.
			queue.BufferSize = 1

			runCtx, cancelRun := context.WithCancel(context.Background())

			go func() {
				defer GinkgoRecover()
				defer cancelRun()

				push(ctx, queue, parcel0)

				var err error
				req, err = queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
			}()

			err := queue.Run(runCtx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("does not block", func() {
			done := make(chan struct{})
			go func() {
				req.Close()
				close(done)
			}()

			select {
			case <-ctx.Done():
				Expect(ctx.Err()).ShouldNot(HaveOccurred())
			case <-done:
			}
		})

		It("does not block, even if the internal buffer is full", func() {
			// Fill the buffer.
			err := queue.Track(
				ctx,
				parcel1,
				&queuestore.Item{
					Revision: 1,
					Envelope: parcel1.Envelope,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())

			done := make(chan struct{})
			go func() {
				req.Close()
				close(done)
			}()

			select {
			case <-ctx.Done():
				Expect(ctx.Err()).ShouldNot(HaveOccurred())
			case <-done:
			}
		})
	})
})
