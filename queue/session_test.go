package queue_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type session", func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		provider   *ProviderStub
		dataStore  *DataStoreStub
		queue      *Queue
		sess       pipeline.Session
		env0, env1 *envelope.Envelope
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		env0 = NewEnvelope("<message-0>", MessageA1)
		env1 = NewEnvelope("<message-1>", MessageA2)

		provider = &ProviderStub{
			Provider: &memory.Provider{},
		}

		ds, err := provider.Open(ctx, "<app-key>")
		Expect(err).ShouldNot(HaveOccurred())

		dataStore = ds.(*DataStoreStub)

		queue = &Queue{
			DataStore: dataStore,
			Marshaler: Marshaler,
		}
	})

	JustBeforeEach(func() {
		go queue.Run(ctx)

		push(ctx, queue, env0)

		var err error
		sess, err = queue.Pop(ctx)
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		sess.Close()

		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
	})

	Describe("func MessageID()", func() {
		It("returns the message ID", func() {
			Expect(sess.MessageID()).To(Equal("<message-0>"))
		})
	})

	Describe("func Envelope()", func() {
		It("returns the unmarshaled message envelope", func() {
			e, err := sess.Envelope()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(e).To(Equal(env0))
		})

		It("unmarshals the envelope if necessary", func() {
			// Push a new envelope, which will get discarded from memory due to
			// the buffer size limit.
			queue.BufferSize = 1
			push(ctx, queue, env1)

			// Commit and close the existing session for env0, freeing us to
			// load again.
			err := sess.Ack(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Close()
			Expect(err).ShouldNot(HaveOccurred())

			// Start the session for env1.
			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()

			// Finally, verify that the message is unpacked.
			e, err := sess.Envelope()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(e).To(Equal(env1))
		})
	})

	Describe("func Transaction()", func() {
		It("begins a transaction", func() {
			tx, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(tx).ShouldNot(BeNil())
		})

		It("returns the same transaction on each call", func() {
			tx1, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			tx2, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(tx1).To(BeIdenticalTo(tx2))
		})

		It("returns an error if the transaction cannot be begun", func() {
			dataStore.BeginFunc = func(
				ctx context.Context,
			) (persistence.Transaction, error) {
				return nil, errors.New("<error>")
			}

			_, err := sess.Tx(ctx)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func Ack()", func() {
		It("commits the underlying transaction", func() {
			tx, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = tx.SaveEvent(ctx, NewEnvelopeProto("<event>", MessageE1))
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Ack(ctx)
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
			_, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			// Ensure no new transactions can be started.
			dataStore.BeginFunc = func(
				ctx context.Context,
			) (persistence.Transaction, error) {
				return nil, errors.New("<error>")
			}

			err = sess.Ack(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			messages, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(messages).To(BeEmpty())
		})

		It("returns an error if the transaction cannot be begun", func() {
			dataStore.BeginFunc = func(
				ctx context.Context,
			) (persistence.Transaction, error) {
				return nil, errors.New("<error>")
			}

			err := sess.Ack(ctx)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the message can not be removed from the queue store", func() {
			tx, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			tx.(*TransactionStub).RemoveMessageFromQueueFunc = func(
				context.Context,
				*queuestore.Message,
			) error {
				return errors.New("<error>")
			}

			err = sess.Ack(ctx)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the transaction can not be committed", func() {
			tx, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			tx.(*TransactionStub).CommitFunc = func(context.Context) error {
				return errors.New("<error>")
			}

			err = sess.Ack(ctx)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func Nack()", func() {
		It("rolls the underlying transaction back", func() {
			tx, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = tx.SaveEvent(ctx, NewEnvelopeProto("<event>", MessageE1))
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Nack(ctx, time.Now().Add(1*time.Hour))
			Expect(err).ShouldNot(HaveOccurred())

			repo := dataStore.EventStoreRepository()
			res, err := repo.QueryEvents(ctx, eventstore.Query{})
			Expect(err).ShouldNot(HaveOccurred())

			_, ok, err := res.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("updates the next-attempt time in the queue store", func() {
			next := time.Now().Add(1 * time.Hour)
			err := sess.Nack(ctx, next)
			Expect(err).ShouldNot(HaveOccurred())

			messages, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(messages).To(HaveLen(1))

			m := messages[0]
			Expect(m.NextAttemptAt).To(BeTemporally("~", next))
		})

		It("returns the message to the in-memory queue when closed", func() {
			next := time.Now().Add(10 * time.Millisecond)

			err := sess.Nack(ctx, next)
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Close()
			Expect(err).ShouldNot(HaveOccurred())

			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()

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

			err := sess.Nack(ctx, time.Now())
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the transaction can not be rolled-back", func() {
			tx, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			tx.(*TransactionStub).RollbackFunc = func() error {
				return errors.New("<error>")
			}

			err = sess.Nack(ctx, time.Now())
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func Close()", func() {
		It("rolls the transaction back", func() {
			tx, err := sess.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Close()
			Expect(err).ShouldNot(HaveOccurred())

			err = tx.(persistence.Transaction).Rollback()
			Expect(err).To(Equal(persistence.ErrTransactionClosed))
		})

		It("returns the message to the in-memory queue for immediate handling", func() {
			err := sess.Close()
			Expect(err).ShouldNot(HaveOccurred())

			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()
		})
	})
})
