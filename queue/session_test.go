package queue_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Session", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		provider  *ProviderStub
		dataStore *DataStoreStub
		queue     *Queue
		sess      *Session
		env       = NewEnvelope("<id>", MessageA1)
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

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

		err = queue.Push(ctx, env)
		Expect(err).ShouldNot(HaveOccurred())

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

	Describe("func Envelope()", func() {
		It("returns the unmarshaled message envelope", func() {
			Expect(sess.Envelope()).To(Equal(env))
		})
	})

	Describe("func Transaction()", func() {
		It("returns a transaction", func() {
			Expect(sess.Tx()).NotTo(BeNil())
		})
	})

	Describe("func Commit()", func() {
		XIt("commits the underlying transaction", func() {

		})

		XIt("removes the message from the queue store within the same transaction", func() {

		})
	})

	Describe("func Rollback()", func() {
		It("rolls the underlying transaction back", func() {
			_, err := sess.Tx().SaveEvent(ctx, NewEnvelopeProto("<event>", MessageE1))
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Rollback(ctx, time.Now().Add(1*time.Hour))
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
			err := sess.Rollback(ctx, next)
			Expect(err).ShouldNot(HaveOccurred())

			messages, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(messages).To(HaveLen(1))

			m := messages[0]
			Expect(m.NextAttemptAt).To(BeTemporally("~", next))
		})

		It("returns the message to the pending list", func() {
			next := time.Now().Add(10 * time.Millisecond)

			err := sess.Rollback(ctx, next)
			Expect(err).ShouldNot(HaveOccurred())

			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()

			Expect(time.Now()).To(BeTemporally(">=", next))
		})
	})

	Describe("func Close()", func() {
		It("rolls the transaction back", func() {
			err := sess.Close()
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Tx().(persistence.Transaction).Rollback()
			Expect(err).To(Equal(persistence.ErrTransactionClosed))
		})
	})
})
