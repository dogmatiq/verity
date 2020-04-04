package queue_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
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
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
	})

	Describe("func Envelope()", func() {
		It("returns the unmarshaled message envelope", func() {
			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()

			Expect(sess.Envelope()).To(Equal(env))
		})
	})

	Describe("func Transaction()", func() {
		It("returns the transaction", func() {
			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()

			tx := sess.Tx()
			Expect(tx).NotTo(BeNil())
		})
	})

	Describe("func Close()", func() {
		It("rolls the transaction back", func() {
			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Close()
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Tx().(persistence.Transaction).Rollback()
			Expect(err).To(Equal(persistence.ErrTransactionClosed))
		})

		XIt("removes the session from the queue's active session list", func() {

		})
	})
})
